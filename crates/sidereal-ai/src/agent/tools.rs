//! Tool definitions and the model-driven answer loop.
//!
//! The agent gives the model three read-only tools over the Sidereal query
//! API and lets it call them repeatedly (up to a bounded number of turns)
//! before producing an answer. A prompt hook records every tool invocation
//! so answers carry verifiable provenance.

use std::sync::{Arc, Mutex};

use rig_core::agent::{AgentBuilder, HookAction, PromptHook};
use rig_core::client::{CompletionClient, Nothing};
use rig_core::completion::{CompletionModel, Prompt, PromptError, ToolDefinition};
use rig_core::providers::{anthropic, ollama, openai};
use rig_core::tool::Tool;
use serde::Deserialize;
use serde_json::json;

use super::{AgentError, Answer, ToolInvocation};
use crate::client::{ClientError, SiderealClient};
use crate::config::{ModelConfig, ModelProvider};

/// Default model when the provider is Anthropic and no name is configured.
const DEFAULT_ANTHROPIC_MODEL: &str = "claude-opus-4-8";

/// Maximum number of tool round-trips per question.
const MAX_TOOL_TURNS: usize = 12;

/// Maximum output tokens per model response.
const MAX_OUTPUT_TOKENS: u64 = 8192;

/// Maximum characters of tool output returned to the model.
const MAX_TOOL_RESULT_CHARS: usize = 24_000;

/// Maximum characters of a tool result kept in the provenance log.
const SUMMARY_CHARS: usize = 400;

const SYSTEM_PROMPT: &str = "\
You are Sidereal AI, an analyst for the Sidereal observability database. You \
answer questions about distributed traces, metrics, and logs by querying the \
data, and you never speculate beyond what the data shows.

The database is queried with DataFusion SQL. The tables are `traces`, \
`metrics`, and `logs`. Column names and types can be discovered with, for \
example: SELECT column_name, data_type FROM information_schema.columns WHERE \
table_name = 'traces'. Discover the schema before querying columns you are \
not certain exist. Timestamp columns use nanosecond precision; inspect data \
types rather than assuming.

Tools available to you:
- execute_sql: run a read-only SQL SELECT statement.
- list_errors: aggregated error groups with counts, trends, and versions.
- list_deployments: recorded deployments and service versions.

Rules:
- Only issue SELECT statements. Always include a LIMIT; keep result sets \
small and aggregate in SQL rather than fetching raw rows.
- Base every claim on a tool result from this conversation. State the time \
window and sample sizes your answer rests on.
- If the data is insufficient to answer, say so plainly rather than guessing.
- Prefer several small, focused queries over one large one.
- Answer in British English, concisely, leading with the finding.";

/// Answer a question using the configured model provider.
pub(super) async fn run(
    sidereal: &Arc<SiderealClient>,
    model: &ModelConfig,
    question: &str,
) -> Result<Answer, AgentError> {
    match model.provider {
        ModelProvider::Anthropic => {
            let api_key = resolve_api_key(model, "ANTHROPIC_API_KEY")?;
            let mut builder = anthropic::Client::builder().api_key(api_key);
            if let Some(base_url) = &model.base_url {
                builder = builder.base_url(base_url);
            }
            let provider = builder.build().map_err(configuration_error)?;
            let name = model
                .name
                .clone()
                .unwrap_or_else(|| DEFAULT_ANTHROPIC_MODEL.to_owned());
            prompt_agent(provider.agent(name), sidereal, question).await
        }
        ModelProvider::Ollama => {
            let name = require_model_name(model, "ollama")?;
            let api_key = model.api_key.clone().map_or_else(
                || ollama::OllamaApiKey::from(Nothing),
                ollama::OllamaApiKey::from,
            );
            let mut builder = ollama::Client::builder().api_key(api_key);
            if let Some(base_url) = &model.base_url {
                builder = builder.base_url(base_url);
            }
            let provider = builder.build().map_err(configuration_error)?;
            prompt_agent(provider.agent(name), sidereal, question).await
        }
        ModelProvider::Openai => {
            let api_key = resolve_api_key(model, "OPENAI_API_KEY")?;
            let name = require_model_name(model, "openai")?;
            let mut builder = openai::CompletionsClient::builder().api_key(api_key);
            if let Some(base_url) = &model.base_url {
                builder = builder.base_url(base_url);
            }
            let provider = builder.build().map_err(configuration_error)?;
            prompt_agent(provider.agent(name), sidereal, question).await
        }
    }
}

fn configuration_error(e: impl std::fmt::Display) -> AgentError {
    AgentError::Configuration(e.to_string())
}

fn resolve_api_key(model: &ModelConfig, env_var: &str) -> Result<String, AgentError> {
    if let Some(key) = &model.api_key {
        return Ok(key.clone());
    }
    std::env::var(env_var).map_err(|_| {
        AgentError::Configuration(format!(
            "no API key configured: set model.api_key or the {env_var} environment variable"
        ))
    })
}

fn require_model_name(model: &ModelConfig, provider: &str) -> Result<String, AgentError> {
    model.name.clone().ok_or_else(|| {
        AgentError::Configuration(format!(
            "model.name must be set for the {provider} provider"
        ))
    })
}

/// Build the agent with tools and run the multi-turn prompt.
async fn prompt_agent<M>(
    builder: AgentBuilder<M>,
    sidereal: &Arc<SiderealClient>,
    question: &str,
) -> Result<Answer, AgentError>
where
    M: CompletionModel + 'static,
{
    let log = Arc::new(Mutex::new(Vec::new()));
    let hook = ProvenanceHook {
        log: Arc::clone(&log),
    };

    let agent = builder
        .preamble(SYSTEM_PROMPT)
        .max_tokens(MAX_OUTPUT_TOKENS)
        .tool(ExecuteSql {
            sidereal: Arc::clone(sidereal),
        })
        .tool(ListErrors {
            sidereal: Arc::clone(sidereal),
        })
        .tool(ListDeployments {
            sidereal: Arc::clone(sidereal),
        })
        .build();

    let result = agent
        .prompt(question)
        .max_turns(MAX_TOOL_TURNS)
        .with_hook(hook)
        .await;

    match result {
        Ok(text) => Ok(Answer {
            text,
            provenance: drain_log(log),
        }),
        Err(PromptError::MaxTurnsError { max_turns, .. }) => Err(AgentError::MaxTurns {
            turns: max_turns,
            provenance: drain_log(log),
        }),
        Err(e) => Err(AgentError::Model {
            message: e.to_string(),
            provenance: drain_log(log),
        }),
    }
}

/// Take the recorded invocations out of the provenance log.
///
/// After the prompt future completes the hook has been dropped, so the log
/// usually has a single owner and the entries can be moved out without
/// cloning.
fn drain_log(log: Arc<Mutex<Vec<ToolInvocation>>>) -> Vec<ToolInvocation> {
    Arc::try_unwrap(log).map_or_else(
        |shared| {
            shared
                .lock()
                .map(|entries| entries.clone())
                .unwrap_or_default()
        },
        |mutex| {
            mutex
                .into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        },
    )
}

/// Prompt hook that records every tool invocation for provenance.
#[derive(Clone)]
struct ProvenanceHook {
    log: Arc<Mutex<Vec<ToolInvocation>>>,
}

impl<M: CompletionModel> PromptHook<M> for ProvenanceHook {
    async fn on_tool_result(
        &self,
        tool_name: &str,
        _tool_call_id: Option<String>,
        _internal_call_id: &str,
        args: &str,
        result: &str,
    ) -> HookAction {
        let arguments = serde_json::from_str(args).unwrap_or_else(|_| json!(args));
        let invocation = ToolInvocation {
            tool: tool_name.to_owned(),
            arguments,
            result_summary: truncate_chars(result, SUMMARY_CHARS),
        };
        if let Ok(mut log) = self.log.lock() {
            log.push(invocation);
        } else {
            tracing::warn!(
                tool = tool_name,
                "provenance log poisoned; invocation not recorded"
            );
        }
        HookAction::cont()
    }
}

/// Cap a tool result so oversized payloads cannot flood the model context.
fn bound_result(value: serde_json::Value) -> serde_json::Value {
    let Ok(serialised) = serde_json::to_string(&value) else {
        return value;
    };
    if serialised.chars().count() <= MAX_TOOL_RESULT_CHARS {
        return value;
    }
    json!({
        "truncated": true,
        "note": format!(
            "result truncated to {MAX_TOOL_RESULT_CHARS} characters; \
             narrow the query or aggregate in SQL"
        ),
        "preview": truncate_chars(&serialised, MAX_TOOL_RESULT_CHARS),
    })
}

fn truncate_chars(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        text.to_owned()
    } else {
        let mut truncated: String = text.chars().take(limit).collect();
        truncated.push('…');
        truncated
    }
}

/// Execute a read-only SQL query against the telemetry tables.
struct ExecuteSql {
    sidereal: Arc<SiderealClient>,
}

#[derive(Deserialize)]
struct ExecuteSqlArgs {
    sql: String,
}

impl Tool for ExecuteSql {
    const NAME: &'static str = "execute_sql";

    type Error = ClientError;
    type Args = ExecuteSqlArgs;
    type Output = serde_json::Value;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_owned(),
            description: "Execute a read-only DataFusion SQL SELECT statement against the \
                          telemetry database. Tables: traces, metrics, logs. Use \
                          information_schema.columns to discover the schema. Always include \
                          a LIMIT clause."
                .to_owned(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "The SQL SELECT statement to execute"
                    }
                },
                "required": ["sql"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        self.sidereal.sql(&args.sql).await.map(bound_result)
    }
}

/// List aggregated error groups from the error tracking API.
struct ListErrors {
    sidereal: Arc<SiderealClient>,
}

#[derive(Deserialize)]
struct ListErrorsArgs {
    start_time: Option<String>,
    end_time: Option<String>,
    service: Option<String>,
    environment: Option<String>,
    error_type: Option<String>,
    version: Option<String>,
    min_count: Option<u64>,
    sort_by: Option<String>,
    limit: Option<u64>,
}

impl Tool for ListErrors {
    const NAME: &'static str = "list_errors";

    type Error = ClientError;
    type Args = ListErrorsArgs;
    type Output = serde_json::Value;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_owned(),
            description: "List aggregated error groups: fingerprint, error type, message, \
                          service, occurrence count, affected traces, first/last seen, \
                          whether the error is new, and its trend."
                .to_owned(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "start_time": {"type": "string", "description": "ISO 8601 window start"},
                    "end_time": {"type": "string", "description": "ISO 8601 window end"},
                    "service": {"type": "string", "description": "Filter by service name"},
                    "environment": {"type": "string", "description": "Filter by environment"},
                    "error_type": {"type": "string", "description": "Filter by error type"},
                    "version": {"type": "string", "description": "Filter by service version"},
                    "min_count": {"type": "integer", "description": "Minimum occurrence count"},
                    "sort_by": {"type": "string", "description": "Sort field, e.g. count or last_seen"},
                    "limit": {"type": "integer", "description": "Maximum groups to return"}
                }
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        self.sidereal
            .get_json(
                "errors",
                vec![
                    ("start_time", args.start_time),
                    ("end_time", args.end_time),
                    ("service", args.service),
                    ("environment", args.environment),
                    ("error_type", args.error_type),
                    ("version", args.version),
                    ("min_count", args.min_count.map(|v| v.to_string())),
                    ("sort_by", args.sort_by),
                    ("limit", args.limit.map(|v| v.to_string())),
                ],
            )
            .await
            .map(bound_result)
    }
}

/// List recorded deployments from the deployments API.
struct ListDeployments {
    sidereal: Arc<SiderealClient>,
}

#[derive(Deserialize)]
struct ListDeploymentsArgs {
    start_time: Option<String>,
    end_time: Option<String>,
    service: Option<String>,
    environment: Option<String>,
    status: Option<String>,
    limit: Option<u64>,
}

impl Tool for ListDeployments {
    const NAME: &'static str = "list_deployments";

    type Error = ClientError;
    type Args = ListDeploymentsArgs;
    type Output = serde_json::Value;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_owned(),
            description: "List recorded deployments with service, version, environment, \
                          status, and timing. Useful for correlating behaviour changes \
                          with releases."
                .to_owned(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "start_time": {"type": "string", "description": "ISO 8601 window start"},
                    "end_time": {"type": "string", "description": "ISO 8601 window end"},
                    "service": {"type": "string", "description": "Filter by service name"},
                    "environment": {"type": "string", "description": "Filter by environment"},
                    "status": {"type": "string", "description": "Filter by deployment status"},
                    "limit": {"type": "integer", "description": "Maximum deployments to return"}
                }
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        self.sidereal
            .get_json(
                "deployments",
                vec![
                    ("start_time", args.start_time),
                    ("end_time", args.end_time),
                    ("service", args.service),
                    ("environment", args.environment),
                    ("status", args.status),
                    ("limit", args.limit.map(|v| v.to_string())),
                ],
            )
            .await
            .map(bound_result)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn truncate_chars_leaves_short_text_untouched() {
        assert_eq!(truncate_chars("short", 10), "short");
    }

    #[test]
    fn truncate_chars_appends_ellipsis_on_character_boundary() {
        let truncated = truncate_chars("ααββγγ", 3);
        assert_eq!(truncated, "ααβ…");
    }

    #[test]
    fn bound_result_passes_small_values_through() {
        let value = json!({"rows": [1, 2, 3]});
        assert_eq!(bound_result(value.clone()), value);
    }

    #[test]
    fn bound_result_truncates_oversized_values() {
        let huge = json!({"rows": vec!["x".repeat(1000); 50]});
        let bounded = bound_result(huge);
        assert_eq!(bounded["truncated"], json!(true));
        let preview = bounded["preview"]
            .as_str()
            .expect("preview should be a string");
        assert!(preview.chars().count() <= MAX_TOOL_RESULT_CHARS + 1);
    }

    #[test]
    fn missing_api_key_is_a_configuration_error() {
        let model = ModelConfig::default();
        let result = resolve_api_key(&model, "SIDEREAL_AI_TEST_ABSENT_KEY");
        assert!(matches!(result, Err(AgentError::Configuration(_))));
    }

    #[test]
    fn configured_api_key_wins_over_environment() {
        let model = ModelConfig {
            api_key: Some("from-config".to_owned()),
            ..ModelConfig::default()
        };
        let key = resolve_api_key(&model, "SIDEREAL_AI_TEST_ABSENT_KEY")
            .expect("configured key should resolve");
        assert_eq!(key, "from-config");
    }
}
