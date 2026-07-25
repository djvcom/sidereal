//! Natural-language query agent.
//!
//! Answers questions about telemetry data by running a model in a tool-calling
//! loop against the Sidereal query API. Every answer carries provenance: the
//! tool calls the model made and summaries of their results.

mod tools;

use std::sync::Arc;

use serde::Serialize;

use crate::client::SiderealClient;
use crate::config::ModelConfig;

/// A record of one tool invocation made while answering a question.
#[derive(Debug, Clone, Serialize)]
pub struct ToolInvocation {
    /// Name of the tool that was called.
    pub tool: String,
    /// Arguments the model supplied, as JSON.
    pub arguments: serde_json::Value,
    /// Brief summary of the result (truncated for display).
    pub result_summary: String,
}

/// An answer to a natural-language question.
#[derive(Debug, Clone, Serialize)]
pub struct Answer {
    /// The model's answer text.
    pub text: String,
    /// The tool calls made to produce the answer.
    pub provenance: Vec<ToolInvocation>,
}

/// Errors that can occur while answering a question.
///
/// Failures that happen after tool calls have been made carry the provenance
/// recorded so far, so partial evidence is never silently discarded.
#[derive(Debug, thiserror::Error)]
pub enum AgentError {
    /// The model provider rejected the configuration.
    #[error("model configuration error: {0}")]
    Configuration(String),
    /// The model used up its tool budget without producing an answer.
    #[error("the model exhausted its tool budget of {turns} turns before answering")]
    MaxTurns {
        /// The turn budget that was exhausted.
        turns: usize,
        /// Tool calls made before the budget ran out.
        provenance: Vec<ToolInvocation>,
    },
    /// The model call failed.
    #[error("model error: {message}")]
    Model {
        /// Description of the failure.
        message: String,
        /// Tool calls made before the failure.
        provenance: Vec<ToolInvocation>,
    },
}

/// Natural-language query agent bound to a Sidereal instance and a model.
pub struct QueryAgent {
    client: Arc<SiderealClient>,
    model: ModelConfig,
}

impl QueryAgent {
    /// Create an agent from configuration and an authenticated client.
    pub const fn new(client: Arc<SiderealClient>, model: ModelConfig) -> Self {
        Self { client, model }
    }

    /// Answer a natural-language question about the telemetry data.
    pub async fn ask(&self, question: &str) -> Result<Answer, AgentError> {
        tools::run(&self.client, &self.model, question).await
    }
}
