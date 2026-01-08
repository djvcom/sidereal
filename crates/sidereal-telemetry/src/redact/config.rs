//! Redaction pipeline configuration.
//!
//! Defines the configuration schema for PII redaction rules including
//! matchers (attribute names, glob patterns, regexes) and actions
//! (drop, hash, redact).

use std::path::PathBuf;

use serde::Deserialize;

use crate::storage::Signal;

/// Configuration for the redaction pipeline.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct RedactionConfig {
    /// Enable redaction pipeline (default: false).
    pub enabled: bool,

    /// Path to HMAC secret key file for hashing.
    /// File must have mode 0600 (owner read/write only).
    /// If not specified, hashing actions will fail.
    pub key_file: Option<PathBuf>,

    /// HMAC secret key from environment variable.
    /// Takes precedence over key_file if both are set.
    pub key_env: Option<String>,

    /// Redaction rules applied in order.
    pub rules: Vec<RedactionRule>,

    /// Whether to redact span/log names (default: false).
    pub redact_names: bool,

    /// Whether to redact error messages and stack traces (default: true).
    #[serde(default = "default_true")]
    pub redact_errors: bool,
}

const fn default_true() -> bool {
    true
}

/// A single redaction rule.
#[derive(Debug, Clone, Deserialize)]
pub struct RedactionRule {
    /// Rule name for logging and metrics.
    pub name: String,

    /// Signal types this rule applies to.
    /// Empty means all signals.
    #[serde(default)]
    pub signals: Vec<SignalFilter>,

    /// Attribute scopes this rule applies to.
    /// Empty means all scopes.
    #[serde(default)]
    pub scopes: Vec<AttributeScope>,

    /// How to match attributes.
    #[serde(rename = "match")]
    pub matcher: MatcherConfig,

    /// Action to perform on matches.
    pub action: ActionConfig,
}

/// Signal type filter for rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SignalFilter {
    Traces,
    Metrics,
    Logs,
}

impl SignalFilter {
    pub const fn matches(&self, signal: Signal) -> bool {
        matches!(
            (self, signal),
            (Self::Traces, Signal::Traces)
                | (Self::Metrics, Signal::Metrics)
                | (Self::Logs, Signal::Logs)
        )
    }
}

/// Attribute scope for targeted redaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AttributeScope {
    /// Resource-level attributes (service.name, etc.)
    Resource,
    /// Scope/instrumentation attributes
    Scope,
    /// Span attributes (traces only)
    Span,
    /// Log record attributes (logs only)
    Log,
    /// Metric metadata attributes (metrics only)
    Metric,
    /// Data point attributes (metrics only)
    DataPoint,
}

/// Matcher configuration for identifying attributes to redact.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum MatcherConfig {
    /// Match by exact attribute key name.
    Attribute {
        /// Attribute keys to match (case-sensitive).
        keys: Vec<String>,
    },

    /// Match by glob pattern on attribute key.
    Glob {
        /// Glob pattern (e.g., "http.request.header.*").
        pattern: String,
    },

    /// Match by regex pattern on attribute value.
    Pattern {
        /// Regex pattern or "builtin:name" for built-in patterns.
        regex: String,
    },

    /// All matchers must match.
    All { matchers: Vec<Self> },

    /// Any matcher can match.
    Any { matchers: Vec<Self> },
}

/// Action to perform on matched attributes.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ActionConfig {
    /// Remove the attribute entirely.
    Drop,

    /// Replace value with HMAC-SHA256 hash.
    /// Produces deterministic output for the same input (allows correlation).
    Hash,

    /// Replace value with a placeholder string.
    Redact {
        /// Placeholder text (default: "[REDACTED]").
        #[serde(default = "default_placeholder")]
        placeholder: String,
    },
}

fn default_placeholder() -> String {
    "[REDACTED]".to_owned()
}

impl RedactionRule {
    /// Check if this rule applies to the given signal type.
    pub fn matches_signal(&self, signal: Signal) -> bool {
        if self.signals.is_empty() {
            return true;
        }
        self.signals.iter().any(|f| f.matches(signal))
    }

    /// Check if this rule applies to the given attribute scope.
    pub fn matches_scope(&self, scope: AttributeScope) -> bool {
        if self.scopes.is_empty() {
            return true;
        }
        self.scopes.contains(&scope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialise_drop_rule() {
        let toml = r#"
            name = "drop-auth"
            signals = ["traces"]
            scopes = ["span"]
            match = { type = "glob", pattern = "http.request.header.authorization" }
            action = { type = "drop" }
        "#;

        let rule: RedactionRule = toml::from_str(toml).unwrap();
        assert_eq!(rule.name, "drop-auth");
        assert!(matches!(rule.action, ActionConfig::Drop));
    }

    #[test]
    fn deserialise_hash_rule() {
        let toml = r#"
            name = "hash-emails"
            match = { type = "pattern", regex = "builtin:email" }
            action = { type = "hash" }
        "#;

        let rule: RedactionRule = toml::from_str(toml).unwrap();
        assert_eq!(rule.name, "hash-emails");
        assert!(matches!(rule.action, ActionConfig::Hash));
    }

    #[test]
    fn deserialise_redact_rule() {
        let toml = r#"
            name = "redact-cards"
            match = { type = "pattern", regex = "\\d{16}" }
            action = { type = "redact", placeholder = "[CARD]" }
        "#;

        let rule: RedactionRule = toml::from_str(toml).unwrap();
        assert!(matches!(
            rule.action,
            ActionConfig::Redact { placeholder } if placeholder == "[CARD]"
        ));
    }

    #[test]
    fn deserialise_composite_matcher() {
        let toml = r#"
            name = "complex"
            match = { type = "all", matchers = [
                { type = "glob", pattern = "http.*" },
                { type = "pattern", regex = "secret" }
            ]}
            action = { type = "drop" }
        "#;

        let rule: RedactionRule = toml::from_str(toml).unwrap();
        assert!(matches!(rule.matcher, MatcherConfig::All { .. }));
    }

    #[test]
    fn signal_filter_matching() {
        let rule = RedactionRule {
            name: "test".to_string(),
            signals: vec![SignalFilter::Traces],
            scopes: vec![],
            matcher: MatcherConfig::Attribute {
                keys: vec!["test".to_string()],
            },
            action: ActionConfig::Drop,
        };

        assert!(rule.matches_signal(Signal::Traces));
        assert!(!rule.matches_signal(Signal::Metrics));
        assert!(!rule.matches_signal(Signal::Logs));
    }

    #[test]
    fn empty_signals_matches_all() {
        let rule = RedactionRule {
            name: "test".to_string(),
            signals: vec![],
            scopes: vec![],
            matcher: MatcherConfig::Attribute {
                keys: vec!["test".to_string()],
            },
            action: ActionConfig::Drop,
        };

        assert!(rule.matches_signal(Signal::Traces));
        assert!(rule.matches_signal(Signal::Metrics));
        assert!(rule.matches_signal(Signal::Logs));
    }
}
