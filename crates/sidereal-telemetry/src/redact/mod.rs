//! PII redaction pipeline for telemetry data.
//!
//! This module provides configurable redaction of sensitive data in OTLP
//! telemetry before it is converted to Arrow format and stored.
//!
//! # Architecture
//!
//! Redaction occurs post-proto parsing, pre-Arrow conversion:
//!
//! ```text
//! OTLP Proto → RedactionEngine → Arrow Conversion → Buffer → Parquet
//! ```
//!
//! # Configuration
//!
//! ```toml
//! [redaction]
//! enabled = true
//! key_file = "/etc/sidereal/redaction.key"
//!
//! [[redaction.rules]]
//! name = "drop-auth-headers"
//! matcher = { type = "glob", pattern = "http.request.header.authorization" }
//! action = { type = "drop" }
//!
//! [[redaction.rules]]
//! name = "hash-emails"
//! matcher = { type = "pattern", regex = "builtin:email" }
//! action = { type = "hash" }
//! ```
//!
//! # Security
//!
//! - Uses HMAC-SHA256 for hashing (not plain SHA-256) to prevent rainbow table attacks
//! - Unicode NFC normalisation ensures consistent hashing across representations
//! - Key files must have mode 0600 (owner read/write only)

pub mod actions;
pub mod config;
pub mod matchers;
pub mod patterns;

use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;

use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use secrecy::SecretBox;
use tracing::{debug, info, instrument, warn};

use self::actions::{ActionResult, CompiledAction};
pub use self::config::{
    ActionConfig, AttributeScope, MatcherConfig, RedactionConfig, RedactionRule, SignalFilter,
};
use self::matchers::{CompiledMatcher, MatcherError};
use self::patterns::PatternError;
use crate::storage::Signal;

/// A compiled redaction rule ready for evaluation.
#[derive(Debug)]
struct CompiledRule {
    name: String,
    signals: Vec<SignalFilter>,
    scopes: Vec<AttributeScope>,
    matcher: CompiledMatcher,
    action: CompiledAction,
}

impl CompiledRule {
    fn compile(rule: &RedactionRule) -> Result<Self, RedactionError> {
        Ok(Self {
            name: rule.name.clone(),
            signals: rule.signals.clone(),
            scopes: rule.scopes.clone(),
            matcher: CompiledMatcher::compile(&rule.matcher)?,
            action: CompiledAction::compile(&rule.action),
        })
    }

    fn matches_signal(&self, signal: Signal) -> bool {
        if self.signals.is_empty() {
            return true;
        }
        self.signals.iter().any(|f| f.matches(signal))
    }

    fn matches_scope(&self, scope: AttributeScope) -> bool {
        if self.scopes.is_empty() {
            return true;
        }
        self.scopes.contains(&scope)
    }
}

/// Statistics from a redaction operation.
#[derive(Debug, Default, Clone)]
pub struct RedactionStats {
    /// Number of attributes processed.
    pub attributes_processed: u64,
    /// Number of attributes dropped.
    pub attributes_dropped: u64,
    /// Number of attributes hashed.
    pub attributes_hashed: u64,
    /// Number of attributes redacted (replaced with placeholder).
    pub attributes_redacted: u64,
    /// Rules that matched, with counts.
    pub rules_matched: Vec<(String, u64)>,
}

impl RedactionStats {
    /// Total number of attributes modified.
    pub const fn total_modified(&self) -> u64 {
        self.attributes_dropped + self.attributes_hashed + self.attributes_redacted
    }

    /// Merge another stats instance into this one.
    pub fn merge(&mut self, other: &Self) {
        self.attributes_processed += other.attributes_processed;
        self.attributes_dropped += other.attributes_dropped;
        self.attributes_hashed += other.attributes_hashed;
        self.attributes_redacted += other.attributes_redacted;

        for (rule, count) in &other.rules_matched {
            if let Some((_, existing)) = self.rules_matched.iter_mut().find(|(n, _)| n == rule) {
                *existing += count;
            } else {
                self.rules_matched.push((rule.clone(), *count));
            }
        }
    }

    fn record_action(&mut self, action: &CompiledAction, rule_name: &str) {
        match action {
            CompiledAction::Drop => self.attributes_dropped += 1,
            CompiledAction::Hash => self.attributes_hashed += 1,
            CompiledAction::Redact { .. } => self.attributes_redacted += 1,
        }

        if let Some((_, count)) = self.rules_matched.iter_mut().find(|(n, _)| n == rule_name) {
            *count += 1;
        } else {
            self.rules_matched.push((rule_name.to_owned(), 1));
        }
    }
}

impl std::fmt::Display for RedactionStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "processed={}, dropped={}, hashed={}, redacted={}",
            self.attributes_processed,
            self.attributes_dropped,
            self.attributes_hashed,
            self.attributes_redacted
        )
    }
}

/// The redaction engine applies configured rules to OTLP attributes.
#[derive(Debug)]
pub struct RedactionEngine {
    rules: Vec<CompiledRule>,
    hmac_key: Option<SecretBox<Vec<u8>>>,
    redact_names: bool,
    redact_errors: bool,
}

impl RedactionEngine {
    /// Create a new redaction engine from configuration.
    pub fn new(config: &RedactionConfig) -> Result<Self, RedactionError> {
        if !config.enabled {
            return Ok(Self {
                rules: Vec::new(),
                hmac_key: None,
                redact_names: false,
                redact_errors: false,
            });
        }

        // Compile all rules
        let rules: Result<Vec<_>, _> = config.rules.iter().map(CompiledRule::compile).collect();
        let rules = rules?;

        // Check if any rule requires HMAC key
        let needs_key = rules.iter().any(|r| r.action.requires_key());

        // Load HMAC key if needed
        let hmac_key = if needs_key {
            Some(load_hmac_key(config)?)
        } else {
            None
        };

        Ok(Self {
            rules,
            hmac_key,
            redact_names: config.redact_names,
            redact_errors: config.redact_errors,
        })
    }

    /// Create a disabled redaction engine (no-op).
    pub const fn disabled() -> Self {
        Self {
            rules: Vec::new(),
            hmac_key: None,
            redact_names: false,
            redact_errors: false,
        }
    }

    /// Check if redaction is enabled.
    pub fn is_enabled(&self) -> bool {
        !self.rules.is_empty()
    }

    /// Redact attributes in place.
    ///
    /// Applies all matching rules to the attributes, modifying them in place.
    /// Attributes marked for dropping are removed from the vector.
    #[instrument(skip(self, attributes), fields(signal = ?signal, scope = ?scope))]
    pub fn redact_attributes(
        &self,
        attributes: &mut Vec<KeyValue>,
        signal: Signal,
        scope: AttributeScope,
    ) -> RedactionStats {
        let mut stats = RedactionStats::default();

        if self.rules.is_empty() {
            return stats;
        }

        // Filter rules applicable to this signal and scope
        let applicable_rules: Vec<_> = self
            .rules
            .iter()
            .filter(|r| r.matches_signal(signal) && r.matches_scope(scope))
            .collect();

        if applicable_rules.is_empty() {
            return stats;
        }

        // Process attributes, marking indices to remove
        let mut indices_to_remove = Vec::new();

        for (idx, kv) in attributes.iter_mut().enumerate() {
            stats.attributes_processed += 1;

            let key = &kv.key;
            let value_str = extract_string_value(&kv.value);

            for rule in &applicable_rules {
                if rule.matcher.matches(key, &value_str) {
                    match rule.action.apply(&value_str, self.hmac_key.as_ref()) {
                        ActionResult::Drop => {
                            indices_to_remove.push(idx);
                            stats.record_action(&rule.action, &rule.name);
                            debug!(key = %key, rule = %rule.name, "dropping attribute");
                            break; // No need to apply more rules
                        }
                        ActionResult::Replace(new_value) => {
                            replace_value(&mut kv.value, new_value);
                            stats.record_action(&rule.action, &rule.name);
                            debug!(key = %key, rule = %rule.name, "redacted attribute");
                            break; // No need to apply more rules
                        }
                        ActionResult::Error(err) => {
                            warn!(key = %key, rule = %rule.name, error = %err, "redaction error");
                        }
                    }
                }
            }
        }

        // Remove dropped attributes (in reverse order to preserve indices)
        for idx in indices_to_remove.into_iter().rev() {
            attributes.remove(idx);
        }

        // Log summary if any attributes were modified
        if stats.total_modified() > 0 {
            info!(
                processed = stats.attributes_processed,
                dropped = stats.attributes_dropped,
                hashed = stats.attributes_hashed,
                redacted = stats.attributes_redacted,
                "Redaction completed"
            );
        }

        stats
    }

    /// Check if any attribute would be affected by redaction rules.
    ///
    /// This allows callers to avoid cloning attributes when no redaction
    /// would actually occur. Returns true if at least one attribute matches
    /// a redaction rule for the given signal and scope.
    pub fn would_redact_any(
        &self,
        attributes: &[KeyValue],
        signal: Signal,
        scope: AttributeScope,
    ) -> bool {
        if self.rules.is_empty() {
            return false;
        }

        // Filter rules applicable to this signal and scope
        let applicable_rules: Vec<_> = self
            .rules
            .iter()
            .filter(|r| r.matches_signal(signal) && r.matches_scope(scope))
            .collect();

        if applicable_rules.is_empty() {
            return false;
        }

        // Check if any attribute matches any rule
        for kv in attributes {
            let key = &kv.key;
            let value_str = extract_string_value(&kv.value);

            for rule in &applicable_rules {
                if rule.matcher.matches(key, &value_str) {
                    return true;
                }
            }
        }

        false
    }

    /// Redact a string value (for span names, log bodies, etc.)
    ///
    /// Only applies pattern-based rules that match values.
    #[instrument(skip(self, value), fields(signal = ?signal))]
    pub fn redact_string(&self, value: &str, signal: Signal) -> (String, bool) {
        if self.rules.is_empty() {
            return (value.to_owned(), false);
        }

        let mut result = value.to_owned();
        let mut modified = false;

        for rule in &self.rules {
            if !rule.matches_signal(signal) {
                continue;
            }

            // Only pattern matchers apply to string values
            if rule.matcher.matches("", &result) {
                match rule.action.apply(&result, self.hmac_key.as_ref()) {
                    ActionResult::Drop => {
                        // For strings, drop means replace with empty or placeholder
                        "[DROPPED]".clone_into(&mut result);
                        modified = true;
                        break;
                    }
                    ActionResult::Replace(new_value) => {
                        result = new_value;
                        modified = true;
                        break;
                    }
                    ActionResult::Error(err) => {
                        warn!(rule = %rule.name, error = %err, "string redaction error");
                    }
                }
            }
        }

        (result, modified)
    }

    /// Check if name redaction is enabled.
    pub fn should_redact_names(&self) -> bool {
        self.redact_names && self.is_enabled()
    }

    /// Check if error redaction is enabled.
    pub fn should_redact_errors(&self) -> bool {
        self.redact_errors && self.is_enabled()
    }
}

/// Extract a string representation from an AnyValue.
#[allow(clippy::ref_option)]
fn extract_string_value(value: &Option<AnyValue>) -> String {
    let Some(av) = value else {
        return String::new();
    };

    let Some(ref inner) = av.value else {
        return String::new();
    };

    match inner {
        any_value::Value::StringValue(s) => s.clone(),
        any_value::Value::IntValue(i) => i.to_string(),
        any_value::Value::DoubleValue(d) => d.to_string(),
        any_value::Value::BoolValue(b) => b.to_string(),
        any_value::Value::BytesValue(b) => String::from_utf8_lossy(b).into_owned(),
        any_value::Value::ArrayValue(_) => "[array]".to_owned(),
        any_value::Value::KvlistValue(_) => "[kvlist]".to_owned(),
    }
}

/// Replace the value in an AnyValue with a new string.
fn replace_value(value: &mut Option<AnyValue>, new_value: String) {
    *value = Some(AnyValue {
        value: Some(any_value::Value::StringValue(new_value)),
    });
}

/// Load HMAC key from configuration.
fn load_hmac_key(config: &RedactionConfig) -> Result<SecretBox<Vec<u8>>, RedactionError> {
    // Try environment variable first
    if let Some(ref env_var) = config.key_env {
        if let Ok(key_base64) = std::env::var(env_var) {
            let key_bytes =
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &key_base64)
                    .map_err(|e| RedactionError::InvalidKey(format!("invalid base64: {e}")))?;

            if key_bytes.len() < 32 {
                return Err(RedactionError::InvalidKey(
                    "HMAC key must be at least 32 bytes".to_owned(),
                ));
            }

            return Ok(SecretBox::new(Box::new(key_bytes)));
        }
    }

    // Fall back to key file
    if let Some(ref key_path) = config.key_file {
        return load_key_file(key_path);
    }

    Err(RedactionError::MissingKey)
}

/// Load HMAC key from a file with permission checks.
fn load_key_file(path: &Path) -> Result<SecretBox<Vec<u8>>, RedactionError> {
    // Check file permissions on Unix
    #[cfg(unix)]
    {
        let metadata = fs::metadata(path)
            .map_err(|e| RedactionError::InvalidKey(format!("cannot read key file: {e}")))?;

        let mode = metadata.permissions().mode();
        if mode & 0o077 != 0 {
            return Err(RedactionError::InsecureKeyFile(format!(
                "key file {} has insecure permissions {mode:o} (must be 0600)",
                path.display()
            )));
        }
    }

    let key_bytes = fs::read(path)
        .map_err(|e| RedactionError::InvalidKey(format!("cannot read key file: {e}")))?;

    if key_bytes.len() < 32 {
        return Err(RedactionError::InvalidKey(
            "HMAC key must be at least 32 bytes".to_owned(),
        ));
    }

    Ok(SecretBox::new(Box::new(key_bytes)))
}

/// Error during redaction configuration or execution.
#[derive(Debug, thiserror::Error)]
pub enum RedactionError {
    #[error("invalid matcher: {0}")]
    Matcher(#[from] MatcherError),

    #[error("pattern error: {0}")]
    Pattern(#[from] PatternError),

    #[error("missing HMAC key for hash action")]
    MissingKey,

    #[error("invalid HMAC key: {0}")]
    InvalidKey(String),

    #[error("insecure key file: {0}")]
    InsecureKeyFile(String),
}

/// Create a shareable redaction engine.
pub fn create_redaction_engine(
    config: &RedactionConfig,
) -> Result<Arc<RedactionEngine>, RedactionError> {
    Ok(Arc::new(RedactionEngine::new(config)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config_with_rules(rules: Vec<RedactionRule>) -> RedactionConfig {
        RedactionConfig {
            enabled: true,
            key_file: None,
            key_env: None,
            rules,
            redact_names: false,
            redact_errors: true,
        }
    }

    fn make_kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.to_string())),
            }),
        }
    }

    #[test]
    fn disabled_engine_is_noop() {
        let engine = RedactionEngine::disabled();
        assert!(!engine.is_enabled());

        let mut attrs = vec![make_kv("secret", "value")];
        let stats = engine.redact_attributes(&mut attrs, Signal::Traces, AttributeScope::Span);

        assert_eq!(stats.total_modified(), 0);
        assert_eq!(attrs.len(), 1);
    }

    #[test]
    fn drop_action_removes_attribute() {
        let config = test_config_with_rules(vec![RedactionRule {
            name: "drop-secret".to_string(),
            signals: vec![],
            scopes: vec![],
            matcher: MatcherConfig::Attribute {
                keys: vec!["secret".to_string()],
            },
            action: ActionConfig::Drop,
        }]);

        let engine = RedactionEngine::new(&config).unwrap();
        let mut attrs = vec![
            make_kv("public", "hello"),
            make_kv("secret", "password123"),
            make_kv("also-public", "world"),
        ];

        let stats = engine.redact_attributes(&mut attrs, Signal::Traces, AttributeScope::Span);

        assert_eq!(stats.attributes_dropped, 1);
        assert_eq!(attrs.len(), 2);
        assert_eq!(attrs[0].key, "public");
        assert_eq!(attrs[1].key, "also-public");
    }

    #[test]
    fn redact_action_replaces_value() {
        let config = test_config_with_rules(vec![RedactionRule {
            name: "redact-secret".to_string(),
            signals: vec![],
            scopes: vec![],
            matcher: MatcherConfig::Attribute {
                keys: vec!["password".to_string()],
            },
            action: ActionConfig::Redact {
                placeholder: "[HIDDEN]".to_string(),
            },
        }]);

        let engine = RedactionEngine::new(&config).unwrap();
        let mut attrs = vec![make_kv("password", "super-secret")];

        let stats = engine.redact_attributes(&mut attrs, Signal::Traces, AttributeScope::Span);

        assert_eq!(stats.attributes_redacted, 1);
        assert_eq!(attrs.len(), 1);

        let value = extract_string_value(&attrs[0].value);
        assert_eq!(value, "[HIDDEN]");
    }

    #[test]
    fn signal_filter_restricts_rules() {
        let config = test_config_with_rules(vec![RedactionRule {
            name: "traces-only".to_string(),
            signals: vec![SignalFilter::Traces],
            scopes: vec![],
            matcher: MatcherConfig::Attribute {
                keys: vec!["secret".to_string()],
            },
            action: ActionConfig::Drop,
        }]);

        let engine = RedactionEngine::new(&config).unwrap();

        // Should apply to traces
        let mut trace_attrs = vec![make_kv("secret", "value")];
        engine.redact_attributes(&mut trace_attrs, Signal::Traces, AttributeScope::Span);
        assert_eq!(trace_attrs.len(), 0);

        // Should not apply to logs
        let mut log_attrs = vec![make_kv("secret", "value")];
        engine.redact_attributes(&mut log_attrs, Signal::Logs, AttributeScope::Log);
        assert_eq!(log_attrs.len(), 1);
    }

    #[test]
    fn scope_filter_restricts_rules() {
        let config = test_config_with_rules(vec![RedactionRule {
            name: "span-only".to_string(),
            signals: vec![],
            scopes: vec![AttributeScope::Span],
            matcher: MatcherConfig::Attribute {
                keys: vec!["secret".to_string()],
            },
            action: ActionConfig::Drop,
        }]);

        let engine = RedactionEngine::new(&config).unwrap();

        // Should apply to span scope
        let mut span_attrs = vec![make_kv("secret", "value")];
        engine.redact_attributes(&mut span_attrs, Signal::Traces, AttributeScope::Span);
        assert_eq!(span_attrs.len(), 0);

        // Should not apply to resource scope
        let mut resource_attrs = vec![make_kv("secret", "value")];
        engine.redact_attributes(
            &mut resource_attrs,
            Signal::Traces,
            AttributeScope::Resource,
        );
        assert_eq!(resource_attrs.len(), 1);
    }

    #[test]
    fn glob_pattern_matching() {
        let config = test_config_with_rules(vec![RedactionRule {
            name: "drop-headers".to_string(),
            signals: vec![],
            scopes: vec![],
            matcher: MatcherConfig::Glob {
                pattern: "http.request.header.*".to_string(),
            },
            action: ActionConfig::Drop,
        }]);

        let engine = RedactionEngine::new(&config).unwrap();
        let mut attrs = vec![
            make_kv("http.request.header.authorization", "Bearer token"),
            make_kv("http.request.header.cookie", "session=abc"),
            make_kv("http.request.method", "GET"),
        ];

        engine.redact_attributes(&mut attrs, Signal::Traces, AttributeScope::Span);

        assert_eq!(attrs.len(), 1);
        assert_eq!(attrs[0].key, "http.request.method");
    }

    #[test]
    fn pattern_matching_on_values() {
        let config = test_config_with_rules(vec![RedactionRule {
            name: "redact-emails".to_string(),
            signals: vec![],
            scopes: vec![],
            matcher: MatcherConfig::Pattern {
                regex: "builtin:email".to_string(),
            },
            action: ActionConfig::Redact {
                placeholder: "[EMAIL]".to_string(),
            },
        }]);

        let engine = RedactionEngine::new(&config).unwrap();
        let mut attrs = vec![
            make_kv("user.contact", "Email: user@example.com"),
            make_kv("user.name", "John Doe"),
        ];

        engine.redact_attributes(&mut attrs, Signal::Traces, AttributeScope::Span);

        assert_eq!(attrs.len(), 2);
        assert_eq!(extract_string_value(&attrs[0].value), "[EMAIL]");
        assert_eq!(extract_string_value(&attrs[1].value), "John Doe");
    }
}
