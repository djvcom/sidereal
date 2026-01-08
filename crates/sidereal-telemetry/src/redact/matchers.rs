//! Matcher implementations for attribute redaction.
//!
//! Matchers determine which attributes should be redacted based on
//! attribute keys, glob patterns, or regex patterns on values.

use std::collections::HashSet;

use regex::Regex;

use super::config::MatcherConfig;
use super::patterns::{compile_pattern, PatternError};

/// A compiled matcher ready for evaluation.
#[derive(Debug)]
pub enum CompiledMatcher {
    /// Match by exact attribute key name.
    Attribute(HashSet<String>),

    /// Match by glob pattern on attribute key.
    Glob(glob::Pattern),

    /// Match by regex pattern on attribute value.
    Pattern(Regex),

    /// All matchers must match.
    All(Vec<Self>),

    /// Any matcher can match.
    Any(Vec<Self>),
}

impl CompiledMatcher {
    /// Compile a matcher configuration into a ready-to-use matcher.
    pub fn compile(config: &MatcherConfig) -> Result<Self, MatcherError> {
        match config {
            MatcherConfig::Attribute { keys } => {
                let set: HashSet<String> = keys.iter().cloned().collect();
                Ok(Self::Attribute(set))
            }

            MatcherConfig::Glob { pattern } => {
                let glob = glob::Pattern::new(pattern).map_err(MatcherError::InvalidGlob)?;
                Ok(Self::Glob(glob))
            }

            MatcherConfig::Pattern { regex } => {
                let re = compile_pattern(regex)?;
                Ok(Self::Pattern(re))
            }

            MatcherConfig::All { matchers } => {
                let compiled: Result<Vec<_>, _> = matchers.iter().map(Self::compile).collect();
                Ok(Self::All(compiled?))
            }

            MatcherConfig::Any { matchers } => {
                let compiled: Result<Vec<_>, _> = matchers.iter().map(Self::compile).collect();
                Ok(Self::Any(compiled?))
            }
        }
    }

    /// Check if this matcher matches the given attribute.
    ///
    /// For `Attribute` and `Glob` matchers, only the key is checked.
    /// For `Pattern` matchers, the value is checked.
    /// For composite matchers, the logic is applied recursively.
    pub fn matches(&self, key: &str, value: &str) -> bool {
        match self {
            Self::Attribute(keys) => keys.contains(key),

            Self::Glob(pattern) => pattern.matches(key),

            Self::Pattern(regex) => regex.is_match(value),

            Self::All(matchers) => matchers.iter().all(|m| m.matches(key, value)),

            Self::Any(matchers) => matchers.iter().any(|m| m.matches(key, value)),
        }
    }

    /// Check if this is a key-only matcher (doesn't need to inspect values).
    ///
    /// Used for optimisation: key-only matchers can skip value extraction.
    pub fn is_key_only(&self) -> bool {
        match self {
            Self::Attribute(_) | Self::Glob(_) => true,
            Self::Pattern(_) => false,
            Self::All(matchers) | Self::Any(matchers) => matchers.iter().all(Self::is_key_only),
        }
    }
}

/// Error compiling a matcher.
#[derive(Debug, thiserror::Error)]
pub enum MatcherError {
    #[error("invalid glob pattern: {0}")]
    InvalidGlob(#[from] glob::PatternError),

    #[error("pattern error: {0}")]
    Pattern(#[from] PatternError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attribute_matcher_exact_match() {
        let matcher = CompiledMatcher::compile(&MatcherConfig::Attribute {
            keys: vec!["user.email".to_string(), "user.id".to_string()],
        })
        .unwrap();

        assert!(matcher.matches("user.email", "anything"));
        assert!(matcher.matches("user.id", "anything"));
        assert!(!matcher.matches("user.name", "anything"));
    }

    #[test]
    fn glob_matcher_wildcard() {
        let matcher = CompiledMatcher::compile(&MatcherConfig::Glob {
            pattern: "http.request.header.*".to_string(),
        })
        .unwrap();

        assert!(matcher.matches("http.request.header.authorization", ""));
        assert!(matcher.matches("http.request.header.cookie", ""));
        assert!(!matcher.matches("http.response.header.content-type", ""));
    }

    #[test]
    fn glob_matcher_question_mark() {
        let matcher = CompiledMatcher::compile(&MatcherConfig::Glob {
            pattern: "db.statement.?".to_string(),
        })
        .unwrap();

        assert!(matcher.matches("db.statement.1", ""));
        assert!(matcher.matches("db.statement.a", ""));
        assert!(!matcher.matches("db.statement.12", ""));
    }

    #[test]
    fn pattern_matcher_email() {
        let matcher = CompiledMatcher::compile(&MatcherConfig::Pattern {
            regex: "builtin:email".to_string(),
        })
        .unwrap();

        assert!(matcher.matches("any.key", "user@example.com"));
        assert!(matcher.matches("another.key", "Contact: admin@test.org for help"));
        assert!(!matcher.matches("key", "no email here"));
    }

    #[test]
    fn pattern_matcher_custom_regex() {
        let matcher = CompiledMatcher::compile(&MatcherConfig::Pattern {
            regex: r"secret-\d+".to_string(),
        })
        .unwrap();

        assert!(matcher.matches("key", "secret-12345"));
        assert!(!matcher.matches("key", "public-12345"));
    }

    #[test]
    fn all_matcher_requires_all() {
        let matcher = CompiledMatcher::compile(&MatcherConfig::All {
            matchers: vec![
                MatcherConfig::Glob {
                    pattern: "http.*".to_string(),
                },
                MatcherConfig::Pattern {
                    regex: r"Bearer".to_string(),
                },
            ],
        })
        .unwrap();

        // Both match
        assert!(matcher.matches("http.header.auth", "Bearer token123"));
        // Key matches but value doesn't
        assert!(!matcher.matches("http.header.auth", "Basic xyz"));
        // Value matches but key doesn't
        assert!(!matcher.matches("grpc.header.auth", "Bearer token123"));
    }

    #[test]
    fn any_matcher_requires_one() {
        let matcher = CompiledMatcher::compile(&MatcherConfig::Any {
            matchers: vec![
                MatcherConfig::Attribute {
                    keys: vec!["password".to_string()],
                },
                MatcherConfig::Pattern {
                    regex: "builtin:email".to_string(),
                },
            ],
        })
        .unwrap();

        // First matcher matches
        assert!(matcher.matches("password", "anything"));
        // Second matcher matches
        assert!(matcher.matches("any.key", "test@example.com"));
        // Neither matches
        assert!(!matcher.matches("username", "john"));
    }

    #[test]
    fn is_key_only_detection() {
        let attr_matcher = CompiledMatcher::compile(&MatcherConfig::Attribute {
            keys: vec!["test".to_string()],
        })
        .unwrap();
        assert!(attr_matcher.is_key_only());

        let glob_matcher = CompiledMatcher::compile(&MatcherConfig::Glob {
            pattern: "test.*".to_string(),
        })
        .unwrap();
        assert!(glob_matcher.is_key_only());

        let pattern_matcher = CompiledMatcher::compile(&MatcherConfig::Pattern {
            regex: "test".to_string(),
        })
        .unwrap();
        assert!(!pattern_matcher.is_key_only());
    }
}
