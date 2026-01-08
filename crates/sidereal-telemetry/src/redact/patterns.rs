//! Built-in regex patterns for common PII types.
//!
//! These patterns are pre-validated and optimised for the Rust regex crate.
//! Reference them in config with "builtin:name" syntax.

use std::collections::HashMap;
use std::sync::LazyLock;

use regex::Regex;

/// Email address pattern.
/// Matches most common email formats.
pub const EMAIL_PATTERN: &str = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b";

/// Credit card number pattern.
/// Matches 16-digit card numbers with optional separators.
pub const CREDIT_CARD_PATTERN: &str = r"\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b";

/// US Social Security Number pattern.
/// Matches XXX-XX-XXXX format.
pub const SSN_PATTERN: &str = r"\b\d{3}-\d{2}-\d{4}\b";

/// IPv4 address pattern.
pub const IPV4_PATTERN: &str = r"\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b";

/// IPv6 address pattern (simplified).
pub const IPV6_PATTERN: &str = r"\b(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}\b";

/// Phone number pattern (US format).
/// Matches various US phone formats.
pub const PHONE_US_PATTERN: &str =
    r"\b(?:\+1[-.\s]?)?(?:\(?[0-9]{3}\)?[-.\s]?)?[0-9]{3}[-.\s]?[0-9]{4}\b";

/// JWT token pattern.
/// Matches the three-part base64url structure.
pub const JWT_PATTERN: &str = r"\beyJ[A-Za-z0-9_-]*\.eyJ[A-Za-z0-9_-]*\.[A-Za-z0-9_-]*\b";

/// AWS access key ID pattern.
pub const AWS_ACCESS_KEY_PATTERN: &str = r"\b(?:AKIA|ABIA|ACCA|ASIA)[0-9A-Z]{16}\b";

/// Generic API key pattern (common formats).
pub const API_KEY_PATTERN: &str = r"\b(?:sk|pk|api|key)[-_][A-Za-z0-9]{20,}\b";

/// Map of built-in pattern names to their regex strings.
#[allow(clippy::incompatible_msrv)]
pub static BUILTIN_PATTERNS: LazyLock<HashMap<&'static str, &'static str>> = LazyLock::new(|| {
    let mut m = HashMap::new();
    m.insert("email", EMAIL_PATTERN);
    m.insert("credit_card", CREDIT_CARD_PATTERN);
    m.insert("ssn", SSN_PATTERN);
    m.insert("ipv4", IPV4_PATTERN);
    m.insert("ipv6", IPV6_PATTERN);
    m.insert("phone_us", PHONE_US_PATTERN);
    m.insert("jwt", JWT_PATTERN);
    m.insert("aws_access_key", AWS_ACCESS_KEY_PATTERN);
    m.insert("api_key", API_KEY_PATTERN);
    m
});

/// Resolve a pattern string, expanding "builtin:name" references.
pub fn resolve_pattern(pattern: &str) -> Result<&str, PatternError> {
    if let Some(name) = pattern.strip_prefix("builtin:") {
        BUILTIN_PATTERNS
            .get(name)
            .copied()
            .ok_or_else(|| PatternError::UnknownBuiltin(name.to_owned()))
    } else {
        Ok(pattern)
    }
}

/// Compile a pattern string to a Regex, expanding built-in references.
pub fn compile_pattern(pattern: &str) -> Result<Regex, PatternError> {
    let resolved = resolve_pattern(pattern)?;
    Regex::new(resolved).map_err(PatternError::InvalidRegex)
}

/// Error resolving or compiling a pattern.
#[derive(Debug, thiserror::Error)]
pub enum PatternError {
    #[error("unknown built-in pattern: {0}")]
    UnknownBuiltin(String),

    #[error("invalid regex pattern: {0}")]
    InvalidRegex(#[from] regex::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn email_pattern_matches() {
        let re = Regex::new(EMAIL_PATTERN).unwrap();
        assert!(re.is_match("user@example.com"));
        assert!(re.is_match("user.name+tag@sub.domain.co.uk"));
        assert!(!re.is_match("not-an-email"));
        assert!(!re.is_match("@missing-local.com"));
    }

    #[test]
    fn credit_card_pattern_matches() {
        let re = Regex::new(CREDIT_CARD_PATTERN).unwrap();
        assert!(re.is_match("4111111111111111"));
        assert!(re.is_match("4111-1111-1111-1111"));
        assert!(re.is_match("4111 1111 1111 1111"));
        assert!(!re.is_match("411111111111")); // Too short
    }

    #[test]
    fn ssn_pattern_matches() {
        let re = Regex::new(SSN_PATTERN).unwrap();
        assert!(re.is_match("123-45-6789"));
        assert!(!re.is_match("123456789")); // No dashes
    }

    #[test]
    fn ipv4_pattern_matches() {
        let re = Regex::new(IPV4_PATTERN).unwrap();
        assert!(re.is_match("192.168.1.1"));
        assert!(re.is_match("10.0.0.1"));
        assert!(re.is_match("255.255.255.255"));
        assert!(!re.is_match("256.1.1.1")); // Invalid octet
    }

    #[test]
    fn jwt_pattern_matches() {
        let re = Regex::new(JWT_PATTERN).unwrap();
        // Simplified JWT structure
        assert!(re.is_match("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signature"));
    }

    #[test]
    fn resolve_builtin_pattern() {
        assert_eq!(resolve_pattern("builtin:email").unwrap(), EMAIL_PATTERN);
        assert_eq!(
            resolve_pattern("builtin:credit_card").unwrap(),
            CREDIT_CARD_PATTERN
        );
    }

    #[test]
    fn resolve_custom_pattern() {
        let custom = r"\btest\b";
        assert_eq!(resolve_pattern(custom).unwrap(), custom);
    }

    #[test]
    fn resolve_unknown_builtin_fails() {
        let err = resolve_pattern("builtin:unknown").unwrap_err();
        assert!(matches!(err, PatternError::UnknownBuiltin(_)));
    }

    #[test]
    fn compile_builtin_pattern() {
        let re = compile_pattern("builtin:email").unwrap();
        assert!(re.is_match("test@example.com"));
    }
}
