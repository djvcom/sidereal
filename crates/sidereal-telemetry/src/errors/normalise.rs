//! Normalisation functions for error fingerprinting.
//!
//! These functions strip variable data (line numbers, memory addresses, UUIDs,
//! timestamps, numeric IDs) from error messages and stacktraces to enable
//! grouping of similar errors.

use regex::Regex;
use std::sync::LazyLock;

/// Compiled regex patterns for normalisation.
struct NormalisationPatterns {
    /// Matches line numbers in file paths: `file.rs:123` or `file.rs:123:45`
    line_numbers: Regex,
    /// Matches memory addresses: `0x7fff5fbfe000`
    memory_addresses: Regex,
    /// Matches UUIDs: `550e8400-e29b-41d4-a716-446655440000`
    uuids: Regex,
    /// Matches ISO 8601 timestamps: `2024-01-15T10:30:00Z`
    timestamps: Regex,
    /// Matches numeric IDs in common patterns: `id=12345`, `user_123`
    numeric_ids: Regex,
    /// Matches IP addresses (v4)
    ipv4_addresses: Regex,
    /// Matches IP addresses (v6)
    ipv6_addresses: Regex,
    /// Matches request IDs and correlation IDs
    request_ids: Regex,
    /// Matches port numbers in URLs/addresses
    port_numbers: Regex,
}

fn build_patterns() -> Option<NormalisationPatterns> {
    Some(NormalisationPatterns {
        line_numbers: Regex::new(r":(\d+)(:\d+)?([\s\)\]>]|$)").ok()?,
        memory_addresses: Regex::new(r"0x[0-9a-fA-F]{4,16}").ok()?,
        uuids: Regex::new(
            r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
        )
        .ok()?,
        timestamps: Regex::new(
            r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?",
        )
        .ok()?,
        numeric_ids: Regex::new(r"(?i)(id[=:_]?|#)\d+").ok()?,
        ipv4_addresses: Regex::new(r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}").ok()?,
        ipv6_addresses: Regex::new(r"[0-9a-fA-F]{1,4}(:[0-9a-fA-F]{1,4}){7}").ok()?,
        request_ids: Regex::new(
            r"(?i)(request[_-]?id|correlation[_-]?id|trace[_-]?id|span[_-]?id)[=:]\s*[0-9a-fA-F]{16,64}",
        )
        .ok()?,
        port_numbers: Regex::new(r":(\d{2,5})(/|\s|$)").ok()?,
    })
}

#[allow(clippy::incompatible_msrv)]
static PATTERNS: LazyLock<Option<NormalisationPatterns>> = LazyLock::new(build_patterns);

/// Normalise a stacktrace for fingerprinting.
///
/// Removes variable data that would cause similar errors to have different
/// fingerprints:
/// - Line numbers (file.rs:123 -> file.rs)
/// - Memory addresses (0x7fff5fbfe000 -> 0xADDR)
/// - UUIDs
/// - Timestamps
/// - Numeric IDs
///
/// # Arguments
///
/// * `stacktrace` - The raw stacktrace string
/// * `max_frames` - Maximum number of stack frames to include (0 = unlimited)
///
/// # Returns
///
/// Normalised stacktrace suitable for fingerprinting.
#[must_use]
pub fn normalise_stacktrace(stacktrace: &str, max_frames: usize) -> String {
    let Some(patterns) = PATTERNS.as_ref() else {
        return stacktrace.to_owned();
    };

    let mut result = stacktrace.to_owned();

    // Apply normalisation patterns
    result = patterns.line_numbers.replace_all(&result, "$3").to_string();
    result = patterns
        .memory_addresses
        .replace_all(&result, "0xADDR")
        .to_string();
    result = patterns.uuids.replace_all(&result, "<UUID>").to_string();
    result = patterns
        .timestamps
        .replace_all(&result, "<TIMESTAMP>")
        .to_string();
    result = patterns
        .numeric_ids
        .replace_all(&result, "${1}<ID>")
        .to_string();
    result = patterns
        .ipv4_addresses
        .replace_all(&result, "<IPV4>")
        .to_string();
    result = patterns
        .ipv6_addresses
        .replace_all(&result, "<IPV6>")
        .to_string();
    result = patterns
        .request_ids
        .replace_all(&result, "${1}=<ID>")
        .to_string();
    result = patterns
        .port_numbers
        .replace_all(&result, ":<PORT>$2")
        .to_string();

    // Limit frames if requested
    if max_frames > 0 {
        result = limit_stack_frames(&result, max_frames);
    }

    // Normalise whitespace
    result = normalise_whitespace(&result);

    result
}

/// Normalise an error message for fingerprinting.
///
/// Removes variable data that would cause similar errors to have different
/// fingerprints.
#[must_use]
pub fn normalise_message(message: &str) -> String {
    let Some(patterns) = PATTERNS.as_ref() else {
        return message.to_owned();
    };

    let mut result = message.to_owned();

    // Apply normalisation patterns (subset appropriate for messages)
    result = patterns.uuids.replace_all(&result, "<UUID>").to_string();
    result = patterns
        .timestamps
        .replace_all(&result, "<TIMESTAMP>")
        .to_string();
    result = patterns
        .numeric_ids
        .replace_all(&result, "${1}<ID>")
        .to_string();
    result = patterns
        .ipv4_addresses
        .replace_all(&result, "<IPV4>")
        .to_string();
    result = patterns
        .ipv6_addresses
        .replace_all(&result, "<IPV6>")
        .to_string();
    result = patterns
        .request_ids
        .replace_all(&result, "${1}=<ID>")
        .to_string();

    // Normalise whitespace
    result = normalise_whitespace(&result);

    result
}

/// Limit stacktrace to the top N frames.
fn limit_stack_frames(stacktrace: &str, max_frames: usize) -> String {
    // Common frame delimiters across languages
    let frame_patterns = [
        "\n    at ",   // Java, JavaScript, TypeScript
        "\n   at ",    // .NET
        "\n  at ",     // Various
        "\n\tat ",     // Java with tabs
        "\nFile \"",   // Python
        "\n  File \"", // Python indented
        "\n    ",      // Generic indented lines (Rust, Go)
    ];

    // Find which pattern matches best
    for pattern in &frame_patterns {
        let parts: Vec<&str> = stacktrace.split(pattern).collect();
        if parts.len() > 1 {
            // Found frame delimiter
            let limited: Vec<&str> = parts.iter().take(max_frames + 1).copied().collect();
            if limited.len() < parts.len() {
                return format!(
                    "{}... ({} more frames)",
                    limited.join(pattern),
                    parts.len() - limited.len()
                );
            }
            return limited.join(pattern);
        }
    }

    // No frame pattern found, just return as-is
    stacktrace.to_owned()
}

/// Normalise whitespace (collapse multiple spaces/newlines).
fn normalise_whitespace(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut prev_whitespace = false;

    for c in s.chars() {
        if c.is_whitespace() {
            if !prev_whitespace {
                result.push(' ');
            }
            prev_whitespace = true;
        } else {
            result.push(c);
            prev_whitespace = false;
        }
    }

    result.trim().to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalise_line_numbers() {
        let input = "at foo (file.rs:123)\nat bar (other.rs:456:78)";
        let result = normalise_stacktrace(input, 0);
        assert!(!result.contains(":123"));
        assert!(!result.contains(":456"));
        assert!(result.contains("file.rs"));
        assert!(result.contains("other.rs"));
    }

    #[test]
    fn normalise_memory_addresses() {
        let input = "crash at 0x7fff5fbfe000 in module";
        let result = normalise_stacktrace(input, 0);
        assert!(result.contains("0xADDR"));
        assert!(!result.contains("7fff5fbfe000"));
    }

    #[test]
    fn normalise_uuids() {
        let input = "Failed to process request 550e8400-e29b-41d4-a716-446655440000";
        let result = normalise_message(input);
        assert!(result.contains("<UUID>"));
        assert!(!result.contains("550e8400"));
    }

    #[test]
    fn normalise_timestamps() {
        let input = "Error at 2024-01-15T10:30:00Z: connection timeout";
        let result = normalise_message(input);
        assert!(result.contains("<TIMESTAMP>"));
        assert!(!result.contains("2024-01-15"));
    }

    #[test]
    fn normalise_numeric_ids() {
        let input = "User id=12345 not found";
        let result = normalise_message(input);
        assert!(result.contains("<ID>"));
        assert!(!result.contains("12345"));
    }

    #[test]
    fn normalise_ipv4() {
        let input = "Connection to 192.168.1.100 failed";
        let result = normalise_message(input);
        assert!(result.contains("<IPV4>"));
        assert!(!result.contains("192.168.1.100"));
    }

    #[test]
    fn normalise_request_ids() {
        let input = "request_id=abc123def456789012345678 failed";
        let result = normalise_message(input);
        assert!(result.contains("<ID>"));
        assert!(!result.contains("abc123def456789012345678"));
    }

    #[test]
    fn limit_frames_java_style() {
        let input = "Exception\n    at com.example.Foo.bar(Foo.java)\n    at com.example.Baz.qux(Baz.java)\n    at com.example.Main.main(Main.java)";
        let result = normalise_stacktrace(input, 2);
        assert!(result.contains("Foo.bar"));
        assert!(result.contains("Baz.qux"));
        assert!(result.contains("more frames"));
    }

    #[test]
    fn same_error_different_ids_same_result() {
        let msg1 = "User id=12345 not found at 2024-01-15T10:30:00Z";
        let msg2 = "User id=67890 not found at 2024-02-20T15:45:00Z";

        let norm1 = normalise_message(msg1);
        let norm2 = normalise_message(msg2);

        assert_eq!(norm1, norm2);
    }

    #[test]
    fn whitespace_normalisation() {
        let input = "Error   with   multiple\n\n\nspaces";
        let result = normalise_whitespace(input);
        assert_eq!(result, "Error with multiple spaces");
    }
}
