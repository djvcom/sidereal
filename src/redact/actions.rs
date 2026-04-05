//! Redaction action implementations.
//!
//! Actions define what happens to matched attributes: drop them entirely,
//! hash their values with HMAC-SHA256, or replace with a placeholder.

use hmac::{Hmac, Mac};
use secrecy::{ExposeSecret, SecretBox};
use sha2::Sha256;
use unicode_normalization::UnicodeNormalization;

use super::config::ActionConfig;

type HmacSha256 = Hmac<Sha256>;

/// A compiled action ready for execution.
#[derive(Debug, Clone)]
pub enum CompiledAction {
    /// Remove the attribute entirely.
    Drop,

    /// Replace value with HMAC-SHA256 hash.
    Hash,

    /// Replace value with a placeholder string.
    Redact { placeholder: String },
}

impl CompiledAction {
    /// Compile an action configuration.
    pub fn compile(config: &ActionConfig) -> Self {
        match config {
            ActionConfig::Drop => Self::Drop,
            ActionConfig::Hash => Self::Hash,
            ActionConfig::Redact { placeholder } => Self::Redact {
                placeholder: placeholder.clone(),
            },
        }
    }

    /// Apply this action to a value.
    ///
    /// Returns `None` if the attribute should be dropped.
    /// Returns `Some(new_value)` if the value should be replaced.
    pub fn apply(&self, value: &str, key: Option<&SecretBox<Vec<u8>>>) -> ActionResult {
        match self {
            Self::Drop => ActionResult::Drop,

            Self::Hash => {
                let Some(secret) = key else {
                    return ActionResult::Error("hash action requires HMAC key".to_owned());
                };

                match hash_value(value, secret) {
                    Some(hashed) => ActionResult::Replace(hashed),
                    None => ActionResult::Error("invalid HMAC key".to_owned()),
                }
            }

            Self::Redact { placeholder } => ActionResult::Replace(placeholder.clone()),
        }
    }

    /// Check if this action requires an HMAC key.
    pub const fn requires_key(&self) -> bool {
        matches!(self, Self::Hash)
    }
}

/// Result of applying an action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActionResult {
    /// Drop the attribute entirely.
    Drop,
    /// Replace the value with a new string.
    Replace(String),
    /// Action failed with an error.
    Error(String),
}

/// Hash a value using HMAC-SHA256 with Unicode normalisation.
///
/// The value is normalised to NFC form before hashing to ensure
/// consistent results across different Unicode representations.
///
/// Returns `None` if the HMAC key is invalid.
pub fn hash_value(value: &str, key: &SecretBox<Vec<u8>>) -> Option<String> {
    let normalised: String = value.nfc().collect();

    let mut mac = HmacSha256::new_from_slice(key.expose_secret()).ok()?;
    mac.update(normalised.as_bytes());

    let result = mac.finalize();
    let hash_bytes = result.into_bytes();

    Some(format!("HMAC_{}", hex::encode(hash_bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> SecretBox<Vec<u8>> {
        SecretBox::new(Box::new(vec![0u8; 32]))
    }

    #[test]
    fn drop_action() {
        let action = CompiledAction::Drop;
        assert_eq!(action.apply("any value", None), ActionResult::Drop);
    }

    #[test]
    fn redact_action() {
        let action = CompiledAction::Redact {
            placeholder: "[HIDDEN]".to_string(),
        };
        assert_eq!(
            action.apply("secret value", None),
            ActionResult::Replace("[HIDDEN]".to_string())
        );
    }

    #[test]
    fn hash_action_produces_deterministic_output() {
        let action = CompiledAction::Hash;
        let key = test_key();

        let result1 = action.apply("test@example.com", Some(&key));
        let result2 = action.apply("test@example.com", Some(&key));

        assert_eq!(result1, result2);

        if let ActionResult::Replace(hash) = result1 {
            assert!(hash.starts_with("HMAC_"));
            assert_eq!(hash.len(), 5 + 64); // "HMAC_" + 64 hex chars
        } else {
            panic!("expected Replace result");
        }
    }

    #[test]
    fn hash_action_different_values_different_hashes() {
        let action = CompiledAction::Hash;
        let key = test_key();

        let result1 = action.apply("user1@example.com", Some(&key));
        let result2 = action.apply("user2@example.com", Some(&key));

        assert_ne!(result1, result2);
    }

    #[test]
    fn hash_action_requires_key() {
        let action = CompiledAction::Hash;
        let result = action.apply("value", None);

        assert!(matches!(result, ActionResult::Error(_)));
    }

    #[test]
    fn hash_action_unicode_normalisation() {
        let action = CompiledAction::Hash;
        let key = test_key();

        // These are the same string in different Unicode forms
        let nfc = "caf\u{00E9}"; // é as single codepoint
        let nfd = "cafe\u{0301}"; // e + combining acute accent

        let result1 = action.apply(nfc, Some(&key));
        let result2 = action.apply(nfd, Some(&key));

        // Should produce the same hash after normalisation
        assert_eq!(result1, result2);
    }

    #[test]
    fn requires_key_detection() {
        assert!(!CompiledAction::Drop.requires_key());
        assert!(CompiledAction::Hash.requires_key());
        assert!(!CompiledAction::Redact {
            placeholder: "x".to_string()
        }
        .requires_key());
    }

    #[test]
    fn compile_from_config() {
        let drop = CompiledAction::compile(&ActionConfig::Drop);
        assert!(matches!(drop, CompiledAction::Drop));

        let hash = CompiledAction::compile(&ActionConfig::Hash);
        assert!(matches!(hash, CompiledAction::Hash));

        let redact = CompiledAction::compile(&ActionConfig::Redact {
            placeholder: "[X]".to_string(),
        });
        assert!(matches!(
            redact,
            CompiledAction::Redact { placeholder } if placeholder == "[X]"
        ));
    }
}
