//! Git forge authentication configuration.
//!
//! Provides authentication mechanisms for cloning repositories from
//! git forges (GitHub, GitLab, etc.).

use std::path::{Path, PathBuf};
use std::process::Command;

use serde::Deserialize;
use tracing::{info, warn};

use crate::error::{BuildError, BuildResult};

/// Default path for the SSH key.
fn default_ssh_key_path() -> PathBuf {
    PathBuf::from("/var/lib/sidereal/ssh/id_ed25519")
}

/// Configuration for forge authentication (deserialized from config).
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ForgeAuthConfig {
    /// SSH key authentication.
    Ssh {
        /// Path to the private key file.
        #[serde(default = "default_ssh_key_path")]
        key_path: PathBuf,
    },
    /// Username and token authentication (for HTTPS).
    Token {
        /// Username for authentication.
        username: String,
        /// Name of the secret containing the token.
        token_secret: String,
    },
    /// No authentication configured.
    None,
}

impl Default for ForgeAuthConfig {
    fn default() -> Self {
        Self::None
    }
}

/// Runtime forge authentication state.
///
/// Created from `ForgeAuthConfig` after loading secrets and generating keys.
#[derive(Debug, Clone)]
pub enum ForgeAuth {
    /// SSH key authentication.
    Ssh {
        /// The public key (for display/adding to forges).
        public_key: String,
        /// Path to the private key file.
        private_key_path: PathBuf,
    },
    /// Username and token authentication.
    Token {
        /// Username for authentication.
        username: String,
        /// The actual token value.
        token: String,
    },
    /// No authentication configured.
    None,
}

impl ForgeAuth {
    /// Initialise forge authentication from configuration.
    ///
    /// For SSH authentication, this will generate a key if one doesn't exist.
    pub fn from_config(config: &ForgeAuthConfig) -> BuildResult<Self> {
        match config {
            ForgeAuthConfig::Ssh { key_path } => {
                ensure_ssh_key_exists(key_path)?;
                let public_key = load_public_key(key_path)?;

                info!(
                    key_path = %key_path.display(),
                    "SSH forge authentication configured"
                );

                Ok(Self::Ssh {
                    public_key,
                    private_key_path: key_path.clone(),
                })
            }
            ForgeAuthConfig::Token {
                username,
                token_secret,
            } => {
                // Token loading would be done via secrets manager
                // For now, return an error indicating it's not yet implemented
                warn!(
                    username = %username,
                    token_secret = %token_secret,
                    "Token authentication not yet implemented"
                );
                Err(BuildError::ConfigParse(
                    "Token authentication not yet implemented".to_owned(),
                ))
            }
            ForgeAuthConfig::None => {
                info!("No forge authentication configured");
                Ok(Self::None)
            }
        }
    }

    /// Get the public key if SSH authentication is configured.
    #[must_use]
    pub fn public_key(&self) -> Option<&str> {
        match self {
            Self::Ssh { public_key, .. } => Some(public_key),
            _ => None,
        }
    }

    /// Get the SSH command to use for git operations.
    ///
    /// Returns `None` if SSH authentication is not configured.
    #[must_use]
    pub fn ssh_command(&self) -> Option<String> {
        match self {
            Self::Ssh {
                private_key_path, ..
            } => Some(format!(
                "ssh -i {} -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/dev/null",
                private_key_path.display()
            )),
            _ => None,
        }
    }

    /// Check if authentication is configured.
    #[must_use]
    pub const fn is_configured(&self) -> bool {
        !matches!(self, Self::None)
    }
}

/// Ensure an SSH key exists at the given path, generating one if needed.
fn ensure_ssh_key_exists(key_path: &Path) -> BuildResult<()> {
    if key_path.exists() {
        info!(path = %key_path.display(), "Using existing SSH key");
        return Ok(());
    }

    info!(path = %key_path.display(), "Generating new SSH key");

    // Create parent directory if needed
    if let Some(parent) = key_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            BuildError::Internal(format!(
                "Failed to create SSH key directory {}: {e}",
                parent.display()
            ))
        })?;
    }

    // Generate ed25519 key using ssh-keygen
    let output = Command::new("ssh-keygen")
        .args([
            "-t",
            "ed25519",
            "-f",
            &key_path.to_string_lossy(),
            "-N",
            "", // No passphrase
            "-C",
            "sidereal-build-service",
        ])
        .output()
        .map_err(|e| BuildError::Internal(format!("Failed to run ssh-keygen: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(BuildError::Internal(format!("ssh-keygen failed: {stderr}")));
    }

    // Set restrictive permissions on private key
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(key_path)
            .map_err(|e| BuildError::Internal(format!("Failed to get key metadata: {e}")))?
            .permissions();
        perms.set_mode(0o600);
        std::fs::set_permissions(key_path, perms)
            .map_err(|e| BuildError::Internal(format!("Failed to set key permissions: {e}")))?;
    }

    info!(path = %key_path.display(), "SSH key generated successfully");
    Ok(())
}

/// Load the public key from disk.
fn load_public_key(private_key_path: &Path) -> BuildResult<String> {
    let public_key_path = private_key_path.with_extension("pub");

    // If .pub file exists, read it
    if public_key_path.exists() {
        let content = std::fs::read_to_string(&public_key_path).map_err(|e| {
            BuildError::Internal(format!(
                "Failed to read public key {}: {e}",
                public_key_path.display()
            ))
        })?;
        return Ok(content.trim().to_owned());
    }

    // Otherwise, derive from private key using ssh-keygen
    let output = Command::new("ssh-keygen")
        .args(["-y", "-f", &private_key_path.to_string_lossy()])
        .output()
        .map_err(|e| BuildError::Internal(format!("Failed to run ssh-keygen: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(BuildError::Internal(format!(
            "ssh-keygen -y failed: {stderr}"
        )));
    }

    let public_key = String::from_utf8_lossy(&output.stdout).trim().to_owned();

    // Cache the public key for next time
    if let Err(e) = std::fs::write(&public_key_path, format!("{public_key}\n")) {
        warn!(
            path = %public_key_path.display(),
            error = %e,
            "Failed to cache public key"
        );
    }

    Ok(public_key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_none() {
        let config = ForgeAuthConfig::default();
        assert!(matches!(config, ForgeAuthConfig::None));
    }

    #[test]
    fn ssh_config_has_default_path() {
        let toml = r#"
            type = "ssh"
        "#;
        let config: ForgeAuthConfig = toml::from_str(toml).unwrap();
        match config {
            ForgeAuthConfig::Ssh { key_path } => {
                assert_eq!(key_path, PathBuf::from("/var/lib/sidereal/ssh/id_ed25519"));
            }
            _ => panic!("Expected SSH config"),
        }
    }

    #[test]
    fn ssh_config_custom_path() {
        let toml = r#"
            type = "ssh"
            key_path = "/custom/path/key"
        "#;
        let config: ForgeAuthConfig = toml::from_str(toml).unwrap();
        match config {
            ForgeAuthConfig::Ssh { key_path } => {
                assert_eq!(key_path, PathBuf::from("/custom/path/key"));
            }
            _ => panic!("Expected SSH config"),
        }
    }

    #[test]
    fn forge_auth_none_not_configured() {
        let auth = ForgeAuth::None;
        assert!(!auth.is_configured());
        assert!(auth.public_key().is_none());
        assert!(auth.ssh_command().is_none());
    }
}
