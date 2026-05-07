//! Persistent token cache stored at `~/.config/sidereal-ai/tokens.json`.
//!
//! The cache file is written with permissions 0o600 to prevent other users
//! from reading the stored credentials.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

/// Cached token set persisted between invocations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedTokens {
    /// `OAuth2` access token.
    pub access_token: String,
    /// `OAuth2` refresh token, if the provider issued one.
    pub refresh_token: Option<String>,
    /// Expiry time for the access token in RFC 3339 format.
    pub expires_at: DateTime<Utc>,
}

impl CachedTokens {
    /// Returns true if the access token is still valid.
    pub fn access_token_valid(&self) -> bool {
        self.expires_at > Utc::now()
    }
}

/// Load cached tokens from disk.
///
/// Returns `None` if the file does not exist or cannot be read.
pub fn load(path: &Path) -> Result<Option<CachedTokens>, CacheError> {
    match fs::read_to_string(path) {
        Ok(contents) => {
            let tokens = serde_json::from_str(&contents)?;
            Ok(Some(tokens))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(CacheError::Io(e)),
    }
}

/// Persist tokens to disk with restricted file permissions.
pub fn save(path: &Path, tokens: &CachedTokens) -> Result<(), CacheError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let json = serde_json::to_string_pretty(tokens)?;
    write_private_file(path, &json)?;
    Ok(())
}

/// Write a file readable only by the current user (mode 0o600 on Unix).
fn write_private_file(path: &Path, contents: &str) -> Result<(), CacheError> {
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut opts = fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true).mode(0o600);
        let mut file = opts.open(path)?;
        file.write_all(contents.as_bytes())?;
    }
    #[cfg(not(unix))]
    {
        fs::write(path, contents)?;
    }
    Ok(())
}

/// Errors that can occur when reading or writing the token cache.
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    /// An I/O error occurred accessing the cache file.
    #[error("cache I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// The cache file contained invalid JSON.
    #[error("cache parse error: {0}")]
    Json(#[from] serde_json::Error),
}
