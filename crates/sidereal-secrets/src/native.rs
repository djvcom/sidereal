//! Native encrypted secrets backend using age and SQLite.
//!
//! This backend stores secrets encrypted with the age encryption library in a
//! SQLite database. It supports versioning, scope-based storage, and automatic
//! key rotation support.
//!
//! # Security
//!
//! - Secrets are encrypted at rest using X25519 + ChaCha20-Poly1305 (via age)
//! - Key file permissions are validated on startup
//! - Secret values are never logged
//! - Memory is zeroed on drop via the `secrecy` crate

use std::fs;
use std::io::{Read as IoRead, Write as IoWrite};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use age::secrecy::ExposeSecret;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use sqlx::Row;
use tracing::{debug, instrument};

use crate::error::SecretsError;
use crate::traits::SecretsBackend;
use crate::types::{SecretContext, SecretMetadata, SecretScope, SecretValue, SecretVersion};

/// Native encrypted secrets backend.
///
/// Uses SQLite for storage and age for encryption. The identity (private key)
/// is loaded from a file and used to encrypt/decrypt all secrets.
pub struct NativeSecrets {
    pool: SqlitePool,
    identity: age::x25519::Identity,
    recipient: age::x25519::Recipient,
}

impl NativeSecrets {
    /// Creates a new native secrets backend.
    ///
    /// # Arguments
    ///
    /// * `db_path` - Path to the SQLite database file
    /// * `identity_path` - Path to the age identity file
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The database connection fails
    /// - The identity file cannot be read or has insecure permissions
    /// - The identity file contains invalid data
    pub async fn new(
        db_path: impl AsRef<Path>,
        identity_path: impl AsRef<Path>,
    ) -> Result<Self, SecretsError> {
        let identity = Self::load_identity(identity_path.as_ref())?;
        let recipient = identity.to_public();

        let pool = Self::connect_db(db_path.as_ref()).await?;
        Self::init_schema(&pool).await?;

        Ok(Self {
            pool,
            identity,
            recipient,
        })
    }

    /// Creates a new native secrets backend, generating a new identity if needed.
    ///
    /// If the identity file does not exist, a new identity is created and saved.
    pub async fn new_or_create(
        db_path: impl AsRef<Path>,
        identity_path: impl AsRef<Path>,
    ) -> Result<Self, SecretsError> {
        let identity_path = identity_path.as_ref();

        if !identity_path.exists() {
            Self::create_identity(identity_path)?;
        }

        Self::new(db_path, identity_path).await
    }

    fn load_identity(path: &Path) -> Result<age::x25519::Identity, SecretsError> {
        #[cfg(unix)]
        Self::check_key_permissions(path)?;

        let mut file = fs::File::open(path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                SecretsError::MasterKeyNotFound
            } else {
                SecretsError::Configuration(format!("failed to open identity file: {e}"))
            }
        })?;

        let mut contents = String::new();
        file.read_to_string(&mut contents).map_err(|e| {
            SecretsError::Configuration(format!("failed to read identity file: {e}"))
        })?;

        let identity = contents
            .lines()
            .find(|line| !line.starts_with('#') && !line.is_empty())
            .ok_or_else(|| SecretsError::Configuration("identity file is empty".to_owned()))?
            .parse::<age::x25519::Identity>()
            .map_err(|e| SecretsError::Configuration(format!("invalid identity: {e}")))?;

        Ok(identity)
    }

    fn create_identity(path: &Path) -> Result<(), SecretsError> {
        let identity = age::x25519::Identity::generate();
        let public_key = identity.to_public();

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                SecretsError::Configuration(format!("failed to create identity directory: {e}"))
            })?;
        }

        // Create file with restrictive permissions atomically (Unix)
        #[cfg(unix)]
        let mut file = {
            use std::os::unix::fs::OpenOptionsExt;
            fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path)
                .map_err(|e| {
                    SecretsError::Configuration(format!("failed to create identity file: {e}"))
                })?
        };

        #[cfg(not(unix))]
        let mut file = fs::File::create(path).map_err(|e| {
            SecretsError::Configuration(format!("failed to create identity file: {e}"))
        })?;

        writeln!(
            file,
            "# created: {}",
            Utc::now().format("%Y-%m-%d %H:%M:%S")
        )
        .ok();
        writeln!(file, "# public key: {public_key}").ok();
        writeln!(file, "{}", identity.to_string().expose_secret()).map_err(|e| {
            SecretsError::Configuration(format!("failed to write identity file: {e}"))
        })?;

        Ok(())
    }

    #[cfg(unix)]
    fn check_key_permissions(path: &Path) -> Result<(), SecretsError> {
        use std::os::unix::fs::MetadataExt;

        let metadata = fs::metadata(path).map_err(|e| {
            SecretsError::Configuration(format!("failed to read identity file metadata: {e}"))
        })?;

        let mode = metadata.mode() & 0o777;

        if mode & 0o077 != 0 {
            return Err(SecretsError::InsecureKeyFile {
                path: path.display().to_string(),
                mode,
            });
        }

        Ok(())
    }

    async fn connect_db(path: &Path) -> Result<SqlitePool, SecretsError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                SecretsError::Configuration(format!("failed to create database directory: {e}"))
            })?;
        }

        // Create database file with restrictive permissions before SQLite opens it
        #[cfg(unix)]
        if !path.exists() {
            use std::os::unix::fs::OpenOptionsExt;
            fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path)
                .map_err(|e| {
                    SecretsError::Configuration(format!("failed to create database file: {e}"))
                })?;
        }

        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await
            .map_err(|e| SecretsError::Database(format!("failed to connect: {e}")))?;

        // Verify database file has secure permissions
        #[cfg(unix)]
        Self::check_db_permissions(path)?;

        Ok(pool)
    }

    #[cfg(unix)]
    fn check_db_permissions(path: &Path) -> Result<(), SecretsError> {
        use std::os::unix::fs::MetadataExt;

        let metadata = fs::metadata(path).map_err(|e| {
            SecretsError::Configuration(format!("failed to read database metadata: {e}"))
        })?;

        let mode = metadata.mode() & 0o777;

        if mode & 0o077 != 0 {
            return Err(SecretsError::Configuration(format!(
                "insecure database file permissions: {} has mode {:o}, expected 0600",
                path.display(),
                mode
            )));
        }

        Ok(())
    }

    async fn init_schema(pool: &SqlitePool) -> Result<(), SecretsError> {
        sqlx::query(
            r"
            CREATE TABLE IF NOT EXISTS secrets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                scope TEXT NOT NULL,
                project TEXT,
                environment TEXT,
                version TEXT NOT NULL,
                encrypted_value BLOB NOT NULL,
                key_version INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                is_current INTEGER NOT NULL DEFAULT 1,
                UNIQUE(name, scope, project, environment, version)
            )
            ",
        )
        .execute(pool)
        .await
        .map_err(|e| SecretsError::Database(format!("failed to create table: {e}")))?;

        sqlx::query(
            r"
            CREATE INDEX IF NOT EXISTS idx_secrets_lookup
            ON secrets(name, scope, project, environment, is_current)
            ",
        )
        .execute(pool)
        .await
        .map_err(|e| SecretsError::Database(format!("failed to create index: {e}")))?;

        Ok(())
    }

    pub(crate) fn encrypt(&self, plaintext: &str) -> Result<Vec<u8>, SecretsError> {
        let encryptor = age::Encryptor::with_recipients(vec![Box::new(self.recipient.clone())])
            .ok_or_else(|| {
                SecretsError::Encryption(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "no valid encryption recipients",
                )))
            })?;

        let mut encrypted = vec![];
        let mut writer = encryptor
            .wrap_output(&mut encrypted)
            .map_err(|e| SecretsError::Encryption(Box::new(e)))?;

        writer
            .write_all(plaintext.as_bytes())
            .map_err(|e| SecretsError::Encryption(Box::new(e)))?;

        writer
            .finish()
            .map_err(|e| SecretsError::Encryption(Box::new(e)))?;

        Ok(encrypted)
    }

    pub(crate) fn decrypt(&self, ciphertext: &[u8]) -> Result<SecretValue, SecretsError> {
        let age::Decryptor::Recipients(decryptor) =
            age::Decryptor::new(ciphertext).map_err(|e| SecretsError::Decryption(Box::new(e)))?
        else {
            return Err(SecretsError::Decryption(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unexpected decryptor type",
            ))));
        };

        let mut decrypted = vec![];
        let identity: &dyn age::Identity = &self.identity;
        let mut reader = decryptor
            .decrypt(std::iter::once(identity))
            .map_err(|e| SecretsError::Decryption(Box::new(e)))?;

        reader
            .read_to_end(&mut decrypted)
            .map_err(|e| SecretsError::Decryption(Box::new(e)))?;

        SecretValue::from_bytes(&decrypted).map_err(|e| SecretsError::Decryption(Box::new(e)))
    }

    fn scope_components(scope: &SecretScope) -> (&'static str, Option<&str>, Option<&str>) {
        match scope {
            SecretScope::Global => ("global", None, None),
            SecretScope::Project { project_id } => ("project", Some(project_id.as_str()), None),
            SecretScope::Environment {
                project_id,
                environment,
            } => (
                "environment",
                Some(project_id.as_str()),
                Some(environment.as_str()),
            ),
        }
    }

    fn datetime_to_systemtime(dt: &str) -> Result<SystemTime, SecretsError> {
        DateTime::parse_from_rfc3339(dt)
            .map(|d| {
                let secs = u64::try_from(d.timestamp()).unwrap_or(0);
                SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
            })
            .map_err(|e| SecretsError::Database(format!("invalid timestamp in database: {e}")))
    }

    fn systemtime_to_datetime(st: SystemTime) -> String {
        let duration = st.duration_since(UNIX_EPOCH).unwrap_or_default();
        let secs = i64::try_from(duration.as_secs()).unwrap_or(i64::MAX);
        DateTime::<Utc>::from_timestamp(secs, 0)
            .map_or_else(|| Utc::now().to_rfc3339(), |d| d.to_rfc3339())
    }

    fn escape_like_pattern(s: &str) -> String {
        let mut result = String::with_capacity(s.len() * 2);
        for c in s.chars() {
            match c {
                '%' | '_' | '\\' => {
                    result.push('\\');
                    result.push(c);
                }
                _ => result.push(c),
            }
        }
        result
    }
}

impl std::fmt::Debug for NativeSecrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeSecrets")
            .field("recipient", &self.recipient.to_string())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SecretsBackend for NativeSecrets {
    #[instrument(skip(self), fields(name = %name))]
    async fn get(
        &self,
        name: &str,
        context: &SecretContext,
    ) -> Result<Option<SecretValue>, SecretsError> {
        for scope in context.resolution_order() {
            if let Some(value) = self.get_at_scope(name, &scope).await? {
                debug!(scope = %scope, "found secret");
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    #[instrument(skip(self), fields(name = %name, scope = %scope))]
    async fn get_at_scope(
        &self,
        name: &str,
        scope: &SecretScope,
    ) -> Result<Option<SecretValue>, SecretsError> {
        let (scope_type, project, environment) = Self::scope_components(scope);

        let row = sqlx::query(
            r"
            SELECT encrypted_value FROM secrets
            WHERE name = ? AND scope = ?
                AND (project IS ? OR (project IS NULL AND ? IS NULL))
                AND (environment IS ? OR (environment IS NULL AND ? IS NULL))
                AND is_current = 1
            ",
        )
        .bind(name)
        .bind(scope_type)
        .bind(project)
        .bind(project)
        .bind(environment)
        .bind(environment)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SecretsError::Database(format!("query failed: {e}")))?;

        match row {
            Some(row) => {
                let encrypted: Vec<u8> = row.get("encrypted_value");
                let value = self.decrypt(&encrypted)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    #[instrument(skip(self), fields(name = %name, scope = %scope, version = %version))]
    async fn get_version(
        &self,
        name: &str,
        scope: &SecretScope,
        version: &SecretVersion,
    ) -> Result<Option<SecretValue>, SecretsError> {
        let (scope_type, project, environment) = Self::scope_components(scope);

        let row = sqlx::query(
            r"
            SELECT encrypted_value FROM secrets
            WHERE name = ? AND scope = ? AND version = ?
                AND (project IS ? OR (project IS NULL AND ? IS NULL))
                AND (environment IS ? OR (environment IS NULL AND ? IS NULL))
            ",
        )
        .bind(name)
        .bind(scope_type)
        .bind(version.as_str())
        .bind(project)
        .bind(project)
        .bind(environment)
        .bind(environment)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SecretsError::Database(format!("query failed: {e}")))?;

        match row {
            Some(row) => {
                let encrypted: Vec<u8> = row.get("encrypted_value");
                let value = self.decrypt(&encrypted)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    #[instrument(skip(self, value), fields(name = %name, scope = %scope))]
    async fn set(
        &self,
        name: &str,
        value: SecretValue,
        scope: &SecretScope,
    ) -> Result<SecretVersion, SecretsError> {
        let version = SecretVersion::generate();
        let encrypted = self.encrypt(value.expose())?;
        let now = Self::systemtime_to_datetime(SystemTime::now());
        let (scope_type, project, environment) = Self::scope_components(scope);

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SecretsError::Database(format!("failed to begin transaction: {e}")))?;

        sqlx::query(
            r"
            UPDATE secrets SET is_current = 0
            WHERE name = ? AND scope = ?
                AND (project IS ? OR (project IS NULL AND ? IS NULL))
                AND (environment IS ? OR (environment IS NULL AND ? IS NULL))
                AND is_current = 1
            ",
        )
        .bind(name)
        .bind(scope_type)
        .bind(project)
        .bind(project)
        .bind(environment)
        .bind(environment)
        .execute(&mut *tx)
        .await
        .map_err(|e| SecretsError::Database(format!("failed to update current: {e}")))?;

        sqlx::query(
            r"
            INSERT INTO secrets (name, scope, project, environment, version, encrypted_value, created_at, updated_at, is_current)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
            ",
        )
        .bind(name)
        .bind(scope_type)
        .bind(project)
        .bind(environment)
        .bind(version.as_str())
        .bind(&encrypted)
        .bind(&now)
        .bind(&now)
        .execute(&mut *tx)
        .await
        .map_err(|e| SecretsError::Database(format!("failed to insert: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| SecretsError::Database(format!("failed to commit: {e}")))?;

        debug!(version = %version, "stored secret");
        Ok(version)
    }

    #[instrument(skip(self), fields(name = %name, scope = %scope))]
    async fn delete(&self, name: &str, scope: &SecretScope) -> Result<bool, SecretsError> {
        let (scope_type, project, environment) = Self::scope_components(scope);

        let result = sqlx::query(
            r"
            DELETE FROM secrets
            WHERE name = ? AND scope = ?
                AND (project IS ? OR (project IS NULL AND ? IS NULL))
                AND (environment IS ? OR (environment IS NULL AND ? IS NULL))
            ",
        )
        .bind(name)
        .bind(scope_type)
        .bind(project)
        .bind(project)
        .bind(environment)
        .bind(environment)
        .execute(&self.pool)
        .await
        .map_err(|e| SecretsError::Database(format!("delete failed: {e}")))?;

        let deleted = result.rows_affected() > 0;
        if deleted {
            debug!("deleted secret");
        }
        Ok(deleted)
    }

    #[instrument(skip(self), fields(name = %name))]
    async fn exists(&self, name: &str, context: &SecretContext) -> Result<bool, SecretsError> {
        for scope in context.resolution_order() {
            let (scope_type, project, environment) = Self::scope_components(&scope);

            let row = sqlx::query(
                r"
                SELECT 1 FROM secrets
                WHERE name = ? AND scope = ?
                    AND (project IS ? OR (project IS NULL AND ? IS NULL))
                    AND (environment IS ? OR (environment IS NULL AND ? IS NULL))
                    AND is_current = 1
                LIMIT 1
                ",
            )
            .bind(name)
            .bind(scope_type)
            .bind(project)
            .bind(project)
            .bind(environment)
            .bind(environment)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| SecretsError::Database(format!("query failed: {e}")))?;

            if row.is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    #[instrument(skip(self), fields(prefix = %prefix, scope = %scope))]
    async fn list(
        &self,
        prefix: &str,
        scope: &SecretScope,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<(Vec<String>, Option<String>), SecretsError> {
        let (scope_type, project, environment) = Self::scope_components(scope);
        let escaped_prefix = Self::escape_like_pattern(prefix);
        let pattern = format!("{escaped_prefix}%");
        let offset: i64 = match cursor {
            Some(c) => c
                .parse()
                .map_err(|_| SecretsError::Configuration(format!("invalid cursor: {c}")))?,
            None => 0,
        };

        let rows = sqlx::query(
            r"
            SELECT DISTINCT name FROM secrets
            WHERE name LIKE ? ESCAPE '\' AND scope = ?
                AND (project IS ? OR (project IS NULL AND ? IS NULL))
                AND (environment IS ? OR (environment IS NULL AND ? IS NULL))
                AND is_current = 1
            ORDER BY name
            LIMIT ? OFFSET ?
            ",
        )
        .bind(&pattern)
        .bind(scope_type)
        .bind(project)
        .bind(project)
        .bind(environment)
        .bind(environment)
        .bind(i64::try_from(limit).unwrap_or(i64::MAX).saturating_add(1))
        .bind(offset)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SecretsError::Database(format!("query failed: {e}")))?;

        let mut names: Vec<String> = rows.iter().map(|r| r.get("name")).collect();
        let next_cursor = if names.len() > limit {
            names.pop();
            Some((offset.saturating_add(i64::try_from(limit).unwrap_or(i64::MAX))).to_string())
        } else {
            None
        };

        Ok((names, next_cursor))
    }

    #[instrument(skip(self), fields(name = %name, scope = %scope))]
    async fn metadata(
        &self,
        name: &str,
        scope: &SecretScope,
    ) -> Result<Option<SecretMetadata>, SecretsError> {
        let (scope_type, project, environment) = Self::scope_components(scope);

        let row = sqlx::query(
            r"
            SELECT version, created_at, updated_at FROM secrets
            WHERE name = ? AND scope = ?
                AND (project IS ? OR (project IS NULL AND ? IS NULL))
                AND (environment IS ? OR (environment IS NULL AND ? IS NULL))
                AND is_current = 1
            ",
        )
        .bind(name)
        .bind(scope_type)
        .bind(project)
        .bind(project)
        .bind(environment)
        .bind(environment)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SecretsError::Database(format!("query failed: {e}")))?;

        match row {
            Some(row) => {
                let version_str: String = row.get("version");
                let created_at_str: String = row.get("created_at");
                let updated_at_str: String = row.get("updated_at");

                let created_at = Self::datetime_to_systemtime(&created_at_str)?;
                let updated_at = Self::datetime_to_systemtime(&updated_at_str)?;

                Ok(Some(SecretMetadata::with_timestamps(
                    name,
                    scope.clone(),
                    SecretVersion::new(version_str),
                    created_at,
                    updated_at,
                )))
            }
            None => Ok(None),
        }
    }

    #[instrument(skip(self), fields(name = %name, scope = %scope))]
    async fn versions(
        &self,
        name: &str,
        scope: &SecretScope,
        limit: usize,
    ) -> Result<Vec<SecretVersion>, SecretsError> {
        let (scope_type, project, environment) = Self::scope_components(scope);

        let rows = sqlx::query(
            r"
            SELECT version FROM secrets
            WHERE name = ? AND scope = ?
                AND (project IS ? OR (project IS NULL AND ? IS NULL))
                AND (environment IS ? OR (environment IS NULL AND ? IS NULL))
            ORDER BY id DESC
            LIMIT ?
            ",
        )
        .bind(name)
        .bind(scope_type)
        .bind(project)
        .bind(project)
        .bind(environment)
        .bind(environment)
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SecretsError::Database(format!("query failed: {e}")))?;

        let versions: Vec<SecretVersion> = rows
            .iter()
            .map(|r| SecretVersion::new(r.get::<String, _>("version")))
            .collect();

        Ok(versions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn setup() -> (NativeSecrets, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("secrets.db");
        let identity_path = temp_dir.path().join("identity.age");

        let backend = NativeSecrets::new_or_create(&db_path, &identity_path)
            .await
            .unwrap();

        (backend, temp_dir)
    }

    #[tokio::test]
    async fn basic_set_get() {
        let (backend, _temp) = setup().await;

        let version = backend
            .set(
                "API_KEY",
                SecretValue::new("sk_test_123"),
                &SecretScope::global(),
            )
            .await
            .unwrap();

        assert!(!version.as_str().is_empty());

        let ctx = SecretContext::new();
        let value = backend.get("API_KEY", &ctx).await.unwrap().unwrap();
        assert_eq!(value.expose(), "sk_test_123");
    }

    #[tokio::test]
    async fn not_found_returns_none() {
        let (backend, _temp) = setup().await;

        let ctx = SecretContext::new();
        let value = backend.get("NONEXISTENT", &ctx).await.unwrap();
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn scope_resolution() {
        let (backend, _temp) = setup().await;

        backend
            .set(
                "API_KEY",
                SecretValue::new("global-key"),
                &SecretScope::global(),
            )
            .await
            .unwrap();

        backend
            .set(
                "API_KEY",
                SecretValue::new("project-key"),
                &SecretScope::project("myapp"),
            )
            .await
            .unwrap();

        backend
            .set(
                "API_KEY",
                SecretValue::new("env-key"),
                &SecretScope::environment("myapp", "prod"),
            )
            .await
            .unwrap();

        let ctx = SecretContext::new()
            .with_project("myapp")
            .with_environment("prod");
        let value = backend.get("API_KEY", &ctx).await.unwrap().unwrap();
        assert_eq!(value.expose(), "env-key");

        let ctx = SecretContext::new().with_project("myapp");
        let value = backend.get("API_KEY", &ctx).await.unwrap().unwrap();
        assert_eq!(value.expose(), "project-key");

        let ctx = SecretContext::new();
        let value = backend.get("API_KEY", &ctx).await.unwrap().unwrap();
        assert_eq!(value.expose(), "global-key");
    }

    #[tokio::test]
    async fn versioning() {
        let (backend, _temp) = setup().await;

        let v1 = backend
            .set(
                "API_KEY",
                SecretValue::new("v1-value"),
                &SecretScope::global(),
            )
            .await
            .unwrap();

        let v2 = backend
            .set(
                "API_KEY",
                SecretValue::new("v2-value"),
                &SecretScope::global(),
            )
            .await
            .unwrap();

        assert_ne!(v1.as_str(), v2.as_str());

        let ctx = SecretContext::new();
        let current = backend.get("API_KEY", &ctx).await.unwrap().unwrap();
        assert_eq!(current.expose(), "v2-value");

        let old = backend
            .get_version("API_KEY", &SecretScope::global(), &v1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(old.expose(), "v1-value");
    }

    #[tokio::test]
    async fn delete() {
        let (backend, _temp) = setup().await;

        backend
            .set(
                "API_KEY",
                SecretValue::new("secret"),
                &SecretScope::global(),
            )
            .await
            .unwrap();

        let deleted = backend
            .delete("API_KEY", &SecretScope::global())
            .await
            .unwrap();
        assert!(deleted);

        let ctx = SecretContext::new();
        let value = backend.get("API_KEY", &ctx).await.unwrap();
        assert!(value.is_none());

        let deleted_again = backend
            .delete("API_KEY", &SecretScope::global())
            .await
            .unwrap();
        assert!(!deleted_again);
    }

    #[tokio::test]
    async fn exists() {
        let (backend, _temp) = setup().await;

        let ctx = SecretContext::new();
        assert!(!backend.exists("API_KEY", &ctx).await.unwrap());

        backend
            .set(
                "API_KEY",
                SecretValue::new("secret"),
                &SecretScope::global(),
            )
            .await
            .unwrap();

        assert!(backend.exists("API_KEY", &ctx).await.unwrap());
    }

    #[tokio::test]
    async fn list_secrets() {
        let (backend, _temp) = setup().await;

        backend
            .set("API_KEY_1", SecretValue::new("v1"), &SecretScope::global())
            .await
            .unwrap();
        backend
            .set("API_KEY_2", SecretValue::new("v2"), &SecretScope::global())
            .await
            .unwrap();
        backend
            .set(
                "DB_PASSWORD",
                SecretValue::new("pw"),
                &SecretScope::global(),
            )
            .await
            .unwrap();

        let (names, cursor) = backend
            .list("API_", &SecretScope::global(), 10, None)
            .await
            .unwrap();

        assert_eq!(names.len(), 2);
        assert!(names.contains(&"API_KEY_1".to_string()));
        assert!(names.contains(&"API_KEY_2".to_string()));
        assert!(cursor.is_none());
    }

    #[tokio::test]
    async fn metadata() {
        let (backend, _temp) = setup().await;

        backend
            .set(
                "API_KEY",
                SecretValue::new("secret"),
                &SecretScope::global(),
            )
            .await
            .unwrap();

        let meta = backend
            .metadata("API_KEY", &SecretScope::global())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(meta.name, "API_KEY");
        assert_eq!(meta.scope, SecretScope::global());
        assert!(!meta.version.as_str().is_empty());
    }

    #[tokio::test]
    async fn versions_list() {
        let (backend, _temp) = setup().await;

        let v1 = backend
            .set("API_KEY", SecretValue::new("v1"), &SecretScope::global())
            .await
            .unwrap();
        let v2 = backend
            .set("API_KEY", SecretValue::new("v2"), &SecretScope::global())
            .await
            .unwrap();
        let v3 = backend
            .set("API_KEY", SecretValue::new("v3"), &SecretScope::global())
            .await
            .unwrap();

        let versions = backend
            .versions("API_KEY", &SecretScope::global(), 10)
            .await
            .unwrap();

        assert_eq!(versions.len(), 3);
        assert_eq!(versions[0], v3);
        assert_eq!(versions[1], v2);
        assert_eq!(versions[2], v1);
    }

    #[tokio::test]
    async fn identity_file_created() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("secrets.db");
        let identity_path = temp_dir.path().join("identity.age");

        assert!(!identity_path.exists());

        let _backend = NativeSecrets::new_or_create(&db_path, &identity_path)
            .await
            .unwrap();

        assert!(identity_path.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn identity_file_permissions_enforced() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("secrets.db");
        let identity_path = temp_dir.path().join("identity.age");

        let _backend = NativeSecrets::new_or_create(&db_path, &identity_path)
            .await
            .unwrap();

        let metadata = fs::metadata(&identity_path).unwrap();
        let mode = metadata.permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "Identity file should have 0600 permissions");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_insecure_identity_file() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("secrets.db");
        let identity_path = temp_dir.path().join("identity.age");

        let _backend = NativeSecrets::new_or_create(&db_path, &identity_path)
            .await
            .unwrap();

        fs::set_permissions(&identity_path, fs::Permissions::from_mode(0o644)).unwrap();

        let result = NativeSecrets::new(&db_path, &identity_path).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, SecretsError::InsecureKeyFile { .. }),
            "Expected InsecureKeyFile error, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn pagination_boundaries() {
        let (backend, _temp) = setup().await;

        for i in 0..5 {
            backend
                .set(
                    &format!("KEY_{i:02}"),
                    SecretValue::new(format!("value_{i}")),
                    &SecretScope::global(),
                )
                .await
                .unwrap();
        }

        let (names, cursor) = backend
            .list("KEY_", &SecretScope::global(), 2, None)
            .await
            .unwrap();
        assert_eq!(names.len(), 2);
        assert!(cursor.is_some());

        let (names2, cursor2) = backend
            .list("KEY_", &SecretScope::global(), 2, cursor.as_deref())
            .await
            .unwrap();
        assert_eq!(names2.len(), 2);
        assert!(cursor2.is_some());

        let (names3, cursor3) = backend
            .list("KEY_", &SecretScope::global(), 2, cursor2.as_deref())
            .await
            .unwrap();
        assert_eq!(names3.len(), 1);
        assert!(cursor3.is_none());

        let all_names: Vec<_> = names.into_iter().chain(names2).chain(names3).collect();
        assert_eq!(all_names.len(), 5);
    }

    #[tokio::test]
    async fn invalid_cursor_rejected() {
        let (backend, _temp) = setup().await;

        let result = backend
            .list("", &SecretScope::global(), 10, Some("not_a_number"))
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, SecretsError::Configuration(_)),
            "Expected Configuration error for invalid cursor, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn list_escapes_special_characters() {
        let (backend, _temp) = setup().await;

        backend
            .set(
                "KEY%TEST",
                SecretValue::new("value1"),
                &SecretScope::global(),
            )
            .await
            .unwrap();

        backend
            .set(
                "KEY_TEST",
                SecretValue::new("value2"),
                &SecretScope::global(),
            )
            .await
            .unwrap();

        backend
            .set(
                "KEYATEST",
                SecretValue::new("value3"),
                &SecretScope::global(),
            )
            .await
            .unwrap();

        let (names, _) = backend
            .list("KEY%", &SecretScope::global(), 10, None)
            .await
            .unwrap();
        assert_eq!(names.len(), 1);
        assert!(names.contains(&"KEY%TEST".to_string()));

        let (names, _) = backend
            .list("KEY_", &SecretScope::global(), 10, None)
            .await
            .unwrap();
        assert_eq!(names.len(), 1);
        assert!(names.contains(&"KEY_TEST".to_string()));
    }

    #[tokio::test]
    async fn encrypted_values_differ_for_identical_inputs() {
        let (backend, _temp) = setup().await;

        // Encrypt same value twice
        let ciphertext1 = backend.encrypt("identical_secret").unwrap();
        let ciphertext2 = backend.encrypt("identical_secret").unwrap();

        // Ciphertexts must differ (proving proper nonce usage)
        assert_ne!(
            ciphertext1, ciphertext2,
            "Encrypting same plaintext must produce different ciphertexts"
        );

        // Both must decrypt to same value
        let decrypted1 = backend.decrypt(&ciphertext1).unwrap();
        let decrypted2 = backend.decrypt(&ciphertext2).unwrap();
        assert_eq!(decrypted1.expose(), "identical_secret");
        assert_eq!(decrypted2.expose(), "identical_secret");
    }

    #[tokio::test]
    async fn rejects_corrupted_ciphertext() {
        let (backend, _temp) = setup().await;

        let ciphertext = backend.encrypt("authentic_secret").unwrap();

        // Corrupt the ciphertext
        let mut corrupted = ciphertext.clone();
        if let Some(byte) = corrupted.get_mut(ciphertext.len() / 2) {
            *byte ^= 0xFF;
        }

        let result = backend.decrypt(&corrupted);
        assert!(
            result.is_err(),
            "Decryption of corrupted ciphertext must fail"
        );
        assert!(
            matches!(result.unwrap_err(), SecretsError::Decryption(_)),
            "Must return Decryption error for corrupted data"
        );
    }

    #[tokio::test]
    async fn rejects_truncated_ciphertext() {
        let (backend, _temp) = setup().await;

        let ciphertext = backend.encrypt("secret").unwrap();

        // Truncate the ciphertext
        let truncated = &ciphertext[..ciphertext.len() / 2];

        let result = backend.decrypt(truncated);
        assert!(
            result.is_err(),
            "Decryption of truncated ciphertext must fail"
        );
        assert!(matches!(result.unwrap_err(), SecretsError::Decryption(_)));
    }

    #[tokio::test]
    async fn rejects_wrong_identity_key() {
        let temp_dir = TempDir::new().unwrap();
        let db_path1 = temp_dir.path().join("db1.db");
        let db_path2 = temp_dir.path().join("db2.db");
        let identity_path1 = temp_dir.path().join("identity1.age");
        let identity_path2 = temp_dir.path().join("identity2.age");

        let backend1 = NativeSecrets::new_or_create(&db_path1, &identity_path1)
            .await
            .unwrap();
        let backend2 = NativeSecrets::new_or_create(&db_path2, &identity_path2)
            .await
            .unwrap();

        // Encrypt with first key
        let ciphertext = backend1.encrypt("secret_data").unwrap();

        // Attempt to decrypt with second key
        let result = backend2.decrypt(&ciphertext);
        assert!(result.is_err(), "Decryption with wrong key must fail");
        assert!(matches!(result.unwrap_err(), SecretsError::Decryption(_)));
    }

    #[tokio::test]
    async fn large_value_encryption_roundtrip() {
        let (backend, _temp) = setup().await;

        // Test various sizes including edge cases
        for size in [0, 1, 255, 256, 1024, 65535, 65536, 1024 * 1024] {
            let large_data = "a".repeat(size);
            let scope = SecretScope::global();

            backend
                .set(
                    &format!("LARGE_{size}"),
                    SecretValue::new(large_data.clone()),
                    &scope,
                )
                .await
                .unwrap();

            let retrieved = backend
                .get_at_scope(&format!("LARGE_{size}"), &scope)
                .await
                .unwrap()
                .unwrap();

            assert_eq!(
                retrieved.expose(),
                &large_data,
                "Failed roundtrip for size {size}"
            );
        }
    }

    #[tokio::test]
    async fn identity_file_empty() {
        let temp_dir = TempDir::new().unwrap();
        let identity_path = temp_dir.path().join("empty.age");

        // Create empty file with secure permissions
        std::fs::File::create(&identity_path).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&identity_path, std::fs::Permissions::from_mode(0o600))
                .unwrap();
        }

        let db_path = temp_dir.path().join("secrets.db");
        let result = NativeSecrets::new(&db_path, &identity_path).await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), SecretsError::Configuration(msg) if msg.contains("empty")),
            "Expected Configuration error about empty file"
        );
    }

    #[tokio::test]
    async fn identity_file_invalid_format() {
        let temp_dir = TempDir::new().unwrap();
        let identity_path = temp_dir.path().join("invalid.age");

        // Write invalid content
        std::fs::write(&identity_path, "not-a-valid-age-identity\n").unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&identity_path, std::fs::Permissions::from_mode(0o600))
                .unwrap();
        }

        let db_path = temp_dir.path().join("secrets.db");
        let result = NativeSecrets::new(&db_path, &identity_path).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SecretsError::Configuration(_)
        ));
    }

    #[tokio::test]
    async fn concurrent_operations_maintain_isolation() {
        use std::sync::Arc;

        let (backend, _temp) = setup().await;
        let backend = Arc::new(backend);
        let scope = SecretScope::global();

        let mut handles = Vec::new();

        // Each task works with its own secret
        for i in 0..10 {
            let backend = Arc::clone(&backend);
            let scope = scope.clone();
            let ctx = SecretContext::new();

            handles.push(tokio::spawn(async move {
                let secret_name = format!("ISOLATED_SECRET_{i}");
                let expected_value = format!("value_{i}");

                // Set
                backend
                    .set(&secret_name, SecretValue::new(&expected_value), &scope)
                    .await
                    .unwrap();

                // Read back
                let retrieved = backend.get(&secret_name, &ctx).await.unwrap().unwrap();

                assert_eq!(
                    retrieved.expose(),
                    expected_value,
                    "Secret {secret_name} was corrupted by concurrent operations"
                );
            }));
        }

        // All operations should succeed without interference
        for handle in handles {
            handle.await.unwrap();
        }
    }
}
