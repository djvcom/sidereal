//! PostgreSQL deployment store implementation.

use async_trait::async_trait;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;

use crate::error::{ControlError, ControlResult};
use crate::types::{
    DeploymentData, DeploymentId, DeploymentRecord, FunctionMetadata, PersistedState, ProjectId,
};

use super::{DeploymentFilter, DeploymentStore};

/// PostgreSQL-backed deployment store.
#[derive(Clone)]
pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
    /// Connect to PostgreSQL and create a new store.
    ///
    /// The required tables are created if they don't exist.
    pub async fn new(url: &str) -> ControlResult<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(url)
            .await?;

        let store = Self { pool };
        store.ensure_schema().await?;

        Ok(store)
    }

    /// Create a store from an existing connection pool.
    pub async fn from_pool(pool: PgPool) -> ControlResult<Self> {
        let store = Self { pool };
        store.ensure_schema().await?;
        Ok(store)
    }

    /// Ensure the required tables exist.
    async fn ensure_schema(&self) -> ControlResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS deployments (
                id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                environment TEXT NOT NULL,
                commit_sha TEXT NOT NULL,
                artifact_url TEXT NOT NULL,
                functions JSONB NOT NULL,
                state TEXT NOT NULL,
                error TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS active_deployments (
                project_id TEXT NOT NULL,
                environment TEXT NOT NULL,
                deployment_id TEXT NOT NULL REFERENCES deployments(id) ON DELETE CASCADE,
                PRIMARY KEY (project_id, environment)
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_deployments_project_env
            ON deployments (project_id, environment)
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_deployments_state
            ON deployments (state)
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_deployments_created_at
            ON deployments (created_at DESC)
            "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Parse a row into a DeploymentRecord.
    fn row_to_record(row: &sqlx::postgres::PgRow) -> ControlResult<DeploymentRecord> {
        let id: String = row.get("id");
        let project_id: String = row.get("project_id");
        let environment: String = row.get("environment");
        let commit_sha: String = row.get("commit_sha");
        let artifact_url: String = row.get("artifact_url");
        let functions_json: serde_json::Value = row.get("functions");
        let state_str: String = row.get("state");
        let error: Option<String> = row.get("error");
        let created_at: chrono::DateTime<chrono::Utc> = row.get("created_at");
        let updated_at: chrono::DateTime<chrono::Utc> = row.get("updated_at");

        let functions: Vec<FunctionMetadata> =
            serde_json::from_value(functions_json).map_err(|e| {
                ControlError::Serialisation(format!("failed to deserialise functions: {e}"))
            })?;

        let state: PersistedState = state_str.parse().map_err(|e| {
            ControlError::Serialisation(format!("failed to parse state '{state_str}': {e}"))
        })?;

        Ok(DeploymentRecord {
            data: DeploymentData {
                id: DeploymentId::new(id),
                project_id: ProjectId::new(project_id),
                environment,
                commit_sha,
                artifact_url,
                functions,
                created_at,
                updated_at,
                error,
            },
            state,
        })
    }
}

#[async_trait]
impl DeploymentStore for PostgresStore {
    async fn insert(&self, record: &DeploymentRecord) -> ControlResult<()> {
        let functions_json = serde_json::to_value(&record.data.functions).map_err(|e| {
            ControlError::Serialisation(format!("failed to serialise functions: {e}"))
        })?;

        sqlx::query(
            r#"
            INSERT INTO deployments (
                id, project_id, environment, commit_sha, artifact_url,
                functions, state, error, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
        )
        .bind(record.data.id.as_str())
        .bind(record.data.project_id.as_str())
        .bind(&record.data.environment)
        .bind(&record.data.commit_sha)
        .bind(&record.data.artifact_url)
        .bind(&functions_json)
        .bind(record.state.as_str())
        .bind(&record.data.error)
        .bind(record.data.created_at)
        .bind(record.data.updated_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get(&self, id: &DeploymentId) -> ControlResult<Option<DeploymentRecord>> {
        let row = sqlx::query(
            r#"
            SELECT id, project_id, environment, commit_sha, artifact_url,
                   functions, state, error, created_at, updated_at
            FROM deployments
            WHERE id = $1
            "#,
        )
        .bind(id.as_str())
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(r) => Ok(Some(Self::row_to_record(&r)?)),
            None => Ok(None),
        }
    }

    async fn update_state(
        &self,
        id: &DeploymentId,
        state: PersistedState,
        error: Option<&str>,
    ) -> ControlResult<()> {
        let result = sqlx::query(
            r#"
            UPDATE deployments
            SET state = $1, error = $2, updated_at = NOW()
            WHERE id = $3
            "#,
        )
        .bind(state.as_str())
        .bind(error)
        .bind(id.as_str())
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(ControlError::DeploymentNotFound(id.to_string()));
        }

        Ok(())
    }

    async fn list(&self, filter: &DeploymentFilter) -> ControlResult<Vec<DeploymentRecord>> {
        let mut query = String::from(
            r#"
            SELECT id, project_id, environment, commit_sha, artifact_url,
                   functions, state, error, created_at, updated_at
            FROM deployments
            WHERE 1=1
            "#,
        );

        let mut params: Vec<String> = Vec::new();

        if let Some(ref project_id) = filter.project_id {
            params.push(project_id.as_str().to_owned());
            query.push_str(&format!(" AND project_id = ${}", params.len()));
        }

        if let Some(ref environment) = filter.environment {
            params.push(environment.clone());
            query.push_str(&format!(" AND environment = ${}", params.len()));
        }

        if let Some(state) = filter.state {
            params.push(state.as_str().to_owned());
            query.push_str(&format!(" AND state = ${}", params.len()));
        }

        query.push_str(" ORDER BY created_at DESC");

        if let Some(limit) = filter.limit {
            query.push_str(&format!(" LIMIT {limit}"));
        }

        if let Some(offset) = filter.offset {
            query.push_str(&format!(" OFFSET {offset}"));
        }

        let mut sqlx_query = sqlx::query(&query);
        for param in &params {
            sqlx_query = sqlx_query.bind(param);
        }

        let rows = sqlx_query.fetch_all(&self.pool).await?;

        rows.iter().map(Self::row_to_record).collect()
    }

    async fn get_active(
        &self,
        project_id: &ProjectId,
        environment: &str,
    ) -> ControlResult<Option<DeploymentRecord>> {
        let row = sqlx::query(
            r#"
            SELECT d.id, d.project_id, d.environment, d.commit_sha, d.artifact_url,
                   d.functions, d.state, d.error, d.created_at, d.updated_at
            FROM deployments d
            INNER JOIN active_deployments a ON d.id = a.deployment_id
            WHERE a.project_id = $1 AND a.environment = $2
            "#,
        )
        .bind(project_id.as_str())
        .bind(environment)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(r) => Ok(Some(Self::row_to_record(&r)?)),
            None => Ok(None),
        }
    }

    async fn set_active(
        &self,
        project_id: &ProjectId,
        environment: &str,
        deployment_id: &DeploymentId,
    ) -> ControlResult<()> {
        sqlx::query(
            r#"
            INSERT INTO active_deployments (project_id, environment, deployment_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (project_id, environment) DO UPDATE
            SET deployment_id = EXCLUDED.deployment_id
            "#,
        )
        .bind(project_id.as_str())
        .bind(environment)
        .bind(deployment_id.as_str())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn clear_active(&self, project_id: &ProjectId, environment: &str) -> ControlResult<()> {
        sqlx::query(
            r#"
            DELETE FROM active_deployments
            WHERE project_id = $1 AND environment = $2
            "#,
        )
        .bind(project_id.as_str())
        .bind(environment)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn delete(&self, id: &DeploymentId) -> ControlResult<()> {
        let result = sqlx::query(
            r#"
            DELETE FROM deployments WHERE id = $1
            "#,
        )
        .bind(id.as_str())
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(ControlError::DeploymentNotFound(id.to_string()));
        }

        Ok(())
    }
}

impl std::fmt::Debug for PostgresStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresStore").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::FunctionTrigger;

    fn get_database_url() -> Option<String> {
        std::env::var("DATABASE_URL").ok()
    }

    fn test_deployment() -> DeploymentRecord {
        let data = DeploymentData::new(
            ProjectId::new("test-project"),
            "production".to_owned(),
            "abc123def456".to_owned(),
            "s3://bucket/artifact.rootfs".to_owned(),
            vec![FunctionMetadata {
                name: "handler".to_owned(),
                trigger: FunctionTrigger::Http {
                    method: "GET".to_owned(),
                    path: "/hello".to_owned(),
                },
                memory_mb: 128,
                vcpus: 1,
            }],
        );
        DeploymentRecord::new(data)
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL (set DATABASE_URL)"]
    async fn insert_and_get() {
        let url = get_database_url().expect("DATABASE_URL not set");
        let store = PostgresStore::new(&url).await.expect("failed to connect");

        let record = test_deployment();
        let id = record.data.id.clone();

        store.insert(&record).await.expect("insert failed");

        let retrieved = store
            .get(&id)
            .await
            .expect("get failed")
            .expect("deployment not found");

        assert_eq!(retrieved.data.id, id);
        assert_eq!(retrieved.data.project_id.as_str(), "test-project");
        assert_eq!(retrieved.data.environment, "production");
        assert_eq!(retrieved.state, PersistedState::Pending);

        store.delete(&id).await.expect("delete failed");
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL (set DATABASE_URL)"]
    async fn update_state() {
        let url = get_database_url().expect("DATABASE_URL not set");
        let store = PostgresStore::new(&url).await.expect("failed to connect");

        let record = test_deployment();
        let id = record.data.id.clone();

        store.insert(&record).await.expect("insert failed");

        store
            .update_state(&id, PersistedState::Registering, None)
            .await
            .expect("update failed");

        let retrieved = store
            .get(&id)
            .await
            .expect("get failed")
            .expect("not found");
        assert_eq!(retrieved.state, PersistedState::Registering);

        store
            .update_state(&id, PersistedState::Failed, Some("test error"))
            .await
            .expect("update failed");

        let retrieved = store
            .get(&id)
            .await
            .expect("get failed")
            .expect("not found");
        assert_eq!(retrieved.state, PersistedState::Failed);
        assert_eq!(retrieved.data.error.as_deref(), Some("test error"));

        store.delete(&id).await.expect("delete failed");
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL (set DATABASE_URL)"]
    async fn active_deployment() {
        let url = get_database_url().expect("DATABASE_URL not set");
        let store = PostgresStore::new(&url).await.expect("failed to connect");

        let record = test_deployment();
        let id = record.data.id.clone();
        let project_id = record.data.project_id.clone();

        store.insert(&record).await.expect("insert failed");

        let active = store
            .get_active(&project_id, "production")
            .await
            .expect("get_active failed");
        assert!(active.is_none());

        store
            .set_active(&project_id, "production", &id)
            .await
            .expect("set_active failed");

        let active = store
            .get_active(&project_id, "production")
            .await
            .expect("get_active failed")
            .expect("should have active deployment");
        assert_eq!(active.data.id, id);

        store
            .clear_active(&project_id, "production")
            .await
            .expect("clear_active failed");

        let active = store
            .get_active(&project_id, "production")
            .await
            .expect("get_active failed");
        assert!(active.is_none());

        store.delete(&id).await.expect("delete failed");
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL (set DATABASE_URL)"]
    async fn list_with_filters() {
        let url = get_database_url().expect("DATABASE_URL not set");
        let store = PostgresStore::new(&url).await.expect("failed to connect");

        let mut record1 = test_deployment();
        record1.data.environment = "staging".to_owned();
        let id1 = record1.data.id.clone();

        let record2 = test_deployment();
        let id2 = record2.data.id.clone();

        store.insert(&record1).await.expect("insert failed");
        store.insert(&record2).await.expect("insert failed");

        let all = store
            .list(&DeploymentFilter::new().with_project(ProjectId::new("test-project")))
            .await
            .expect("list failed");
        assert!(all.len() >= 2);

        let staging = store
            .list(
                &DeploymentFilter::new()
                    .with_project(ProjectId::new("test-project"))
                    .with_environment("staging"),
            )
            .await
            .expect("list failed");
        assert!(staging.iter().any(|r| r.data.id == id1));

        store.delete(&id1).await.expect("delete failed");
        store.delete(&id2).await.expect("delete failed");
    }
}
