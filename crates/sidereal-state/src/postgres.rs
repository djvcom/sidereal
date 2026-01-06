//! PostgreSQL adapter for Queue backend.

use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;

use crate::error::QueueError;
use crate::traits::QueueBackend;
use crate::types::{Message, MessageId};

/// PostgreSQL Queue backend using SELECT FOR UPDATE SKIP LOCKED.
#[derive(Clone)]
pub struct PostgresQueue {
    pool: PgPool,
    table: String,
}

impl PostgresQueue {
    /// Create a new PostgreSQL queue backend.
    ///
    /// The table will be created if it doesn't exist.
    pub async fn new(url: &str, table: String) -> Result<Self, QueueError> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(url)
            .await
            .map_err(|e| QueueError::Connection(e.to_string()))?;

        let queue = Self { pool, table };
        queue.ensure_table().await?;

        Ok(queue)
    }

    /// Create the queue table if it doesn't exist.
    async fn ensure_table(&self) -> Result<(), QueueError> {
        let create_table = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                id BIGSERIAL PRIMARY KEY,
                queue_name TEXT NOT NULL,
                payload BYTEA NOT NULL,
                attempt INT NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                visible_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
            self.table
        );

        sqlx::query(&create_table)
            .execute(&self.pool)
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;

        // Create index for efficient queue polling
        let create_index = format!(
            r#"
            CREATE INDEX IF NOT EXISTS idx_{}_receive
            ON {} (queue_name, visible_at)
            WHERE visible_at <= NOW()
            "#,
            self.table.replace('.', "_"),
            self.table
        );

        sqlx::query(&create_index)
            .execute(&self.pool)
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;

        Ok(())
    }
}

#[async_trait]
impl QueueBackend for PostgresQueue {
    async fn publish(&self, queue: &str, message: &[u8]) -> Result<MessageId, QueueError> {
        let query = format!(
            r#"
            INSERT INTO {} (queue_name, payload)
            VALUES ($1, $2)
            RETURNING id
            "#,
            self.table
        );

        let row = sqlx::query(&query)
            .bind(queue)
            .bind(message)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;

        let id: i64 = row.get("id");
        Ok(MessageId::new(id.to_string()))
    }

    async fn receive(
        &self,
        queue: &str,
        visibility_timeout: Duration,
    ) -> Result<Option<Message>, QueueError> {
        let timeout_seconds = visibility_timeout.as_secs() as i64;

        // Use SELECT FOR UPDATE SKIP LOCKED to atomically claim a message
        let query = format!(
            r#"
            UPDATE {}
            SET
                visible_at = NOW() + INTERVAL '{} seconds',
                attempt = attempt + 1
            WHERE id = (
                SELECT id FROM {}
                WHERE queue_name = $1 AND visible_at <= NOW()
                ORDER BY id
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, payload, attempt, created_at
            "#,
            self.table, timeout_seconds, self.table
        );

        let result = sqlx::query(&query)
            .bind(queue)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;

        match result {
            Some(row) => {
                let id: i64 = row.get("id");
                let payload: Vec<u8> = row.get("payload");
                let attempt: i32 = row.get("attempt");
                let created_at: chrono::DateTime<chrono::Utc> = row.get("created_at");

                Ok(Some(Message {
                    id: MessageId::new(id.to_string()),
                    payload,
                    attempt: attempt as u32,
                    enqueued_at: SystemTime::from(created_at),
                }))
            }
            None => Ok(None),
        }
    }

    async fn ack(&self, queue: &str, message_id: &MessageId) -> Result<(), QueueError> {
        let id: i64 = message_id
            .as_str()
            .parse()
            .map_err(|_| QueueError::MessageNotFound(message_id.to_string()))?;

        let query = format!(
            r#"
            DELETE FROM {}
            WHERE id = $1 AND queue_name = $2
            "#,
            self.table
        );

        let result = sqlx::query(&query)
            .bind(id)
            .bind(queue)
            .execute(&self.pool)
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;

        if result.rows_affected() == 0 {
            return Err(QueueError::MessageNotFound(message_id.to_string()));
        }

        Ok(())
    }

    async fn nack(&self, queue: &str, message_id: &MessageId) -> Result<(), QueueError> {
        let id: i64 = message_id
            .as_str()
            .parse()
            .map_err(|_| QueueError::MessageNotFound(message_id.to_string()))?;

        // Make message immediately visible again
        let query = format!(
            r#"
            UPDATE {}
            SET visible_at = NOW()
            WHERE id = $1 AND queue_name = $2
            "#,
            self.table
        );

        let result = sqlx::query(&query)
            .bind(id)
            .bind(queue)
            .execute(&self.pool)
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))?;

        if result.rows_affected() == 0 {
            return Err(QueueError::MessageNotFound(message_id.to_string()));
        }

        Ok(())
    }
}

impl std::fmt::Debug for PostgresQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresQueue")
            .field("table", &self.table)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Integration tests require a running PostgreSQL instance
    // Run with: cargo test --features postgres -- --ignored
    //
    // Set DATABASE_URL environment variable, e.g.:
    // DATABASE_URL=postgres://postgres:postgres@localhost/sidereal_test

    fn get_database_url() -> Option<String> {
        std::env::var("DATABASE_URL").ok()
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL instance (set DATABASE_URL)"]
    async fn queue_basic_operations() {
        let url = get_database_url().expect("DATABASE_URL not set");
        let queue = PostgresQueue::new(&url, "test_queue".to_string())
            .await
            .expect("Failed to connect to PostgreSQL");

        // Publish a message
        let id = queue.publish("test", b"hello world").await.unwrap();

        // Receive the message
        let msg = queue
            .receive("test", Duration::from_secs(30))
            .await
            .unwrap()
            .expect("Expected a message");

        assert_eq!(msg.id, id);
        assert_eq!(msg.payload, b"hello world");
        assert_eq!(msg.attempt, 1);

        // Acknowledge the message
        queue.ack("test", &id).await.unwrap();

        // Should be no more messages
        assert!(queue
            .receive("test", Duration::from_secs(30))
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL instance (set DATABASE_URL)"]
    async fn queue_visibility_timeout() {
        let url = get_database_url().expect("DATABASE_URL not set");
        let queue = PostgresQueue::new(&url, "test_queue".to_string())
            .await
            .expect("Failed to connect to PostgreSQL");

        // Publish a message
        let id = queue
            .publish("timeout_test", b"test message")
            .await
            .unwrap();

        // Receive with short timeout
        let msg1 = queue
            .receive("timeout_test", Duration::from_secs(1))
            .await
            .unwrap()
            .expect("Expected a message");

        assert_eq!(msg1.attempt, 1);

        // Should not be visible yet
        assert!(queue
            .receive("timeout_test", Duration::from_secs(30))
            .await
            .unwrap()
            .is_none());

        // Wait for visibility timeout
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Should be visible again
        let msg2 = queue
            .receive("timeout_test", Duration::from_secs(30))
            .await
            .unwrap()
            .expect("Expected message to be visible again");

        assert_eq!(msg2.id, id);
        assert_eq!(msg2.attempt, 2);

        // Clean up
        queue.ack("timeout_test", &id).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL instance (set DATABASE_URL)"]
    async fn queue_nack() {
        let url = get_database_url().expect("DATABASE_URL not set");
        let queue = PostgresQueue::new(&url, "test_queue".to_string())
            .await
            .expect("Failed to connect to PostgreSQL");

        // Publish a message
        let id = queue.publish("nack_test", b"test message").await.unwrap();

        // Receive with long timeout
        let msg1 = queue
            .receive("nack_test", Duration::from_secs(300))
            .await
            .unwrap()
            .expect("Expected a message");

        assert_eq!(msg1.attempt, 1);

        // Nack the message
        queue.nack("nack_test", &id).await.unwrap();

        // Should be immediately visible
        let msg2 = queue
            .receive("nack_test", Duration::from_secs(30))
            .await
            .unwrap()
            .expect("Expected message after nack");

        assert_eq!(msg2.id, id);
        assert_eq!(msg2.attempt, 2);

        // Clean up
        queue.ack("nack_test", &id).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL instance (set DATABASE_URL)"]
    async fn queue_multiple_queues() {
        let url = get_database_url().expect("DATABASE_URL not set");
        let queue = PostgresQueue::new(&url, "test_queue".to_string())
            .await
            .expect("Failed to connect to PostgreSQL");

        // Publish to different queues
        let id1 = queue.publish("queue_a", b"message a").await.unwrap();
        let id2 = queue.publish("queue_b", b"message b").await.unwrap();

        // Receive from queue_a
        let msg_a = queue
            .receive("queue_a", Duration::from_secs(30))
            .await
            .unwrap()
            .expect("Expected message from queue_a");
        assert_eq!(msg_a.payload, b"message a");

        // Receive from queue_b
        let msg_b = queue
            .receive("queue_b", Duration::from_secs(30))
            .await
            .unwrap()
            .expect("Expected message from queue_b");
        assert_eq!(msg_b.payload, b"message b");

        // Clean up
        queue.ack("queue_a", &id1).await.unwrap();
        queue.ack("queue_b", &id2).await.unwrap();
    }
}
