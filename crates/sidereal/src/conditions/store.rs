//! Storage of reliability conditions in the bucket.
//!
//! Conditions live beside the telemetry they are about, under
//! `config/conditions/`, so that the bucket remains the only thing to
//! back up or restore. Nothing is ever overwritten: each save writes a new
//! revision object, named by a time-ordered ID, and the newest revision is
//! the current definition. Retiring a condition writes a final revision
//! that says so. The trail of who changed what, and when, is therefore
//! simply the list of objects.
//!
//! A bucket lifecycle rule that expires telemetry must not cover the
//! `config/` prefix, or definitions will expire with it.

use std::collections::BTreeSet;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};

use super::Condition;
use crate::TelemetryError;

/// Where condition revisions are kept.
pub const PREFIX: &str = "config/conditions";

/// One saved version of a condition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Revision {
    /// Time-ordered identifier of this revision.
    pub revision: String,
    /// When it was saved.
    pub saved_at: DateTime<Utc>,
    /// Who saved it, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    /// Whether this revision retires the condition.
    #[serde(default)]
    pub retired: bool,
    /// The definition as of this revision.
    pub condition: Condition,
}

/// A condition as it currently stands.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Stored {
    /// The current definition.
    pub condition: Condition,
    /// When the condition was first saved. Its trial runs from here.
    pub created: DateTime<Utc>,
    /// When it was last changed.
    pub updated: DateTime<Utc>,
    /// The current revision's identifier.
    pub revision: String,
}

/// Reads and writes conditions in an object store.
pub struct ConditionStore {
    store: Arc<dyn ObjectStore>,
}

fn directory(id: &str) -> Path {
    Path::from(format!("{PREFIX}/{id}"))
}

/// When the revision with this file name was written, from its ID.
fn time_of(name: &str) -> Option<DateTime<Utc>> {
    let id = name.strip_suffix(".json")?;
    Some(ulid::Ulid::from_string(id).ok()?.datetime().into())
}

impl ConditionStore {
    /// Use `store` to hold conditions.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    /// Revision file names of one condition, oldest first.
    async fn revision_names(&self, id: &str) -> Result<Vec<String>, TelemetryError> {
        let mut names: Vec<String> = self
            .store
            .list(Some(&directory(id)))
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .filter_map(|meta| meta.location.filename().map(str::to_owned))
            .filter(|name| time_of(name).is_some())
            .collect();
        names.sort_unstable();
        Ok(names)
    }

    async fn read(&self, id: &str, name: &str) -> Result<Revision, TelemetryError> {
        let bytes = self
            .store
            .get(&directory(id).child(name))
            .await?
            .bytes()
            .await?;
        serde_json::from_slice(&bytes).map_err(|e| {
            TelemetryError::Config(format!("stored condition {id} is unreadable: {e}"))
        })
    }

    async fn write(
        &self,
        condition: &Condition,
        author: Option<String>,
        retired: bool,
    ) -> Result<Revision, TelemetryError> {
        let id = ulid::Ulid::new();
        let revision = Revision {
            revision: id.to_string(),
            saved_at: id.datetime().into(),
            author,
            retired,
            condition: condition.clone(),
        };
        let body = serde_json::to_vec_pretty(&revision)
            .map_err(|e| TelemetryError::Config(format!("cannot encode condition: {e}")))?;
        self.store
            .put(
                &directory(&condition.id).child(format!("{id}.json")),
                body.into(),
            )
            .await?;
        Ok(revision)
    }

    /// Save a definition as the newest revision of its condition.
    ///
    /// # Errors
    ///
    /// Returns an error if the object cannot be written.
    pub async fn save(
        &self,
        condition: &Condition,
        author: Option<String>,
    ) -> Result<Revision, TelemetryError> {
        self.write(condition, author, false).await
    }

    /// Retire a condition. Returns false if there was none to retire.
    ///
    /// # Errors
    ///
    /// Returns an error if storage cannot be read or written.
    pub async fn retire(&self, id: &str, author: Option<String>) -> Result<bool, TelemetryError> {
        let Some(current) = self.get(id).await? else {
            return Ok(false);
        };
        self.write(&current.condition, author, true).await?;
        Ok(true)
    }

    /// The current definition of a condition, or `None` if it does not
    /// exist or has been retired.
    ///
    /// # Errors
    ///
    /// Returns an error if storage cannot be read.
    pub async fn get(&self, id: &str) -> Result<Option<Stored>, TelemetryError> {
        let names = self.revision_names(id).await?;
        let (Some(first), Some(last)) = (names.first(), names.last()) else {
            return Ok(None);
        };
        let latest = self.read(id, last).await?;
        if latest.retired {
            return Ok(None);
        }
        Ok(Some(Stored {
            condition: latest.condition,
            created: time_of(first).unwrap_or(latest.saved_at),
            updated: latest.saved_at,
            revision: latest.revision,
        }))
    }

    /// Every revision of a condition, oldest first, including a retiring
    /// one.
    ///
    /// # Errors
    ///
    /// Returns an error if storage cannot be read.
    pub async fn history(&self, id: &str) -> Result<Vec<Revision>, TelemetryError> {
        let mut revisions = Vec::new();
        for name in self.revision_names(id).await? {
            revisions.push(self.read(id, &name).await?);
        }
        Ok(revisions)
    }

    /// Every current condition, in ID order.
    ///
    /// # Errors
    ///
    /// Returns an error if storage cannot be read.
    pub async fn list(&self) -> Result<Vec<Stored>, TelemetryError> {
        let prefix = Path::from(PREFIX);
        let objects: Vec<_> = self.store.list(Some(&prefix)).try_collect().await?;
        let mut ids: BTreeSet<String> = BTreeSet::new();
        for meta in objects {
            let mut rest = meta.location.prefix_match(&prefix).into_iter().flatten();
            if let (Some(id), Some(_revision), None) = (rest.next(), rest.next(), rest.next()) {
                ids.insert(id.as_ref().to_owned());
            }
        }
        let mut conditions = Vec::new();
        for id in ids {
            if let Some(stored) = self.get(&id).await? {
                conditions.push(stored);
            }
        }
        Ok(conditions)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;
    use crate::conditions::tests::GOOGLEBOT;

    fn condition() -> Condition {
        serde_json::from_str(GOOGLEBOT).unwrap()
    }

    fn store() -> (ConditionStore, Arc<InMemory>) {
        let bucket = Arc::new(InMemory::new());
        (ConditionStore::new(bucket.clone()), bucket)
    }

    #[tokio::test]
    async fn a_saved_condition_is_read_back_from_the_bucket_alone() {
        let (conditions, bucket) = store();
        assert!(conditions
            .get("googlebot-product-availability")
            .await
            .unwrap()
            .is_none());

        let saved = conditions
            .save(&condition(), Some("dana".to_owned()))
            .await
            .unwrap();

        // A different process, with only the bucket.
        let stored = ConditionStore::new(bucket)
            .get("googlebot-product-availability")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.condition, condition());
        assert_eq!(stored.revision, saved.revision);
        assert_eq!(stored.created, stored.updated);
    }

    #[tokio::test]
    async fn every_change_is_kept_and_the_newest_is_current() {
        let (conditions, _) = store();
        let original = condition();
        conditions
            .save(&original, Some("dana".to_owned()))
            .await
            .unwrap();

        // ULIDs order by millisecond; make sure the edit is later.
        tokio::time::sleep(std::time::Duration::from_millis(3)).await;
        let mut stricter = original.clone();
        stricter.target.percent = 99.5;
        conditions
            .save(&stricter, Some("sam".to_owned()))
            .await
            .unwrap();

        let stored = conditions.get(&original.id).await.unwrap().unwrap();
        assert_eq!(stored.condition.target.percent, 99.5);
        assert!(
            stored.updated > stored.created,
            "trial still runs from the first save"
        );

        let history = conditions.history(&original.id).await.unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].condition.target.percent, 99.0);
        assert_eq!(history[0].author.as_deref(), Some("dana"));
        assert_eq!(history[1].author.as_deref(), Some("sam"));
    }

    #[tokio::test]
    async fn a_retired_condition_disappears_but_its_history_does_not() {
        let (conditions, _) = store();
        let original = condition();
        conditions.save(&original, None).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(3)).await;

        assert!(conditions
            .retire(&original.id, Some("dana".to_owned()))
            .await
            .unwrap());
        assert!(conditions.get(&original.id).await.unwrap().is_none());
        assert!(conditions.list().await.unwrap().is_empty());
        assert!(
            !conditions.retire(&original.id, None).await.unwrap(),
            "already retired"
        );

        let history = conditions.history(&original.id).await.unwrap();
        assert_eq!(history.len(), 2);
        assert!(history[1].retired);
    }

    #[tokio::test]
    async fn listing_returns_each_current_condition_once() {
        let (conditions, bucket) = store();
        let mut first = condition();
        first.id = "b-second".to_owned();
        let mut second = condition();
        second.id = "a-first".to_owned();
        conditions.save(&first, None).await.unwrap();
        conditions.save(&second, None).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(3)).await;
        conditions.save(&first, None).await.unwrap();

        // Unrelated objects in the bucket are not mistaken for conditions.
        bucket
            .put(
                &Path::from("config/conditions/stray.json"),
                vec![1u8].into(),
            )
            .await
            .unwrap();
        bucket
            .put(
                &Path::from("traces/date=2026-11-01/hour=00/a.parquet"),
                vec![1u8].into(),
            )
            .await
            .unwrap();

        let ids: Vec<String> = conditions
            .list()
            .await
            .unwrap()
            .into_iter()
            .map(|stored| stored.condition.id)
            .collect();
        assert_eq!(ids, vec!["a-first", "b-second"]);
    }
}
