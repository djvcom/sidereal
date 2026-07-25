//! Deletion of telemetry partitions past their retention period.
//!
//! Storage is partitioned by date (`{signal}/date=YYYY-MM-DD/hour=HH/...`),
//! so retention removes whole date partitions once they fall outside the
//! configured window. This works uniformly across every storage backend;
//! provider-side lifecycle rules remain an alternative for S3-compatible
//! object stores.

use std::sync::Arc;

use chrono::{Days, NaiveDate, Utc};
use futures::StreamExt;
use object_store::path::Path;
use object_store::ObjectStore;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Duration;

use crate::config::RetentionConfig;
use crate::storage::Signal;
use crate::TelemetryError;

const SIGNALS: [Signal; 3] = [Signal::Traces, Signal::Metrics, Signal::Logs];

/// Summary of a completed retention sweep.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SweepReport {
    /// Number of date partitions removed.
    pub partitions_removed: usize,
    /// Number of objects deleted.
    pub objects_deleted: usize,
}

/// Deletes telemetry partitions older than the retention window.
pub struct RetentionSweeper {
    store: Arc<dyn ObjectStore>,
    days: u32,
}

impl RetentionSweeper {
    /// Create a sweeper that keeps `days` days of telemetry.
    pub fn new(store: Arc<dyn ObjectStore>, days: u32) -> Self {
        Self { store, days }
    }

    /// Number of days of telemetry the sweeper keeps.
    #[must_use]
    pub const fn days(&self) -> u32 {
        self.days
    }

    /// Remove every date partition dated more than the retention window
    /// before the current UTC date.
    ///
    /// # Errors
    ///
    /// Returns the first listing or deletion error from the object store.
    /// Partitions already deleted in this sweep stay deleted; the remainder
    /// are picked up by the next sweep.
    pub async fn sweep(&self) -> Result<SweepReport, TelemetryError> {
        let cutoff = Utc::now()
            .date_naive()
            .checked_sub_days(Days::new(u64::from(self.days)))
            .unwrap_or(NaiveDate::MIN);
        self.sweep_before(cutoff).await
    }

    async fn sweep_before(&self, cutoff: NaiveDate) -> Result<SweepReport, TelemetryError> {
        let mut report = SweepReport::default();

        for signal in SIGNALS {
            let prefix = Path::from(signal.as_str());
            let listing = self.store.list_with_delimiter(Some(&prefix)).await?;

            for partition in listing.common_prefixes {
                let Some(date) = partition_date(&partition) else {
                    continue;
                };
                if date >= cutoff {
                    continue;
                }

                let mut objects = self.store.list(Some(&partition));
                while let Some(object) = objects.next().await {
                    let object = object?;
                    self.store.delete(&object.location).await?;
                    report.objects_deleted += 1;
                }

                report.partitions_removed += 1;
                tracing::info!(
                    signal = %signal,
                    partition = %partition,
                    "Removed expired partition"
                );
            }
        }

        Ok(report)
    }
}

fn partition_date(partition: &Path) -> Option<NaiveDate> {
    let segment = partition.parts().last()?;
    let date = segment.as_ref().strip_prefix("date=")?.to_owned();
    NaiveDate::parse_from_str(&date, "%Y-%m-%d").ok()
}

/// Handle for controlling a background retention task.
pub struct RetentionHandle {
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: JoinHandle<()>,
}

impl RetentionHandle {
    /// Signal the background task to stop and wait for it to complete.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        let _ = self.join_handle.await;
    }
}

/// Start a background task that sweeps expired partitions on an interval.
///
/// The first sweep runs immediately so a server that was stopped for longer
/// than the sweep interval catches up on startup.
#[must_use]
pub fn start_background_retention(
    sweeper: Arc<RetentionSweeper>,
    config: &RetentionConfig,
) -> RetentionHandle {
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    let interval = Duration::from_secs(config.sweep_interval_secs);

    let join_handle = tokio::spawn(async move {
        tracing::info!(
            days = sweeper.days(),
            interval_secs = interval.as_secs(),
            "Starting background retention task"
        );

        let mut interval_timer = tokio::time::interval(interval);

        loop {
            tokio::select! {
                _ = interval_timer.tick() => {
                    match sweeper.sweep().await {
                        Ok(report) if report.partitions_removed > 0 => {
                            tracing::info!(
                                partitions_removed = report.partitions_removed,
                                objects_deleted = report.objects_deleted,
                                "Retention sweep complete"
                            );
                        }
                        Ok(_) => {
                            tracing::debug!("Retention sweep found nothing to remove");
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "Retention sweep failed");
                        }
                    }
                }
                _ = &mut shutdown_rx => {
                    tracing::info!("Shutdown signal received, stopping retention task");
                    break;
                }
            }
        }
    });

    RetentionHandle {
        shutdown_tx: Some(shutdown_tx),
        join_handle,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    async fn put(store: &InMemory, path: &str) {
        store
            .put(&Path::from(path), vec![0u8].into())
            .await
            .unwrap();
    }

    async fn paths(store: &InMemory) -> Vec<String> {
        let mut all: Vec<String> = store
            .list(None)
            .map(|object| object.unwrap().location.to_string())
            .collect()
            .await;
        all.sort();
        all
    }

    #[test]
    fn partition_dates_parse_from_prefixes() {
        assert_eq!(
            partition_date(&Path::from("traces/date=2026-07-25")),
            Some(date(2026, 7, 25))
        );
        assert_eq!(partition_date(&Path::from("traces/other")), None);
        assert_eq!(partition_date(&Path::from("traces/date=notadate")), None);
    }

    #[tokio::test]
    async fn sweep_removes_only_partitions_before_the_cutoff() {
        let store = Arc::new(InMemory::new());
        put(&store, "traces/date=2026-06-01/hour=00/a.parquet").await;
        put(&store, "traces/date=2026-06-01/hour=01/b.parquet").await;
        put(&store, "traces/date=2026-06-25/hour=00/c.parquet").await;
        put(&store, "traces/date=2026-07-01/hour=00/d.parquet").await;
        put(&store, "logs/date=2026-06-01/hour=05/e.parquet").await;
        put(&store, "metrics/unrelated/f.parquet").await;

        let sweeper = RetentionSweeper::new(store.clone(), 30);
        let report = sweeper.sweep_before(date(2026, 6, 25)).await.unwrap();

        assert_eq!(report.partitions_removed, 2);
        assert_eq!(report.objects_deleted, 3);
        assert_eq!(
            paths(&store).await,
            vec![
                "metrics/unrelated/f.parquet".to_owned(),
                "traces/date=2026-06-25/hour=00/c.parquet".to_owned(),
                "traces/date=2026-07-01/hour=00/d.parquet".to_owned(),
            ]
        );
    }

    #[tokio::test]
    async fn sweep_keeps_the_partition_dated_exactly_at_the_cutoff() {
        let store = Arc::new(InMemory::new());
        put(&store, "logs/date=2026-06-25/hour=00/a.parquet").await;

        let sweeper = RetentionSweeper::new(store.clone(), 0);
        let report = sweeper.sweep_before(date(2026, 6, 25)).await.unwrap();

        assert_eq!(report, SweepReport::default());
        assert_eq!(paths(&store).await.len(), 1);
    }

    #[tokio::test]
    async fn sweep_reports_nothing_on_an_empty_store() {
        let store = Arc::new(InMemory::new());
        let sweeper = RetentionSweeper::new(store, 7);

        let report = sweeper.sweep().await.unwrap();

        assert_eq!(report, SweepReport::default());
    }
}
