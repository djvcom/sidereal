//! In-memory catalogue of the objects in storage.
//!
//! Listing a bucket is slow (a round trip per directory level, per query)
//! and gets slower as data accumulates. [`CatalogueStore`] wraps the object
//! store used by the whole process and keeps the set of objects in memory,
//! so listings are answered without touching storage.
//!
//! The bucket remains the only source of truth. The catalogue is a view of
//! it that can always be rebuilt, which is what makes it safe:
//!
//! - **Restart or crash**: the catalogue is rebuilt by listing the bucket
//!   once at startup. There is nothing else to recover or back up.
//! - **Our own writes and deletes** (flushes, retention) go through the
//!   wrapper and update the catalogue immediately.
//! - **Objects removed behind our back**, for example by a bucket lifecycle
//!   rule, are noticed in two ways: a read that finds an object missing
//!   drops it from the catalogue on the spot, and a periodic refresh
//!   re-lists the bucket to reconcile everything else.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::{self, BoxStream, StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// What a refresh changed in the catalogue.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RefreshReport {
    /// Objects found in storage that the catalogue did not know about.
    pub added: usize,
    /// Objects the catalogue held that are no longer in storage.
    pub removed: usize,
}

/// An [`ObjectStore`] wrapper that answers listings from memory.
///
/// Every component that writes or deletes telemetry must use the same
/// wrapper instance, so that the catalogue sees those changes as they
/// happen.
pub struct CatalogueStore {
    inner: Arc<dyn ObjectStore>,
    objects: RwLock<BTreeMap<Path, ObjectMeta>>,
    /// Serialises the operations that reconcile or rewrite whole sets of
    /// objects (refresh and compaction), so neither sees the other's
    /// half-finished work.
    maintenance: tokio::sync::Mutex<()>,
}

impl CatalogueStore {
    /// Wrap `inner` and build the catalogue by listing everything in it.
    ///
    /// # Errors
    ///
    /// Returns an error if storage cannot be listed.
    pub async fn load(inner: Arc<dyn ObjectStore>) -> Result<Self> {
        let catalogue = Self {
            inner,
            objects: RwLock::new(BTreeMap::new()),
            maintenance: tokio::sync::Mutex::new(()),
        };
        catalogue.refresh().await?;
        Ok(catalogue)
    }

    /// Number of objects in the catalogue.
    #[must_use]
    pub fn len(&self) -> usize {
        self.read().len()
    }

    /// Whether the catalogue holds no objects.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.read().is_empty()
    }

    /// The wrapped store, for writing objects that must not appear in the
    /// catalogue until they are committed with [`CatalogueStore::replace`].
    #[must_use]
    pub fn inner(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.inner)
    }

    /// Take the lock that serialises refresh and compaction.
    pub async fn maintenance(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.maintenance.lock().await
    }

    /// Catalogued objects strictly below `prefix`, in path order.
    #[must_use]
    pub fn objects(&self, prefix: Option<&Path>) -> Vec<ObjectMeta> {
        self.below(prefix)
    }

    /// Swap `inputs` for `outputs` in one step.
    ///
    /// Queries planned before the call see the inputs, queries planned
    /// after it see the outputs, and none sees both or neither. This is
    /// what lets compaction rewrite files underneath running queries.
    pub fn replace(&self, outputs: Vec<ObjectMeta>, inputs: &[Path]) {
        let mut objects = self.write();
        for path in inputs {
            objects.remove(path);
        }
        for meta in outputs {
            objects.insert(meta.location.clone(), meta);
        }
    }

    /// Drop objects from the catalogue without touching storage.
    pub fn forget_all(&self, paths: &[Path]) {
        let mut objects = self.write();
        for path in paths {
            objects.remove(path);
        }
    }

    /// Reconcile the catalogue with what is actually in storage.
    ///
    /// Objects written through this wrapper while the listing was in
    /// progress are kept even if the listing missed them. Objects that an
    /// unfinished or uncleared compaction says should not be visible are
    /// left out, so an interrupted compaction can never show data twice.
    ///
    /// # Errors
    ///
    /// Returns an error if storage cannot be listed; the catalogue is left
    /// unchanged.
    pub async fn refresh(&self) -> Result<RefreshReport> {
        let _guard = self.maintenance().await;
        self.refresh_locked().await
    }

    async fn refresh_locked(&self) -> Result<RefreshReport> {
        let started = Utc::now();
        let mut listed: Vec<ObjectMeta> = self.inner.list(None).try_collect().await?;
        let hidden = crate::compaction::hidden_by_markers(self.inner.as_ref(), &listed).await;
        listed.retain(|meta| !hidden.contains(&meta.location));
        let listed_paths: BTreeSet<&Path> = listed.iter().map(|meta| &meta.location).collect();

        let mut report = RefreshReport::default();
        let mut objects = self.write();

        let before = objects.len();
        objects.retain(|path, meta| listed_paths.contains(path) || meta.last_modified >= started);
        report.removed = before - objects.len();

        for meta in listed {
            if !objects.contains_key(&meta.location) {
                report.added += 1;
            }
            objects.insert(meta.location.clone(), meta);
        }
        Ok(report)
    }

    fn read(&self) -> RwLockReadGuard<'_, BTreeMap<Path, ObjectMeta>> {
        self.objects.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> RwLockWriteGuard<'_, BTreeMap<Path, ObjectMeta>> {
        self.objects.write().unwrap_or_else(PoisonError::into_inner)
    }

    fn record(&self, location: &Path, size: u64, last_modified: DateTime<Utc>, put: &PutResult) {
        let meta = ObjectMeta {
            location: location.clone(),
            last_modified,
            size,
            e_tag: put.e_tag.clone(),
            version: put.version.clone(),
        };
        self.write().insert(location.clone(), meta);
    }

    fn forget(&self, location: &Path) {
        self.write().remove(location);
    }

    /// Record an object that was created by a route which does not report
    /// its size, by asking storage for its metadata.
    async fn record_from_storage(&self, location: &Path) {
        match self.inner.head(location).await {
            Ok(meta) => {
                self.write().insert(location.clone(), meta);
            }
            Err(error) => {
                tracing::warn!(%location, %error, "Could not catalogue new object until the next refresh");
            }
        }
    }

    /// Catalogued objects strictly below `prefix`, in path order.
    fn below(&self, prefix: Option<&Path>) -> Vec<ObjectMeta> {
        let objects = self.read();
        match prefix {
            None => objects.values().cloned().collect(),
            Some(prefix) => objects
                .range(prefix.clone()..)
                .take_while(|(path, _)| path.as_ref().starts_with(prefix.as_ref()))
                .filter(|(path, _)| {
                    path.prefix_match(prefix)
                        .is_some_and(|mut rest| rest.next().is_some())
                })
                .map(|(_, meta)| meta.clone())
                .collect(),
        }
    }
}

impl fmt::Debug for CatalogueStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CatalogueStore")
            .field("inner", &self.inner)
            .field("objects", &self.len())
            .finish_non_exhaustive()
    }
}

impl fmt::Display for CatalogueStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CatalogueStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CatalogueStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let size = u64::try_from(payload.content_length()).unwrap_or(u64::MAX);
        let result = self.inner.put_opts(location, payload, opts).await?;
        self.record(location, size, Utc::now(), &result);
        Ok(result)
    }

    /// Multipart uploads are not tracked as they complete; the object is
    /// catalogued by the next refresh. Flushes use single-request puts.
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let result = self.inner.get_opts(location, options).await;
        if matches!(result, Err(object_store::Error::NotFound { .. })) {
            self.forget(location);
        }
        result
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let result = self.inner.delete(location).await;
        if matches!(result, Ok(()) | Err(object_store::Error::NotFound { .. })) {
            self.forget(location);
        }
        result
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        stream::iter(self.below(prefix).into_iter().map(Ok)).boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let root = prefix.cloned().unwrap_or_default();
        let mut common_prefixes: BTreeSet<Path> = BTreeSet::new();
        let mut objects = Vec::new();

        for meta in self.below(prefix) {
            let child = meta.location.prefix_match(&root).and_then(|mut rest| {
                let first = rest.next()?;
                Some((root.child(first), rest.next().is_some()))
            });
            match child {
                Some((directory, true)) => {
                    common_prefixes.insert(directory);
                }
                Some((_, false)) => objects.push(meta),
                None => {}
            }
        }

        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy(from, to).await?;
        self.record_from_storage(to).await;
        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy_if_not_exists(from, to).await?;
        self.record_from_storage(to).await;
        Ok(())
    }
}

/// Handle for the background catalogue refresh task.
pub struct CatalogueRefreshHandle {
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: JoinHandle<()>,
}

impl CatalogueRefreshHandle {
    /// Stop the background task and wait for it to finish.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        let _ = self.join_handle.await;
    }
}

/// Periodically reconcile `catalogue` with storage.
///
/// A failed refresh is logged and retried at the next interval; the
/// catalogue keeps serving its previous view in the meantime.
pub fn start_background_refresh(
    catalogue: Arc<CatalogueStore>,
    interval: Duration,
) -> CatalogueRefreshHandle {
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();

    let join_handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await; // the first tick fires immediately; load() just listed

        loop {
            tokio::select! {
                _ = ticker.tick() => match catalogue.refresh().await {
                    Ok(report) if report.added > 0 || report.removed > 0 => {
                        tracing::info!(
                            added = report.added,
                            removed = report.removed,
                            objects = catalogue.len(),
                            "Catalogue reconciled with storage"
                        );
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(%error, "Catalogue refresh failed, keeping previous view");
                    }
                },
                _ = &mut shutdown_rx => break,
            }
        }
    });

    CatalogueRefreshHandle {
        shutdown_tx: Some(shutdown_tx),
        join_handle,
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::as_conversions,
    clippy::indexing_slicing
)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;

    async fn put(store: &dyn ObjectStore, path: &str) {
        store
            .put(&Path::from(path), vec![0u8; 10].into())
            .await
            .unwrap();
    }

    async fn listed(store: &dyn ObjectStore, prefix: Option<&str>) -> Vec<String> {
        let prefix = prefix.map(Path::from);
        let metas: Vec<ObjectMeta> = store.list(prefix.as_ref()).try_collect().await.unwrap();
        metas.into_iter().map(|m| m.location.to_string()).collect()
    }

    /// Paths chosen to catch prefix mistakes: a sibling that shares a
    /// string prefix but not a path segment, and nested partitions.
    const PATHS: [&str; 6] = [
        "logs/date=2026-11-01/hour=00/c.parquet",
        "traces-archive/x.parquet",
        "traces/date=2026-11-01/hour=00/a.parquet",
        "traces/date=2026-11-01/hour=01/b.parquet",
        "traces/date=2026-11-02/hour=00/d.parquet",
        "traces/loose.parquet",
    ];

    async fn loaded() -> (CatalogueStore, Arc<InMemory>) {
        let inner = Arc::new(InMemory::new());
        for path in PATHS {
            put(inner.as_ref(), path).await;
        }
        (CatalogueStore::load(inner.clone()).await.unwrap(), inner)
    }

    #[tokio::test]
    async fn listings_match_the_underlying_store() {
        let (catalogue, inner) = loaded().await;

        for prefix in [
            None,
            Some("traces"),
            Some("traces/date=2026-11-01"),
            Some("traces/date=2026-11-01/hour=00"),
            Some("traces/loose.parquet"),
            Some("trace"),
            Some("missing"),
        ] {
            assert_eq!(
                listed(&catalogue, prefix).await,
                listed(inner.as_ref(), prefix).await,
                "list({prefix:?})"
            );

            let path = prefix.map(Path::from);
            let ours = catalogue.list_with_delimiter(path.as_ref()).await.unwrap();
            let theirs = inner.list_with_delimiter(path.as_ref()).await.unwrap();
            assert_eq!(ours.common_prefixes, theirs.common_prefixes, "{prefix:?}");
            let locations = |result: &ListResult| -> Vec<Path> {
                result.objects.iter().map(|m| m.location.clone()).collect()
            };
            assert_eq!(locations(&ours), locations(&theirs), "{prefix:?}");
        }
    }

    #[tokio::test]
    async fn writes_and_deletes_through_the_wrapper_are_seen_immediately() {
        let (catalogue, _) = loaded().await;
        let path = "traces/date=2026-11-03/hour=05/new.parquet";

        put(&catalogue, path).await;
        assert!(listed(&catalogue, Some("traces/date=2026-11-03"))
            .await
            .contains(&path.to_owned()));
        let meta = catalogue.list(None).try_collect::<Vec<_>>().await.unwrap();
        let new = meta.iter().find(|m| m.location.as_ref() == path).unwrap();
        assert_eq!(new.size, 10);

        catalogue.delete(&Path::from(path)).await.unwrap();
        assert!(listed(&catalogue, Some("traces/date=2026-11-03"))
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn reading_an_object_that_vanished_drops_it_from_the_catalogue() {
        let (catalogue, inner) = loaded().await;
        let path = Path::from("traces/date=2026-11-01/hour=00/a.parquet");

        // Removed behind the catalogue's back, as a lifecycle rule would.
        inner.delete(&path).await.unwrap();
        assert_eq!(catalogue.len(), PATHS.len(), "not yet noticed");

        let result = catalogue.get(&path).await;
        assert!(matches!(result, Err(object_store::Error::NotFound { .. })));
        assert_eq!(catalogue.len(), PATHS.len() - 1);
        assert!(listed(&catalogue, Some("traces/date=2026-11-01/hour=00"))
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn refresh_reconciles_external_changes() {
        let (catalogue, inner) = loaded().await;

        inner
            .delete(&Path::from("traces/loose.parquet"))
            .await
            .unwrap();
        put(
            inner.as_ref(),
            "traces/date=2026-11-04/hour=00/other-writer.parquet",
        )
        .await;

        let report = catalogue.refresh().await.unwrap();
        assert_eq!(
            report,
            RefreshReport {
                added: 1,
                removed: 1
            }
        );
        assert_eq!(
            listed(&catalogue, None).await,
            listed(inner.as_ref(), None).await
        );
    }

    #[tokio::test]
    async fn refresh_keeps_objects_written_while_it_was_listing() {
        let (catalogue, _) = loaded().await;

        // An entry newer than the refresh's start that the listing did not
        // return, as happens when a flush lands mid-listing.
        let path = Path::from("traces/date=2026-11-05/hour=00/racing.parquet");
        catalogue.write().insert(
            path.clone(),
            ObjectMeta {
                location: path.clone(),
                last_modified: Utc::now() + chrono::Duration::seconds(5),
                size: 1,
                e_tag: None,
                version: None,
            },
        );

        let report = catalogue.refresh().await.unwrap();
        assert_eq!(report.removed, 0);
        assert!(catalogue.read().contains_key(&path));
    }

    #[tokio::test]
    async fn restart_rebuilds_the_same_catalogue() {
        let (catalogue, inner) = loaded().await;
        put(&catalogue, "traces/date=2026-11-06/hour=00/flushed.parquet").await;

        let restarted = CatalogueStore::load(inner).await.unwrap();
        assert_eq!(
            listed(&restarted, None).await,
            listed(&catalogue, None).await
        );
    }
}
