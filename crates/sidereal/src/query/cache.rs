//! In-memory block cache in front of object storage.
//!
//! On remote object storage a query's latency is set by how many requests
//! it makes, not by how much it computes: every request costs tens of
//! milliseconds before the first byte arrives. Reading one Parquet file
//! takes several (footer, bloom filter, filter columns, result columns),
//! and a query touches many files.
//!
//! [`BlockCache`] wraps a store and serves ranged reads from fixed-size
//! blocks held in memory. A file smaller than one block is fetched whole
//! by the first read that touches it, so every later read of that file,
//! by this query or the next, costs nothing. Larger files are cached
//! block by block as they are read. Flushed files are immutable, so a
//! cached block never goes stale; the cache only has to bound its size,
//! which it does by evicting the least recently used blocks.

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    Attributes, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

/// Size of one cached block.
///
/// Large enough that a typical flushed file fits in a single block and is
/// fetched with one request; small enough that reading the footer of a
/// large compacted file does not pull in much unrelated data.
pub const BLOCK_BYTES: u64 = 4 * 1024 * 1024;

/// Fraction of the capacity kept after an eviction pass, as a percentage.
///
/// Evicting slightly below capacity means an eviction pass is not needed
/// again for every subsequent insert.
const EVICT_TO_PERCENT: usize = 90;

#[derive(Debug)]
struct Block {
    data: Bytes,
    meta: Arc<ObjectMeta>,
    last_used: u64,
}

#[derive(Debug, Default)]
struct State {
    blocks: HashMap<(Path, u64), Block>,
    bytes: usize,
    clock: u64,
}

impl State {
    fn tick(&mut self) -> u64 {
        self.clock = self.clock.wrapping_add(1);
        self.clock
    }

    fn insert(&mut self, key: (Path, u64), data: Bytes, meta: Arc<ObjectMeta>, capacity: usize) {
        if data.len() > capacity {
            return;
        }
        let last_used = self.tick();
        let added = data.len();
        let block = Block {
            data,
            meta,
            last_used,
        };
        if let Some(previous) = self.blocks.insert(key, block) {
            self.bytes = self.bytes.saturating_sub(previous.data.len());
        }
        self.bytes = self.bytes.saturating_add(added);

        if self.bytes > capacity {
            self.evict(capacity / 100 * EVICT_TO_PERCENT);
        }
    }

    /// Remove least recently used blocks until at most `target` bytes remain.
    fn evict(&mut self, target: usize) {
        let mut by_age: Vec<((Path, u64), u64, usize)> = self
            .blocks
            .iter()
            .map(|(key, block)| (key.clone(), block.last_used, block.data.len()))
            .collect();
        by_age.sort_unstable_by_key(|(_, last_used, _)| *last_used);

        for (key, _, size) in by_age {
            if self.bytes <= target {
                break;
            }
            self.blocks.remove(&key);
            self.bytes = self.bytes.saturating_sub(size);
        }
    }
}

/// An [`ObjectStore`] wrapper that caches ranged reads in memory.
///
/// Only plain bounded range reads are cached, which is how Parquet files
/// are read. Everything else, including writes and listings, passes
/// straight through to the wrapped store.
pub struct BlockCache {
    inner: Arc<dyn ObjectStore>,
    capacity: usize,
    state: Mutex<State>,
}

impl BlockCache {
    /// Wrap `inner`, holding at most `capacity_bytes` of cached data.
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>, capacity_bytes: usize) -> Self {
        Self {
            inner,
            capacity: capacity_bytes,
            state: Mutex::new(State::default()),
        }
    }

    /// Bytes currently held in the cache.
    #[must_use]
    pub fn cached_bytes(&self) -> usize {
        self.lock().bytes
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        // The state stays consistent if a holder panics: every update
        // completes its bookkeeping before anything that could unwind.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Serve `range` of `location` from cached blocks, fetching the ones
    /// that are missing.
    async fn get_cached(&self, location: &Path, range: Range<u64>) -> Result<GetResult> {
        let first = range.start / BLOCK_BYTES;
        let last = range.end.saturating_sub(1) / BLOCK_BYTES;

        let mut blocks: HashMap<u64, Bytes> = HashMap::new();
        let mut meta: Option<Arc<ObjectMeta>> = None;
        let mut missing: Vec<u64> = Vec::new();
        {
            let mut state = self.lock();
            let now = state.tick();
            for index in first..=last {
                match state.blocks.get_mut(&(location.clone(), index)) {
                    Some(block) => {
                        block.last_used = now;
                        blocks.insert(index, block.data.clone());
                        meta = Some(block.meta.clone());
                    }
                    None => missing.push(index),
                }
            }
        }

        // Fetch each run of adjacent missing blocks with a single request.
        let requests = contiguous_runs(&missing).into_iter().map(|run| async move {
            let byte_range = (run.start * BLOCK_BYTES)..(run.end * BLOCK_BYTES);
            let options = GetOptions {
                range: Some(GetRange::Bounded(byte_range)),
                ..Default::default()
            };
            let result = self.inner.get_opts(location, options).await?;
            let fetched_meta = Arc::new(result.meta.clone());
            let data = result.bytes().await?;
            Ok::<_, object_store::Error>((run, fetched_meta, data))
        });
        let fetched = futures::future::try_join_all(requests).await?;

        if !fetched.is_empty() {
            let mut state = self.lock();
            for (run, fetched_meta, data) in fetched {
                for (position, index) in run.enumerate() {
                    // A read past the end of the object returns fewer bytes
                    // than requested, so trailing blocks may be short or absent.
                    let Some(block) = block_slice(&data, position) else {
                        break;
                    };
                    state.insert(
                        (location.clone(), index),
                        block.clone(),
                        fetched_meta.clone(),
                        self.capacity,
                    );
                    blocks.insert(index, block);
                }
                meta = Some(fetched_meta);
            }
        }

        let Some(meta) = meta else {
            // Nothing cached and nothing fetched: let the store report it.
            let options = GetOptions {
                range: Some(GetRange::Bounded(range)),
                ..Default::default()
            };
            return self.inner.get_opts(location, options).await;
        };

        let end = range.end.min(meta.size);
        let mut chunks: Vec<Result<Bytes>> = Vec::new();
        for index in first..=last {
            let Some(block) = blocks.get(&index) else {
                continue;
            };
            let block_start = index * BLOCK_BYTES;
            let block_len = u64::try_from(block.len()).unwrap_or(u64::MAX);
            let from = range.start.max(block_start) - block_start;
            let to = end
                .min(block_start.saturating_add(block_len))
                .saturating_sub(block_start);
            if let (Ok(from), Ok(to)) = (usize::try_from(from), usize::try_from(to)) {
                if from < to {
                    chunks.push(Ok(block.slice(from..to)));
                }
            }
        }

        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::iter(chunks).boxed()),
            meta: meta.as_ref().clone(),
            range: range.start.min(end)..end,
            attributes: Attributes::default(),
        })
    }
}

/// The range of a request the cache can serve, if it is a plain bounded
/// read with no preconditions.
fn cacheable_range(options: &GetOptions) -> Option<Range<u64>> {
    let unconditional = options.if_match.is_none()
        && options.if_none_match.is_none()
        && options.if_modified_since.is_none()
        && options.if_unmodified_since.is_none()
        && options.version.is_none()
        && !options.head;
    match &options.range {
        Some(GetRange::Bounded(range)) if unconditional && range.start < range.end => {
            Some(range.clone())
        }
        _ => None,
    }
}

/// Group sorted block indices into half-open runs of adjacent indices.
fn contiguous_runs(indices: &[u64]) -> Vec<Range<u64>> {
    let mut runs: Vec<Range<u64>> = Vec::new();
    for &index in indices {
        match runs.last_mut() {
            Some(run) if run.end == index => run.end = index + 1,
            _ => runs.push(index..index + 1),
        }
    }
    runs
}

/// The `position`th block-sized slice of `data`, if it starts within it.
fn block_slice(data: &Bytes, position: usize) -> Option<Bytes> {
    let block = usize::try_from(BLOCK_BYTES).ok()?;
    let start = position.checked_mul(block)?;
    if start >= data.len() {
        return None;
    }
    let end = start.saturating_add(block).min(data.len());
    Some(data.slice(start..end))
}

impl fmt::Debug for BlockCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockCache")
            .field("inner", &self.inner)
            .field("capacity", &self.capacity)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for BlockCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlockCache({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for BlockCache {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        match cacheable_range(&options) {
            Some(range) => self.get_cached(location, range).await,
            None => self.inner.get_opts(location, options).await,
        }
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        {
            let mut state = self.lock();
            let mut freed = 0usize;
            state.blocks.retain(|(path, _), block| {
                let keep = path != location;
                if !keep {
                    freed = freed.saturating_add(block.data.len());
                }
                keep
            });
            state.bytes = state.bytes.saturating_sub(freed);
        }
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::as_conversions,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation
)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use object_store::memory::InMemory;

    use super::*;

    /// Counts ranged reads reaching the wrapped store.
    #[derive(Debug)]
    struct Counting {
        inner: InMemory,
        gets: AtomicUsize,
    }

    impl fmt::Display for Counting {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Counting")
        }
    }

    #[async_trait]
    impl ObjectStore for Counting {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            self.inner.get_opts(location, options).await
        }
        async fn delete(&self, location: &Path) -> Result<()> {
            self.inner.delete(location).await
        }
        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
            self.inner.copy(from, to).await
        }
        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    fn pattern(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    async fn cache_with(objects: &[(&str, usize)], capacity: usize) -> (BlockCache, Arc<Counting>) {
        let counting = Arc::new(Counting {
            inner: InMemory::new(),
            gets: AtomicUsize::new(0),
        });
        for (name, len) in objects {
            counting
                .put(&Path::from(*name), pattern(*len).into())
                .await
                .unwrap();
        }
        (BlockCache::new(counting.clone(), capacity), counting)
    }

    const BLOCK: usize = BLOCK_BYTES as usize;

    #[tokio::test]
    async fn small_object_is_fetched_once_for_any_number_of_reads() {
        let (cache, store) = cache_with(&[("a", 10_000)], 64 * BLOCK).await;
        let path = Path::from("a");
        let expected = pattern(10_000);

        // Footer-style read at the end, then reads elsewhere in the file.
        for range in [9_000..10_000u64, 0..100, 4_000..6_000, 0..10_000] {
            let bytes = cache.get_range(&path, range.clone()).await.unwrap();
            assert_eq!(
                &bytes[..],
                &expected[range.start as usize..range.end as usize]
            );
        }

        assert_eq!(store.gets.load(Ordering::SeqCst), 1);
        assert_eq!(cache.cached_bytes(), 10_000);
    }

    #[tokio::test]
    async fn read_spanning_blocks_matches_the_object() {
        let len = 2 * BLOCK + 1_000;
        let (cache, store) = cache_with(&[("big", len)], 64 * BLOCK).await;
        let path = Path::from("big");
        let expected = pattern(len);

        let range = (BLOCK as u64 + 10)..(2 * BLOCK as u64 + 500);
        let bytes = cache.get_range(&path, range.clone()).await.unwrap();
        assert_eq!(
            &bytes[..],
            &expected[range.start as usize..range.end as usize]
        );
        assert_eq!(
            store.gets.load(Ordering::SeqCst),
            1,
            "adjacent blocks share a request"
        );

        // Only the missing first block is fetched for an overlapping read.
        let bytes = cache.get_range(&path, 0..(BLOCK as u64 + 5)).await.unwrap();
        assert_eq!(&bytes[..], &expected[..BLOCK + 5]);
        assert_eq!(store.gets.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn range_past_the_end_is_clipped_like_the_store() {
        let (cache, _) = cache_with(&[("a", 1_000)], 64 * BLOCK).await;
        let path = Path::from("a");

        let result = cache
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(900..5_000)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(result.range, 900..1_000);
        assert_eq!(result.meta.size, 1_000);
        assert_eq!(result.bytes().await.unwrap().len(), 100);
    }

    #[tokio::test]
    async fn missing_object_is_an_error_and_is_not_cached() {
        let (cache, _) = cache_with(&[], 64 * BLOCK).await;
        let result = cache.get_range(&Path::from("nope"), 0..10).await;
        assert!(matches!(result, Err(object_store::Error::NotFound { .. })));
        assert_eq!(cache.cached_bytes(), 0);
    }

    #[tokio::test]
    async fn least_recently_used_blocks_are_evicted_at_capacity() {
        let (cache, store) = cache_with(&[("a", 600), ("b", 600), ("c", 600)], 1_500).await;
        let (a, b, c) = (Path::from("a"), Path::from("b"), Path::from("c"));

        cache.get_range(&a, 0..10).await.unwrap();
        cache.get_range(&b, 0..10).await.unwrap();
        cache.get_range(&a, 0..10).await.unwrap(); // a is now more recent than b
        cache.get_range(&c, 0..10).await.unwrap(); // exceeds capacity, evicts b
        assert!(cache.cached_bytes() <= 1_500);
        assert_eq!(store.gets.load(Ordering::SeqCst), 3);

        cache.get_range(&a, 0..10).await.unwrap();
        assert_eq!(store.gets.load(Ordering::SeqCst), 3, "a survived eviction");
        cache.get_range(&b, 0..10).await.unwrap();
        assert_eq!(store.gets.load(Ordering::SeqCst), 4, "b was evicted");
    }

    #[tokio::test]
    async fn delete_drops_cached_blocks() {
        let (cache, store) = cache_with(&[("a", 600)], 64 * BLOCK).await;
        let path = Path::from("a");

        cache.get_range(&path, 0..10).await.unwrap();
        cache.delete(&path).await.unwrap();

        assert_eq!(cache.cached_bytes(), 0);
        assert!(cache.get_range(&path, 0..10).await.is_err());
        assert_eq!(store.gets.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn unbounded_reads_bypass_the_cache() {
        let (cache, store) = cache_with(&[("a", 600)], 64 * BLOCK).await;
        let path = Path::from("a");

        cache.get(&path).await.unwrap();
        cache.get(&path).await.unwrap();

        assert_eq!(store.gets.load(Ordering::SeqCst), 2);
        assert_eq!(cache.cached_bytes(), 0);
    }

    #[test]
    fn runs_group_adjacent_indices() {
        assert_eq!(contiguous_runs(&[]), Vec::<Range<u64>>::new());
        assert_eq!(contiguous_runs(&[3]), vec![3..4]);
        assert_eq!(contiguous_runs(&[0, 1, 2, 5, 7, 8]), vec![0..3, 5..6, 7..9]);
    }
}
