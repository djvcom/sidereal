//! In-memory index from trace ID to the files that hold it.
//!
//! A trace is fetched by its ID alone, with no time range, so without help
//! a lookup has to consider every trace file ever written. Each file can
//! be ruled out cheaply by its bloom filter, but "cheaply" still means
//! opening it, and the number of files grows with retention.
//!
//! [`TraceIndex`] keeps every trace file's bloom filters in memory. A
//! lookup probes them directly, which costs microseconds for thousands of
//! files and touches no storage, and comes back with the few files that
//! can contain the trace. Only those files are read.
//!
//! The index holds nothing that is not already in the bucket: the filters
//! are the ones written into each Parquet file. They are loaded on first
//! use and after a restart, and a file whose filter cannot be read is
//! simply treated as one that might contain any trace. Losing the index
//! therefore costs time, never correctness.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, PoisonError, RwLock};

use futures::stream::{self, StreamExt};
use object_store::path::Path;
use object_store::ObjectMeta;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use parquet::bloom_filter::Sbbf;

use crate::catalogue::CatalogueStore;
use crate::storage::Signal;

/// Name of the indexed column.
const TRACE_ID_COLUMN: &str = "trace_id";

/// How many files' filters are loaded from storage at once.
const LOAD_CONCURRENCY: usize = 32;

/// An hour partition, as it appears in paths and in the `date` and `hour`
/// columns.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Partition {
    /// `YYYY-MM-DD`.
    pub date: String,
    /// `HH`.
    pub hour: String,
}

impl Partition {
    /// Read the partition from a data file's path, if it is in one.
    fn of(path: &Path) -> Option<Self> {
        let mut date = None;
        let mut hour = None;
        for part in path.parts() {
            let part = part.as_ref();
            if let Some(value) = part.strip_prefix("date=") {
                date = Some(value.to_owned());
            } else if let Some(value) = part.strip_prefix("hour=") {
                hour = Some(value.to_owned());
            }
        }
        Some(Self {
            date: date?,
            hour: hour?,
        })
    }
}

/// What is known about the trace IDs in one file.
#[derive(Debug)]
enum FileFilter {
    /// One bloom filter per row group, and their total size in bytes.
    Blooms(Vec<Sbbf>, usize),
    /// The file has no usable filter; it may contain any trace.
    Unknown,
}

impl FileFilter {
    fn may_contain(&self, trace_id: &Vec<u8>) -> bool {
        match self {
            Self::Blooms(blooms, _) => blooms.iter().any(|bloom| bloom.check(trace_id)),
            Self::Unknown => true,
        }
    }

    fn bytes(&self) -> usize {
        match self {
            Self::Blooms(_, bytes) => *bytes,
            Self::Unknown => 0,
        }
    }
}

/// Index from trace ID to the files that may hold it.
pub struct TraceIndex {
    catalogue: Arc<CatalogueStore>,
    files: RwLock<HashMap<Path, Arc<FileFilter>>>,
}

impl TraceIndex {
    /// Create an empty index over a catalogued store. Filters are loaded
    /// as lookups need them, or all at once by [`TraceIndex::warm`].
    #[must_use]
    pub fn new(catalogue: Arc<CatalogueStore>) -> Self {
        Self {
            catalogue,
            files: RwLock::new(HashMap::new()),
        }
    }

    /// Number of files whose filters are held.
    #[must_use]
    pub fn len(&self) -> usize {
        self.read().len()
    }

    /// Whether no filters are held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.read().is_empty()
    }

    /// Approximate memory used by the held filters, in bytes.
    #[must_use]
    pub fn memory_bytes(&self) -> usize {
        self.read().values().map(|filter| filter.bytes()).sum()
    }

    /// Load the filters of every trace file not yet indexed.
    ///
    /// Called in the background after start so that the first lookup does
    /// not pay for it.
    pub async fn warm(&self) {
        let files = self.trace_files();
        self.load_missing(&files).await;
    }

    /// The files that may contain `trace_id`, in path order.
    ///
    /// Never misses a file that does contain the trace. May include one
    /// that does not: bloom filters give occasional false positives, and
    /// files without a filter always count.
    pub async fn files(&self, trace_id: &[u8; 16]) -> Vec<Path> {
        let files = self.trace_files();
        self.load_missing(&files).await;

        let needle = trace_id.to_vec();
        let filters = self.read();
        files
            .into_iter()
            .filter(|meta| {
                filters
                    .get(&meta.location)
                    .is_none_or(|filter| filter.may_contain(&needle))
            })
            .map(|meta| meta.location)
            .collect()
    }

    /// The hour partitions that may contain `trace_id`, in order.
    pub async fn partitions(&self, trace_id: &[u8; 16]) -> Vec<Partition> {
        let partitions: BTreeSet<Partition> = self
            .files(trace_id)
            .await
            .iter()
            .filter_map(Partition::of)
            .collect();
        partitions.into_iter().collect()
    }

    /// Trace data files currently in the catalogue.
    fn trace_files(&self) -> Vec<ObjectMeta> {
        let prefix = Path::from(Signal::Traces.as_str());
        self.catalogue
            .objects(Some(&prefix))
            .into_iter()
            .filter(|meta| {
                meta.location.filename().is_some_and(|name| {
                    name.rsplit_once('.')
                        .is_some_and(|(_, ext)| ext == "parquet")
                })
            })
            .collect()
    }

    /// Load filters for the files in `current` that have none yet, and
    /// forget files that are no longer in the catalogue.
    async fn load_missing(&self, current: &[ObjectMeta]) {
        let missing: Vec<ObjectMeta> = {
            let filters = self.read();
            current
                .iter()
                .filter(|meta| !filters.contains_key(&meta.location))
                .cloned()
                .collect()
        };

        let loaded: Vec<(Path, Option<FileFilter>)> = stream::iter(missing)
            .map(|meta| async move {
                let filter = self.load(&meta).await;
                (meta.location, filter)
            })
            .buffer_unordered(LOAD_CONCURRENCY)
            .collect()
            .await;

        let mut filters = self.write();
        for (path, filter) in loaded {
            // A file that vanished is left out: the catalogue will drop it
            // too, and until then the query treats it as a possible match.
            if let Some(filter) = filter {
                filters.insert(path, Arc::new(filter));
            }
        }
        if filters.len() > current.len() {
            let live: std::collections::HashSet<&Path> =
                current.iter().map(|meta| &meta.location).collect();
            filters.retain(|path, _| live.contains(path));
        }
    }

    /// Read one file's trace ID bloom filters from storage.
    ///
    /// Returns `None` if the file could not be read at all, so that the
    /// load is tried again later, and [`FileFilter::Unknown`] if it was
    /// read but has no filter for every row group.
    async fn load(&self, meta: &ObjectMeta) -> Option<FileFilter> {
        let reader = ParquetObjectReader::new(self.catalogue.inner(), meta.location.clone())
            .with_file_size(meta.size);
        let mut builder = match ParquetRecordBatchStreamBuilder::new(reader).await {
            Ok(builder) => builder,
            Err(error) => {
                tracing::debug!(path = %meta.location, %error, "Could not read trace file footer");
                return None;
            }
        };

        let metadata = Arc::clone(builder.metadata());
        let Some(column) = metadata
            .file_metadata()
            .schema_descr()
            .columns()
            .iter()
            .position(|column| column.name() == TRACE_ID_COLUMN)
        else {
            return Some(FileFilter::Unknown);
        };

        let mut blooms = Vec::with_capacity(metadata.num_row_groups());
        let mut bytes = 0usize;
        for row_group in 0..metadata.num_row_groups() {
            let length = metadata
                .row_group(row_group)
                .column(column)
                .bloom_filter_length()
                .and_then(|length| usize::try_from(length).ok())
                .unwrap_or(0);
            bytes = bytes.saturating_add(length);
            match builder
                .get_row_group_column_bloom_filter(row_group, column)
                .await
            {
                Ok(Some(bloom)) => blooms.push(bloom),
                // One row group without a filter means the file as a whole
                // cannot be ruled out.
                Ok(None) => return Some(FileFilter::Unknown),
                Err(error) => {
                    tracing::debug!(path = %meta.location, %error, "Could not read trace bloom filter");
                    return None;
                }
            }
        }
        Some(FileFilter::Blooms(blooms, bytes))
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<Path, Arc<FileFilter>>> {
        self.files.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<Path, Arc<FileFilter>>> {
        self.files.write().unwrap_or_else(PoisonError::into_inner)
    }
}

impl std::fmt::Debug for TraceIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TraceIndex")
            .field("files", &self.len())
            .finish_non_exhaustive()
    }
}
