//! Write-ahead log for buffered telemetry.
//!
//! Buffered record batches exist only in memory between flushes, so a crash
//! would silently drop every acknowledged-but-unflushed row. The write-ahead
//! log makes acknowledgement durable: each batch is appended to an on-disk
//! segment before it enters the buffer, every segment is sealed when a flush
//! begins, and sealed segments are removed only once the data they cover is
//! confirmed in object storage. On startup, leftover segments are replayed
//! into the buffer, restoring anything a previous process failed to flush.
//!
//! Segments use the Arrow IPC stream format under ULID filenames, so
//! lexicographic order is chronological order. A crash can tear the tail of
//! the active segment; reads stop at the first corrupt message and discard
//! the remainder, so a torn final append cannot poison the log. With `fsync`
//! disabled (the default) appends survive a process crash but not
//! necessarily host power loss; enabling `fsync` extends the guarantee to
//! power loss at a per-append latency cost. Removal is deferred until after
//! upload, so a crash between upload and removal replays already-flushed
//! rows: delivery to object storage is at-least-once, not exactly-once.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use tokio::sync::Mutex;

use crate::storage::Signal;
use crate::TelemetryError;

const SEGMENT_EXTENSION: &str = "wal";

struct ActiveSegment {
    path: PathBuf,
    writer: StreamWriter<File>,
    sync_handle: File,
}

/// Append-only log of buffered batches for one telemetry signal.
pub struct Wal {
    signal: Signal,
    dir: PathBuf,
    schema: SchemaRef,
    fsync: bool,
    active: Mutex<ActiveSegment>,
}

impl Wal {
    /// Open the log for a signal, creating its directory and a fresh active
    /// segment.
    ///
    /// Segments left behind by a previous process are not touched here; they
    /// are surfaced by [`Wal::sealed_segments`] for replay and removed by the
    /// first successful flush.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory or the initial segment file cannot
    /// be created.
    pub fn open(
        root: &Path,
        signal: Signal,
        schema: SchemaRef,
        fsync: bool,
    ) -> Result<Self, TelemetryError> {
        let dir = root.join(signal.as_str());
        std::fs::create_dir_all(&dir)?;
        let active = open_segment(&dir, &schema)?;
        Ok(Self {
            signal,
            dir,
            schema,
            fsync,
            active: Mutex::new(active),
        })
    }

    /// Append a batch to the active segment.
    ///
    /// Returns once the bytes have reached the file (and the disk, when
    /// `fsync` is enabled), making it safe to acknowledge the batch upstream.
    ///
    /// # Errors
    ///
    /// Returns an error if writing or synchronising the segment file fails;
    /// the batch must then be rejected rather than acknowledged.
    pub async fn append(&self, batch: &RecordBatch) -> Result<(), TelemetryError> {
        let mut active = self.active.lock().await;
        active.writer.write(batch)?;
        if self.fsync {
            active.sync_handle.sync_data()?;
        }
        Ok(())
    }

    /// Seal every existing segment and start a new active one.
    ///
    /// Called at the start of a flush: batches taken from the buffer are
    /// exactly the batches in the sealed segments (plus any left over from
    /// earlier failed flushes, which the same flush also carries). The
    /// returned paths must be passed to [`Wal::remove_segments`] only after
    /// the flush confirms their contents in object storage.
    ///
    /// # Errors
    ///
    /// Returns an error if the active segment cannot be finished or a
    /// replacement segment cannot be created; existing segments are left in
    /// place in that case.
    pub async fn seal_and_rotate(&self) -> Result<Vec<PathBuf>, TelemetryError> {
        let mut active = self.active.lock().await;
        active.writer.finish()?;
        if self.fsync {
            active.sync_handle.sync_data()?;
        }
        let replacement = open_segment(&self.dir, &self.schema)?;
        drop(std::mem::replace(&mut *active, replacement));
        self.segments_except(&active.path)
    }

    /// List every segment except the current active one, oldest first.
    ///
    /// # Errors
    ///
    /// Returns an error if the log directory cannot be read.
    pub async fn sealed_segments(&self) -> Result<Vec<PathBuf>, TelemetryError> {
        let active = self.active.lock().await;
        self.segments_except(&active.path)
    }

    /// Remove segments whose contents are confirmed in object storage.
    ///
    /// Failures are logged rather than propagated: a leftover segment merely
    /// causes a harmless replay on the next startup.
    pub fn remove_segments(&self, paths: &[PathBuf]) {
        for path in paths {
            if let Err(e) = std::fs::remove_file(path) {
                tracing::warn!(
                    signal = %self.signal,
                    path = %path.display(),
                    error = %e,
                    "Failed to remove flushed WAL segment"
                );
            }
        }
    }

    /// Read every batch from a segment, stopping at the first corrupt
    /// message (a torn write from a crash mid-append).
    ///
    /// A segment whose header is unreadable is treated as empty: whatever it
    /// held is unrecoverable, and failing recovery outright would also strand
    /// every later, intact segment.
    ///
    /// # Errors
    ///
    /// Returns an error only if the segment file cannot be opened; corrupt
    /// contents are tolerated as described above.
    pub fn read_segment(&self, path: &Path) -> Result<Vec<RecordBatch>, TelemetryError> {
        let file = File::open(path)?;
        let reader = match StreamReader::try_new(file, None) {
            Ok(reader) => reader,
            Err(e) => {
                tracing::warn!(
                    signal = %self.signal,
                    path = %path.display(),
                    error = %e,
                    "WAL segment header unreadable; treating segment as empty"
                );
                return Ok(Vec::new());
            }
        };

        let mut batches = Vec::new();
        for result in reader {
            match result {
                Ok(batch) => batches.push(batch),
                Err(e) => {
                    tracing::warn!(
                        signal = %self.signal,
                        path = %path.display(),
                        error = %e,
                        "WAL segment truncated mid-message; discarding remainder"
                    );
                    break;
                }
            }
        }
        Ok(batches)
    }

    fn segments_except(&self, active_path: &Path) -> Result<Vec<PathBuf>, TelemetryError> {
        let mut segments = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let path = entry?.path();
            let is_segment = path.extension().is_some_and(|ext| ext == SEGMENT_EXTENSION);
            if is_segment && path != active_path {
                segments.push(path);
            }
        }
        segments.sort();
        Ok(segments)
    }
}

fn open_segment(dir: &Path, schema: &SchemaRef) -> Result<ActiveSegment, TelemetryError> {
    let path = dir.join(format!("{}.{SEGMENT_EXTENSION}", ulid::Ulid::new()));
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)?;
    let sync_handle = file.try_clone()?;
    let writer = StreamWriter::try_new(file, schema)?;
    Ok(ActiveSegment {
        path,
        writer,
        sync_handle,
    })
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::Arc;

    use arrow::array::UInt64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]))
    }

    fn test_batch(schema: &SchemaRef, start: u64, rows: u64) -> RecordBatch {
        let ids: Vec<u64> = (start..start + rows).collect();
        RecordBatch::try_new(schema.clone(), vec![Arc::new(UInt64Array::from(ids))]).unwrap()
    }

    #[tokio::test]
    async fn appended_batches_survive_rotation_and_replay() {
        let root = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let wal = Wal::open(root.path(), Signal::Traces, schema.clone(), false).unwrap();

        wal.append(&test_batch(&schema, 0, 5)).await.unwrap();
        wal.append(&test_batch(&schema, 5, 3)).await.unwrap();

        let sealed = wal.seal_and_rotate().await.unwrap();
        assert_eq!(sealed.len(), 1);

        let batches = wal.read_segment(&sealed[0]).unwrap();
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(batches.len(), 2);
        assert_eq!(rows, 8);
    }

    #[tokio::test]
    async fn a_reopened_wal_sees_previous_segments_as_sealed() {
        let root = tempfile::tempdir().unwrap();
        let schema = test_schema();

        let first = Wal::open(root.path(), Signal::Logs, schema.clone(), false).unwrap();
        first.append(&test_batch(&schema, 0, 4)).await.unwrap();
        drop(first);

        let second = Wal::open(root.path(), Signal::Logs, schema.clone(), false).unwrap();
        let sealed = second.sealed_segments().await.unwrap();
        assert_eq!(sealed.len(), 1);

        let rows: usize = sealed
            .iter()
            .flat_map(|path| second.read_segment(path).unwrap())
            .map(|batch| batch.num_rows())
            .sum();
        assert_eq!(rows, 4);
    }

    #[tokio::test]
    async fn a_torn_tail_is_discarded_without_losing_earlier_batches() {
        let root = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let wal = Wal::open(root.path(), Signal::Metrics, schema.clone(), false).unwrap();

        wal.append(&test_batch(&schema, 0, 6)).await.unwrap();
        wal.append(&test_batch(&schema, 6, 6)).await.unwrap();
        let sealed = wal.seal_and_rotate().await.unwrap();

        let path = &sealed[0];
        let intact_len = std::fs::metadata(path).unwrap().len();
        let mut file = OpenOptions::new().write(true).open(path).unwrap();
        file.set_len(intact_len - 20).unwrap();
        file.seek(SeekFrom::End(0)).unwrap();
        file.write_all(&[0xFF; 3]).unwrap();
        drop(file);

        let batches = wal.read_segment(path).unwrap();
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 6, "the first intact batch should survive");
    }

    #[tokio::test]
    async fn removed_segments_no_longer_appear_as_sealed() {
        let root = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let wal = Wal::open(root.path(), Signal::Traces, schema.clone(), false).unwrap();

        wal.append(&test_batch(&schema, 0, 2)).await.unwrap();
        let sealed = wal.seal_and_rotate().await.unwrap();
        assert_eq!(sealed.len(), 1);

        wal.remove_segments(&sealed);
        assert!(wal.sealed_segments().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_unreadable_header_is_treated_as_an_empty_segment() {
        let root = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let wal = Wal::open(root.path(), Signal::Logs, schema, false).unwrap();

        let bogus = root
            .path()
            .join("logs")
            .join("00000000000000000000000000.wal");
        std::fs::write(&bogus, b"not an arrow stream").unwrap();

        assert!(wal.read_segment(&bogus).unwrap().is_empty());
    }
}
