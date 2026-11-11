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
//! Segments have ULID filenames, so lexicographic order is chronological
//! order. Each append is one frame: a four-byte little-endian length
//! followed by a complete Arrow IPC stream holding the batch and its own
//! schema. Carrying the schema per batch matters because batches differ in
//! which attribute columns they have. A crash can tear the tail of the
//! active segment; reads stop at the first incomplete or corrupt frame and
//! discard the remainder, so a torn final append cannot poison the log. With `fsync`
//! disabled (the default) appends survive a process crash but not
//! necessarily host power loss; enabling `fsync` extends the guarantee to
//! power loss at a per-append latency cost. Removal is deferred until after
//! upload, so a crash between upload and removal replays already-flushed
//! rows: delivery to object storage is at-least-once, not exactly-once.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use tokio::sync::Mutex;

use crate::storage::Signal;
use crate::TelemetryError;

const SEGMENT_EXTENSION: &str = "wal";

/// First four bytes of a segment written before frames were introduced:
/// the Arrow IPC continuation marker. No frame length can equal it.
const LEGACY_MARKER: [u8; 4] = [0xFF; 4];

struct ActiveSegment {
    path: PathBuf,
    file: File,
}

/// Append-only log of buffered batches for one telemetry signal.
pub struct Wal {
    signal: Signal,
    dir: PathBuf,
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
    pub fn open(root: &Path, signal: Signal, fsync: bool) -> Result<Self, TelemetryError> {
        let dir = root.join(signal.as_str());
        std::fs::create_dir_all(&dir)?;
        let active = open_segment(&dir)?;
        Ok(Self {
            signal,
            dir,
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
        let mut body = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut body, batch.schema_ref())?;
            writer.write(batch)?;
            writer.finish()?;
        }
        let length = u32::try_from(body.len()).map_err(|_| {
            TelemetryError::Config("batch too large for the write-ahead log".to_owned())
        })?;
        let mut frame = Vec::with_capacity(body.len() + 4);
        frame.extend_from_slice(&length.to_le_bytes());
        frame.extend_from_slice(&body);

        // One write per frame, so a crash leaves at most one torn frame.
        let mut active = self.active.lock().await;
        active.file.write_all(&frame)?;
        if self.fsync {
            active.file.sync_data()?;
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
        if self.fsync {
            active.file.sync_data()?;
        }
        let replacement = open_segment(&self.dir)?;
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
        let mut bytes = Vec::new();
        File::open(path)?.read_to_end(&mut bytes)?;

        if bytes.starts_with(&LEGACY_MARKER) {
            return Ok(self.read_ipc_stream(path, &bytes));
        }

        let mut batches = Vec::new();
        let mut rest = bytes.as_slice();
        while !rest.is_empty() {
            let frame = rest.split_first_chunk::<4>().and_then(|(length, tail)| {
                let length = usize::try_from(u32::from_le_bytes(*length)).ok()?;
                tail.split_at_checked(length)
            });
            let Some((body, tail)) = frame else {
                tracing::warn!(
                    signal = %self.signal,
                    path = %path.display(),
                    "WAL segment truncated mid-frame; discarding remainder"
                );
                break;
            };
            let frame_batches = self.read_ipc_stream(path, body);
            if frame_batches.is_empty() {
                // A frame always holds one batch; an unreadable one means
                // the rest of the segment cannot be trusted either.
                break;
            }
            batches.extend(frame_batches);
            rest = tail;
        }
        Ok(batches)
    }

    /// Read the batches of one Arrow IPC stream, stopping at the first
    /// message that cannot be decoded.
    fn read_ipc_stream(&self, path: &Path, bytes: &[u8]) -> Vec<RecordBatch> {
        let reader = match StreamReader::try_new(bytes, None) {
            Ok(reader) => reader,
            Err(e) => {
                tracing::warn!(
                    signal = %self.signal,
                    path = %path.display(),
                    error = %e,
                    "WAL data unreadable; discarding it"
                );
                return Vec::new();
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
                        "WAL data truncated mid-message; discarding remainder"
                    );
                    break;
                }
            }
        }
        batches
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

fn open_segment(dir: &Path) -> Result<ActiveSegment, TelemetryError> {
    let path = dir.join(format!("{}.{SEGMENT_EXTENSION}", ulid::Ulid::new()));
    let file = OpenOptions::new()
        .create_new(true)
        .append(true)
        .open(&path)?;
    Ok(ActiveSegment { path, file })
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
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

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
        let wal = Wal::open(root.path(), Signal::Traces, false).unwrap();

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

        let first = Wal::open(root.path(), Signal::Logs, false).unwrap();
        first.append(&test_batch(&schema, 0, 4)).await.unwrap();
        drop(first);

        let second = Wal::open(root.path(), Signal::Logs, false).unwrap();
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
        let wal = Wal::open(root.path(), Signal::Metrics, false).unwrap();

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
        let wal = Wal::open(root.path(), Signal::Traces, false).unwrap();

        wal.append(&test_batch(&schema, 0, 2)).await.unwrap();
        let sealed = wal.seal_and_rotate().await.unwrap();
        assert_eq!(sealed.len(), 1);

        wal.remove_segments(&sealed);
        assert!(wal.sealed_segments().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn batches_with_different_schemas_share_a_segment() {
        let root = tempfile::tempdir().unwrap();
        let wal = Wal::open(root.path(), Signal::Traces, false).unwrap();

        let narrow = test_schema();
        let wide = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("attr.int.extra", DataType::UInt64, true),
        ]));
        let wide_batch = RecordBatch::try_new(
            wide.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![7, 8])),
                Arc::new(UInt64Array::from(vec![Some(1), None])),
            ],
        )
        .unwrap();

        wal.append(&test_batch(&narrow, 0, 3)).await.unwrap();
        wal.append(&wide_batch).await.unwrap();
        wal.append(&test_batch(&narrow, 3, 1)).await.unwrap();
        let sealed = wal.seal_and_rotate().await.unwrap();

        let batches = wal.read_segment(&sealed[0]).unwrap();
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].schema(), narrow);
        assert_eq!(batches[1], wide_batch);
        assert_eq!(batches[2].num_rows(), 1);
    }

    #[tokio::test]
    async fn segments_from_before_framing_are_still_replayed() {
        let root = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let wal = Wal::open(root.path(), Signal::Logs, false).unwrap();

        // The previous format: one Arrow IPC stream for the whole segment.
        let legacy = root
            .path()
            .join("logs")
            .join("00000000000000000000000000.wal");
        let mut writer = StreamWriter::try_new(File::create(&legacy).unwrap(), &schema).unwrap();
        writer.write(&test_batch(&schema, 0, 4)).unwrap();
        writer.write(&test_batch(&schema, 4, 2)).unwrap();
        writer.finish().unwrap();
        drop(writer);

        let rows: usize = wal
            .read_segment(&legacy)
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert_eq!(rows, 6);
    }

    #[tokio::test]
    async fn an_unreadable_header_is_treated_as_an_empty_segment() {
        let root = tempfile::tempdir().unwrap();
        let wal = Wal::open(root.path(), Signal::Logs, false).unwrap();

        let bogus = root
            .path()
            .join("logs")
            .join("00000000000000000000000000.wal");
        std::fs::write(&bogus, b"not an arrow stream").unwrap();

        assert!(wal.read_segment(&bogus).unwrap().is_empty());
    }
}
