//! Build queue with cancellation support.
//!
//! The queue manages pending builds and ensures that only one build runs per
//! project/branch combination at a time. When a new build arrives for a branch
//! that already has a build in progress, the old build is cancelled.

use std::collections::VecDeque;
use std::sync::{Arc, RwLock as StdRwLock};

use dashmap::DashMap;
use tokio::sync::{Notify, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::error::{BuildError, BuildResult, CancelReason};
use crate::types::{BuildId, BuildRequest, BuildStatus, ProjectId};

/// Key for tracking builds per project/branch.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BranchKey {
    project_id: ProjectId,
    branch: String,
}

impl BranchKey {
    fn new(project_id: &ProjectId, branch: &str) -> Self {
        Self {
            project_id: project_id.clone(),
            branch: branch.to_owned(),
        }
    }
}

/// Handle for a build that allows cancellation and status updates.
#[derive(Debug)]
pub struct BuildHandle {
    /// The build identifier.
    pub id: BuildId,
    /// Token to signal or check for cancellation.
    pub token: CancellationToken,
}

impl BuildHandle {
    fn new(id: BuildId) -> Self {
        Self {
            id,
            token: CancellationToken::new(),
        }
    }
}

/// Build queue managing pending and in-progress builds.
pub struct BuildQueue {
    pending: RwLock<VecDeque<BuildRequest>>,
    in_progress: DashMap<BuildId, Arc<BuildHandle>>,
    branch_builds: DashMap<BranchKey, BuildId>,
    statuses: DashMap<BuildId, BuildStatus>,
    logs: DashMap<BuildId, StdRwLock<Vec<String>>>,
    max_queue_size: usize,
    notify: Notify,
}

impl BuildQueue {
    /// Create a new build queue with the specified maximum size.
    #[must_use]
    pub fn new(max_queue_size: usize) -> Self {
        Self {
            pending: RwLock::new(VecDeque::new()),
            in_progress: DashMap::new(),
            branch_builds: DashMap::new(),
            statuses: DashMap::new(),
            logs: DashMap::new(),
            max_queue_size,
            notify: Notify::new(),
        }
    }

    /// Submit a build request to the queue.
    ///
    /// If a build is already in progress for the same project/branch, it will
    /// be cancelled with [`CancelReason::SupersededByCommit`].
    ///
    /// Returns the build ID on success.
    pub async fn submit(&self, request: BuildRequest) -> BuildResult<BuildId> {
        let branch_key = BranchKey::new(&request.project_id, &request.branch);
        let build_id = request.id.clone();

        // Check if queue is full
        {
            let pending = self.pending.read().await;
            if pending.len() >= self.max_queue_size {
                return Err(BuildError::QueueFull);
            }
        }

        // Cancel any in-progress build for the same branch
        // Note: We remove rather than get to avoid holding a DashMap read lock
        // while cancel_internal tries to acquire write locks (deadlock prevention)
        if let Some((_, existing_id)) = self.branch_builds.remove(&branch_key) {
            self.cancel_internal(
                &existing_id,
                CancelReason::SupersededByCommit {
                    new_commit: request.commit_sha.clone(),
                },
            )
            .await;
        }

        // Add to queue
        {
            let mut pending = self.pending.write().await;
            self.statuses.insert(build_id.clone(), BuildStatus::Queued);
            pending.push_back(request);
        }

        // Notify workers
        self.notify.notify_one();

        info!(build_id = %build_id, "build queued");
        Ok(build_id)
    }

    /// Cancel a build with the given reason.
    pub async fn cancel(&self, id: &BuildId, reason: CancelReason) -> BuildResult<()> {
        // Check if build exists
        if !self.statuses.contains_key(id) {
            return Err(BuildError::BuildNotFound(id.to_string()));
        }

        // Check if already completed
        if let Some(status) = self.statuses.get(id) {
            if status.is_terminal() {
                return Err(BuildError::BuildCompleted(id.to_string()));
            }
        }

        self.cancel_internal(id, reason).await;
        Ok(())
    }

    /// Internal cancellation without error checking.
    async fn cancel_internal(&self, id: &BuildId, reason: CancelReason) {
        // Trigger cancellation token if in progress
        if let Some(handle) = self.in_progress.get(id) {
            info!(build_id = %id, reason = %reason, "cancelling in-progress build");
            handle.token.cancel();
        }

        // Remove from pending queue if present
        {
            let mut pending = self.pending.write().await;
            pending.retain(|req| &req.id != id);
        }

        // Update status
        self.statuses
            .insert(id.clone(), BuildStatus::Cancelled { reason });

        // Clean up tracking
        self.in_progress.remove(id);

        // Clean up branch mapping
        self.branch_builds.retain(|_, build_id| build_id != id);
    }

    /// Get the next build request from the queue.
    ///
    /// This is called by build workers. The returned handle includes a
    /// cancellation token that should be checked periodically during the build.
    pub async fn next(&self) -> Option<(BuildRequest, Arc<BuildHandle>)> {
        loop {
            // Try to get from queue
            {
                let mut pending = self.pending.write().await;
                if let Some(request) = pending.pop_front() {
                    let handle = Arc::new(BuildHandle::new(request.id.clone()));

                    // Track as in-progress
                    self.in_progress
                        .insert(request.id.clone(), Arc::clone(&handle));
                    self.branch_builds.insert(
                        BranchKey::new(&request.project_id, &request.branch),
                        request.id.clone(),
                    );

                    debug!(build_id = %request.id, "build dequeued");
                    return Some((request, handle));
                }
            }

            // Wait for notification
            self.notify.notified().await;
        }
    }

    /// Try to get the next build request without blocking.
    ///
    /// Returns `None` if the queue is empty.
    pub async fn try_next(&self) -> Option<(BuildRequest, Arc<BuildHandle>)> {
        let mut pending = self.pending.write().await;
        if let Some(request) = pending.pop_front() {
            let handle = Arc::new(BuildHandle::new(request.id.clone()));

            self.in_progress
                .insert(request.id.clone(), Arc::clone(&handle));
            self.branch_builds.insert(
                BranchKey::new(&request.project_id, &request.branch),
                request.id.clone(),
            );

            debug!(build_id = %request.id, "build dequeued");
            Some((request, handle))
        } else {
            None
        }
    }

    /// Update the status of a build.
    pub fn update_status(&self, id: &BuildId, status: BuildStatus) {
        let is_terminal = status.is_terminal();
        self.statuses.insert(id.clone(), status);

        if is_terminal {
            self.in_progress.remove(id);
            self.branch_builds.retain(|_, build_id| build_id != id);
        }
    }

    /// Get the current status of a build.
    #[must_use]
    pub fn status(&self, id: &BuildId) -> Option<BuildStatus> {
        self.statuses.get(id).map(|s| s.clone())
    }

    /// Get the number of pending builds.
    pub async fn pending_count(&self) -> usize {
        self.pending.read().await.len()
    }

    /// Get the number of in-progress builds.
    #[must_use]
    pub fn in_progress_count(&self) -> usize {
        self.in_progress.len()
    }

    /// Check if a specific build is in progress.
    #[must_use]
    pub fn is_in_progress(&self, id: &BuildId) -> bool {
        self.in_progress.contains_key(id)
    }

    /// Get the cancellation token for a build (if in progress).
    #[must_use]
    pub fn cancellation_token(&self, id: &BuildId) -> Option<CancellationToken> {
        self.in_progress.get(id).map(|h| h.token.clone())
    }

    /// Cancel all pending and in-progress builds (for shutdown).
    pub async fn cancel_all(&self, reason: CancelReason) {
        warn!("cancelling all builds: {}", reason);

        // Cancel all in-progress builds
        let in_progress_ids: Vec<BuildId> =
            self.in_progress.iter().map(|r| r.key().clone()).collect();

        for id in in_progress_ids {
            if let Some(handle) = self.in_progress.get(&id) {
                handle.token.cancel();
            }
            self.statuses.insert(
                id.clone(),
                BuildStatus::Cancelled {
                    reason: reason.clone(),
                },
            );
        }

        // Clear pending queue
        {
            let mut pending = self.pending.write().await;
            for request in pending.drain(..) {
                self.statuses.insert(
                    request.id.clone(),
                    BuildStatus::Cancelled {
                        reason: reason.clone(),
                    },
                );
            }
        }

        self.in_progress.clear();
        self.branch_builds.clear();
    }

    /// Append a log line for a build.
    pub fn append_log(&self, id: &BuildId, line: String) {
        self.logs
            .entry(id.clone())
            .or_insert_with(|| StdRwLock::new(Vec::new()))
            .write()
            .expect("log lock poisoned")
            .push(line);
    }

    /// Get logs for a build, optionally starting from an offset.
    ///
    /// Returns the logs and the next offset to use for subsequent calls.
    #[must_use]
    pub fn logs(&self, id: &BuildId, offset: usize) -> Option<(Vec<String>, usize)> {
        self.logs.get(id).map(|logs| {
            let logs = logs.read().expect("log lock poisoned");
            let new_logs: Vec<String> = logs.iter().skip(offset).cloned().collect();
            let next_offset = logs.len();
            (new_logs, next_offset)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_request(project: &str, branch: &str, commit: &str) -> BuildRequest {
        BuildRequest::new(
            project,
            "https://github.com/example/repo.git",
            branch,
            commit,
        )
    }

    #[tokio::test]
    async fn submit_and_retrieve() {
        let queue = BuildQueue::new(10);
        let request = make_request("proj1", "main", "abc123");
        let id = request.id.clone();

        queue.submit(request).await.expect("submit should succeed");

        assert_eq!(queue.pending_count().await, 1);
        assert!(matches!(queue.status(&id), Some(BuildStatus::Queued)));

        let (retrieved, _handle) = queue.try_next().await.expect("should get build");
        assert_eq!(retrieved.id, id);
        assert_eq!(queue.pending_count().await, 0);
        assert_eq!(queue.in_progress_count(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn supersedes_existing_build() {
        let queue = BuildQueue::new(10);

        let req1 = make_request("proj1", "main", "commit1");
        let id1 = req1.id.clone();
        queue.submit(req1).await.expect("submit 1");

        // Start processing build 1
        let (_build1, handle1) = queue.try_next().await.expect("get build 1");

        // Submit new build for same branch
        let req2 = make_request("proj1", "main", "commit2");
        let id2 = req2.id.clone();
        queue.submit(req2).await.expect("submit 2");

        // Build 1 should be cancelled
        assert!(handle1.token.is_cancelled());
        assert!(matches!(
            queue.status(&id1),
            Some(BuildStatus::Cancelled {
                reason: CancelReason::SupersededByCommit { .. }
            })
        ));

        // Build 2 should be queued
        assert!(matches!(queue.status(&id2), Some(BuildStatus::Queued)));
    }

    #[tokio::test]
    async fn different_branches_not_cancelled() {
        let queue = BuildQueue::new(10);

        let req1 = make_request("proj1", "main", "commit1");
        let id1 = req1.id.clone();
        queue.submit(req1).await.expect("submit 1");

        let (_build1, handle1) = queue.try_next().await.expect("get build 1");

        // Submit build for different branch
        let req2 = make_request("proj1", "feature", "commit2");
        queue.submit(req2).await.expect("submit 2");

        // Build 1 should NOT be cancelled
        assert!(!handle1.token.is_cancelled());
        assert!(queue.is_in_progress(&id1));
    }

    #[tokio::test]
    async fn queue_full() {
        let queue = BuildQueue::new(2);

        queue
            .submit(make_request("p1", "b1", "c1"))
            .await
            .expect("submit 1");
        queue
            .submit(make_request("p2", "b2", "c2"))
            .await
            .expect("submit 2");

        let result = queue.submit(make_request("p3", "b3", "c3")).await;
        assert!(matches!(result, Err(BuildError::QueueFull)));
    }

    #[tokio::test]
    async fn cancel_not_found() {
        let queue = BuildQueue::new(10);
        let result = queue
            .cancel(&BuildId::new("nonexistent"), CancelReason::UserRequested)
            .await;
        assert!(matches!(result, Err(BuildError::BuildNotFound(_))));
    }

    #[tokio::test]
    async fn cancel_already_completed() {
        let queue = BuildQueue::new(10);
        let request = make_request("proj1", "main", "abc123");
        let id = request.id.clone();

        queue.submit(request).await.expect("submit");
        let (_build, _handle) = queue.try_next().await.expect("get build");

        // Mark as completed
        queue.update_status(
            &id,
            BuildStatus::Completed {
                artifact_id: crate::types::ArtifactId::generate(),
            },
        );

        let result = queue.cancel(&id, CancelReason::UserRequested).await;
        assert!(matches!(result, Err(BuildError::BuildCompleted(_))));
    }

    #[tokio::test]
    async fn update_status() {
        let queue = BuildQueue::new(10);
        let request = make_request("proj1", "main", "abc123");
        let id = request.id.clone();

        queue.submit(request).await.expect("submit");
        let (_build, _handle) = queue.try_next().await.expect("get build");

        queue.update_status(&id, BuildStatus::CheckingOut);
        assert!(matches!(queue.status(&id), Some(BuildStatus::CheckingOut)));

        queue.update_status(&id, BuildStatus::Compiling { progress: None });
        assert!(matches!(
            queue.status(&id),
            Some(BuildStatus::Compiling { .. })
        ));

        // Terminal status should clean up
        queue.update_status(
            &id,
            BuildStatus::Completed {
                artifact_id: crate::types::ArtifactId::generate(),
            },
        );
        assert!(!queue.is_in_progress(&id));
    }

    #[tokio::test]
    async fn cancel_all() {
        let queue = BuildQueue::new(10);

        let req1 = make_request("proj1", "main", "c1");
        let id1 = req1.id.clone();
        queue.submit(req1).await.expect("submit 1");

        let req2 = make_request("proj2", "main", "c2");
        let id2 = req2.id.clone();
        queue.submit(req2).await.expect("submit 2");

        // Start one build
        let (_build, handle) = queue.try_next().await.expect("get build");

        queue.cancel_all(CancelReason::Shutdown).await;

        assert!(handle.token.is_cancelled());
        assert!(matches!(
            queue.status(&id1),
            Some(BuildStatus::Cancelled {
                reason: CancelReason::Shutdown
            })
        ));
        assert!(matches!(
            queue.status(&id2),
            Some(BuildStatus::Cancelled {
                reason: CancelReason::Shutdown
            })
        ));
        assert_eq!(queue.pending_count().await, 0);
        assert_eq!(queue.in_progress_count(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_submits() {
        let queue = Arc::new(BuildQueue::new(100));

        let handles: Vec<_> = (0..10)
            .map(|i| {
                let queue = Arc::clone(&queue);
                tokio::spawn(async move {
                    let req = make_request(&format!("proj{i}"), "main", &format!("commit{i}"));
                    queue.submit(req).await
                })
            })
            .collect();

        for handle in handles {
            handle.await.expect("task").expect("submit");
        }

        assert_eq!(queue.pending_count().await, 10);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn blocking_next_receives_notification() {
        let queue = Arc::new(BuildQueue::new(10));
        let queue_clone = Arc::clone(&queue);

        // Spawn a task that waits for a build
        let waiter = tokio::spawn(async move { queue_clone.next().await });

        // Give the waiter time to start waiting
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Submit a build
        let req = make_request("proj1", "main", "abc123");
        let id = req.id.clone();
        queue.submit(req).await.expect("submit");

        // The waiter should receive the build
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), waiter).await;

        let (build, _handle) = result
            .expect("timeout")
            .expect("task")
            .expect("should get build");
        assert_eq!(build.id, id);
    }
}
