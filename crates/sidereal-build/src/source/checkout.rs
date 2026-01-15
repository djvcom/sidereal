//! Git checkout operations.

use std::path::{Path, PathBuf};
use std::time::Duration;

use gix::progress::Discard;
use tokio::task;
use tracing::{debug, info, instrument, warn};

use crate::error::{BuildError, BuildResult};
use crate::types::ProjectId;

/// A checked-out source directory ready for building.
#[derive(Debug)]
pub struct SourceCheckout {
    /// Path to the checkout directory.
    pub path: PathBuf,
    /// The commit SHA that was checked out.
    pub commit_sha: String,
    /// Path to Cargo.toml (if present).
    pub cargo_toml: Option<PathBuf>,
    /// Path to Cargo.lock (if present).
    pub cargo_lock: Option<PathBuf>,
}

impl SourceCheckout {
    /// Check if the checkout has a Cargo.lock file.
    #[must_use]
    pub fn has_lockfile(&self) -> bool {
        self.cargo_lock.is_some()
    }
}

/// Manages source checkouts for builds.
pub struct SourceManager {
    checkout_dir: PathBuf,
    cache_dir: PathBuf,
}

impl SourceManager {
    /// Create a new source manager.
    ///
    /// - `checkout_dir`: Directory for active checkouts (cleaned after build)
    /// - `cache_dir`: Directory for cached repositories (persisted)
    pub fn new(checkout_dir: impl Into<PathBuf>, cache_dir: impl Into<PathBuf>) -> Self {
        Self {
            checkout_dir: checkout_dir.into(),
            cache_dir: cache_dir.into(),
        }
    }

    /// Clone or fetch repository and checkout a specific commit.
    ///
    /// If the repository was previously cloned, it will be fetched to update.
    /// The specific commit is then checked out to a working directory.
    #[instrument(skip(self), fields(project = %project_id, commit = %commit))]
    pub async fn checkout(
        &self,
        project_id: &ProjectId,
        repo_url: &str,
        commit: &str,
    ) -> BuildResult<SourceCheckout> {
        // Validate inputs
        validate_commit_sha(commit)?;

        let repo_url = repo_url.to_owned();
        let commit = commit.to_owned();
        let project_id = project_id.clone();
        let checkout_dir = self.checkout_dir.clone();
        let cache_dir = self.cache_dir.clone();

        // Run blocking git operations in a separate thread
        task::spawn_blocking(move || {
            checkout_sync(&project_id, &repo_url, &commit, &checkout_dir, &cache_dir)
        })
        .await
        .map_err(|e| BuildError::Internal(format!("checkout task failed: {e}")))?
    }

    /// Clean up old checkouts.
    ///
    /// Returns the number of directories removed.
    #[instrument(skip(self))]
    pub async fn cleanup(&self, max_age: Duration) -> BuildResult<usize> {
        let checkout_dir = self.checkout_dir.clone();

        task::spawn_blocking(move || cleanup_old_checkouts(&checkout_dir, max_age))
            .await
            .map_err(|e| BuildError::Internal(format!("cleanup task failed: {e}")))?
    }
}

/// Validate that a commit SHA looks reasonable.
fn validate_commit_sha(commit: &str) -> BuildResult<()> {
    if commit.is_empty() {
        return Err(BuildError::GitCheckout {
            commit: commit.to_owned(),
            message: "commit SHA cannot be empty".to_owned(),
        });
    }

    // SHA should be hexadecimal
    if !commit.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(BuildError::GitCheckout {
            commit: commit.to_owned(),
            message: "commit SHA must be hexadecimal".to_owned(),
        });
    }

    // Full SHA is 40 chars, but we accept short SHAs too (minimum 7)
    if commit.len() < 7 {
        return Err(BuildError::GitCheckout {
            commit: commit.to_owned(),
            message: "commit SHA too short (minimum 7 characters)".to_owned(),
        });
    }

    Ok(())
}

/// Synchronous checkout implementation.
fn checkout_sync(
    project_id: &ProjectId,
    repo_url: &str,
    commit: &str,
    checkout_dir: &Path,
    cache_dir: &Path,
) -> BuildResult<SourceCheckout> {
    // Create directories if needed
    std::fs::create_dir_all(checkout_dir)?;
    std::fs::create_dir_all(cache_dir)?;

    // Cache path for bare repository
    let cache_path = cache_dir.join(sanitise_for_path(project_id.as_str()));

    // Clone or fetch the repository
    let repo = if cache_path.exists() {
        fetch_repository(&cache_path, repo_url)?
    } else {
        clone_repository(repo_url, &cache_path)?
    };

    // Create checkout directory for this build
    let work_dir = checkout_dir.join(format!(
        "{}-{}",
        sanitise_for_path(project_id.as_str()),
        &commit[..8.min(commit.len())]
    ));

    if work_dir.exists() {
        std::fs::remove_dir_all(&work_dir)?;
    }
    std::fs::create_dir_all(&work_dir)?;

    // Find the commit
    let commit_id = resolve_commit(&repo, commit)?;

    // Checkout the commit to the working directory
    checkout_tree(&repo, &commit_id, &work_dir)?;

    // Verify the checkout
    let cargo_toml = work_dir.join("Cargo.toml");
    let cargo_lock = work_dir.join("Cargo.lock");

    info!(
        path = %work_dir.display(),
        commit = %commit_id,
        "checkout complete"
    );

    Ok(SourceCheckout {
        path: work_dir,
        commit_sha: commit_id.to_string(),
        cargo_toml: cargo_toml.exists().then_some(cargo_toml),
        cargo_lock: cargo_lock.exists().then_some(cargo_lock),
    })
}

/// Clone a repository as a bare repository.
fn clone_repository(url: &str, path: &Path) -> BuildResult<gix::Repository> {
    info!(url = %url, path = %path.display(), "cloning repository");

    let mut prepare = gix::prepare_clone_bare(url, path)
        .map_err(|e| BuildError::GitClone {
            url: url.to_owned(),
            message: e.to_string(),
        })?
        .with_shallow(gix::remote::fetch::Shallow::DepthAtRemote(
            1.try_into().expect("valid depth"),
        ));

    let (repo, _outcome) = prepare
        .fetch_only(Discard, &gix::interrupt::IS_INTERRUPTED)
        .map_err(|e| BuildError::GitClone {
            url: url.to_owned(),
            message: e.to_string(),
        })?;

    Ok(repo)
}

/// Fetch updates to an existing repository.
fn fetch_repository(path: &Path, url: &str) -> BuildResult<gix::Repository> {
    debug!(path = %path.display(), "fetching updates");

    let repo = gix::open(path).map_err(|e| BuildError::GitFetch(e.to_string()))?;

    // Find or create remote
    let remote = repo
        .find_remote("origin")
        .or_else(|_| {
            // Remote doesn't exist, try to use the URL directly
            repo.remote_at(url)
        })
        .map_err(|e| BuildError::GitFetch(e.to_string()))?;

    // Fetch with shallow depth
    let _outcome = remote
        .connect(gix::remote::Direction::Fetch)
        .map_err(|e| BuildError::GitFetch(e.to_string()))?
        .prepare_fetch(Discard, Default::default())
        .map_err(|e| BuildError::GitFetch(e.to_string()))?
        .with_shallow(gix::remote::fetch::Shallow::DepthAtRemote(
            1.try_into().expect("valid depth"),
        ))
        .receive(Discard, &gix::interrupt::IS_INTERRUPTED)
        .map_err(|e| BuildError::GitFetch(e.to_string()))?;

    Ok(repo)
}

/// Resolve a commit reference to an object ID.
fn resolve_commit(repo: &gix::Repository, commit: &str) -> BuildResult<gix::ObjectId> {
    // Try to parse as full object ID first
    if commit.len() == 40 {
        if let Ok(id) = gix::ObjectId::from_hex(commit.as_bytes()) {
            if repo.find_object(id).is_ok() {
                return Ok(id);
            }
        }
    }

    // Try as a reference (branch name, tag)
    if let Ok(reference) = repo.find_reference(commit) {
        if let Ok(id) = reference.into_fully_peeled_id() {
            return Ok(id.detach());
        }
    }

    // Try HEAD if "HEAD" is specified
    if commit.eq_ignore_ascii_case("head") {
        if let Ok(head) = repo.head_id() {
            return Ok(head.detach());
        }
    }

    // For short SHAs, iterate through objects (simple but works)
    if commit.len() >= 7 && commit.len() < 40 && commit.chars().all(|c| c.is_ascii_hexdigit()) {
        let commit_lower = commit.to_lowercase();
        for oid in repo.objects.iter().map_err(|e| BuildError::GitCheckout {
            commit: commit.to_owned(),
            message: format!("failed to iterate objects: {e}"),
        })? {
            if let Ok(oid) = oid {
                let hex = oid.to_hex().to_string();
                if hex.starts_with(&commit_lower) {
                    return Ok(oid);
                }
            }
        }
    }

    Err(BuildError::GitCheckout {
        commit: commit.to_owned(),
        message: "could not resolve commit reference".to_owned(),
    })
}

/// Checkout a commit's tree to a working directory.
fn checkout_tree(
    repo: &gix::Repository,
    commit_id: &gix::ObjectId,
    work_dir: &Path,
) -> BuildResult<()> {
    let commit = repo
        .find_commit(*commit_id)
        .map_err(|e| BuildError::GitCheckout {
            commit: commit_id.to_string(),
            message: e.to_string(),
        })?;

    let tree = commit.tree().map_err(|e| BuildError::GitCheckout {
        commit: commit_id.to_string(),
        message: e.to_string(),
    })?;

    // Recursively extract the tree to the working directory
    extract_tree(repo, &tree, work_dir)?;

    Ok(())
}

/// Recursively extract a tree to a directory.
fn extract_tree(repo: &gix::Repository, tree: &gix::Tree<'_>, dest: &Path) -> BuildResult<()> {
    for entry in tree.iter() {
        let entry = entry.map_err(|e| BuildError::GitCheckout {
            commit: String::new(),
            message: format!("failed to read tree entry: {e}"),
        })?;

        let name = std::str::from_utf8(entry.filename()).map_err(|_| BuildError::GitCheckout {
            commit: String::new(),
            message: "invalid filename encoding".to_owned(),
        })?;

        // Validate filename for security
        if name.contains("..") || name.starts_with('/') || name.contains('\0') {
            return Err(BuildError::SymlinkEscape {
                path: dest.join(name),
            });
        }

        let entry_path = dest.join(name);

        match entry.mode().kind() {
            gix::object::tree::EntryKind::Tree => {
                std::fs::create_dir_all(&entry_path)?;
                let subtree = repo
                    .find_tree(entry.oid())
                    .map_err(|e| BuildError::GitCheckout {
                        commit: String::new(),
                        message: format!("failed to find subtree: {e}"),
                    })?;
                extract_tree(repo, &subtree, &entry_path)?;
            }
            gix::object::tree::EntryKind::Blob | gix::object::tree::EntryKind::BlobExecutable => {
                let object =
                    repo.find_object(entry.oid())
                        .map_err(|e| BuildError::GitCheckout {
                            commit: String::new(),
                            message: format!("failed to find blob: {e}"),
                        })?;
                std::fs::write(&entry_path, object.data.as_slice())?;

                // Set executable bit for executable blobs
                #[cfg(unix)]
                if matches!(
                    entry.mode().kind(),
                    gix::object::tree::EntryKind::BlobExecutable
                ) {
                    use std::os::unix::fs::PermissionsExt;
                    let mut perms = std::fs::metadata(&entry_path)?.permissions();
                    perms.set_mode(0o755);
                    std::fs::set_permissions(&entry_path, perms)?;
                }
            }
            gix::object::tree::EntryKind::Link => {
                // Symlinks are potential security risks - log and skip
                warn!(path = %entry_path.display(), "skipping symlink in repository");
            }
            gix::object::tree::EntryKind::Commit => {
                // Submodule - skip for now
                warn!(path = %entry_path.display(), "skipping submodule");
            }
        }
    }

    Ok(())
}

/// Clean up old checkout directories.
fn cleanup_old_checkouts(checkout_dir: &Path, max_age: Duration) -> BuildResult<usize> {
    if !checkout_dir.exists() {
        return Ok(0);
    }

    let now = std::time::SystemTime::now();
    let mut removed = 0;

    for entry in std::fs::read_dir(checkout_dir)? {
        let entry = entry?;
        let metadata = entry.metadata()?;

        if !metadata.is_dir() {
            continue;
        }

        if let Ok(modified) = metadata.modified() {
            if let Ok(age) = now.duration_since(modified) {
                if age > max_age {
                    debug!(path = %entry.path().display(), age = ?age, "removing old checkout");
                    if let Err(e) = std::fs::remove_dir_all(entry.path()) {
                        warn!(path = %entry.path().display(), error = %e, "failed to remove old checkout");
                    } else {
                        removed += 1;
                    }
                }
            }
        }
    }

    info!(removed = removed, "cleaned up old checkouts");
    Ok(removed)
}

/// Sanitise a string for use in a filesystem path.
fn sanitise_for_path(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_commit_sha_valid() {
        assert!(validate_commit_sha("abc123def456789").is_ok());
        assert!(validate_commit_sha("1234567").is_ok());
        assert!(validate_commit_sha("abcdef1234567890abcdef1234567890abcdef12").is_ok());
    }

    #[test]
    fn validate_commit_sha_invalid() {
        assert!(validate_commit_sha("").is_err());
        assert!(validate_commit_sha("abc").is_err()); // too short
        assert!(validate_commit_sha("xyz123").is_err()); // non-hex
        assert!(validate_commit_sha("abc123!").is_err()); // special char
    }

    #[test]
    fn sanitise_for_path_works() {
        assert_eq!(sanitise_for_path("my-project"), "my-project");
        assert_eq!(sanitise_for_path("my/project"), "my_project");
        assert_eq!(sanitise_for_path("my:project"), "my_project");
        assert_eq!(sanitise_for_path("alice/my-api"), "alice_my-api");
    }
}
