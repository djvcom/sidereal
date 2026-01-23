//! Push command - deploy a project to the Sidereal platform.
//!
//! Submits a build request for a git repository. The build service clones the repo,
//! builds the workspace, discovers deployable projects by scanning for sidereal.toml
//! files, and creates artifacts for each.

use serde::{Deserialize, Serialize};
use std::process::Command;
use std::time::Duration;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PushError {
    #[error("Not a git repository")]
    NotGitRepo,

    #[error("No git remote configured")]
    NoGitRemote,

    #[error("Failed to get git info: {0}")]
    GitError(String),

    #[error("Build failed: {0}")]
    BuildFailed(String),

    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub struct PushArgs {
    pub url: String,
    pub environment: String,
}

#[derive(Debug, Serialize)]
struct SubmitBuildRequest {
    project_id: String,
    repo_url: String,
    branch: String,
    commit_sha: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    environment: Option<String>,
    callback_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SubmitBuildResponse {
    id: String,
    #[serde(rename = "status")]
    _status: String,
}

#[derive(Debug, Deserialize)]
struct BuildStatusResponse {
    #[serde(rename = "id")]
    _id: String,
    status: String,
    #[serde(default)]
    details: Option<String>,
    is_terminal: bool,
    #[serde(default)]
    artifact_id: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    error: String,
}

struct GitInfo {
    remote_url: String,
    branch: String,
    commit_sha: String,
    subdir: Option<String>,
}

impl GitInfo {
    /// Derive a project identifier from the git remote URL.
    ///
    /// Examples:
    /// - `git@github.com:user/repo.git` → `github.com/user/repo`
    /// - `https://github.com/user/repo.git` → `github.com/user/repo`
    fn project_id(&self) -> String {
        let url = &self.remote_url;

        // Handle SSH URLs: git@github.com:user/repo.git
        if let Some(rest) = url.strip_prefix("git@") {
            return rest.replace(':', "/").trim_end_matches(".git").to_owned();
        }

        // Handle HTTPS URLs: https://github.com/user/repo.git
        if let Some(rest) = url.strip_prefix("https://") {
            return rest.trim_end_matches(".git").to_owned();
        }

        if let Some(rest) = url.strip_prefix("http://") {
            return rest.trim_end_matches(".git").to_owned();
        }

        // Fallback: use URL as-is
        url.trim_end_matches(".git").to_owned()
    }
}

pub async fn run(args: PushArgs) -> Result<(), PushError> {
    let git_info = get_git_info()?;
    let project_id = git_info.project_id();

    println!("Pushing {} to {}", project_id, args.url);
    println!(
        "  Branch: {} @ {}",
        git_info.branch,
        &git_info.commit_sha[..8]
    );
    println!("  Environment: {}", args.environment);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(300))
        .build()?;

    // Submit build
    println!("\nSubmitting build...");
    let build_id = submit_build(
        &client,
        &args.url,
        &project_id,
        &git_info,
        &args.environment,
    )
    .await?;
    println!("  Build ID: {build_id}");

    // Poll for build completion
    println!("\nBuilding...");
    let artifact_id = poll_build_status(&client, &args.url, &build_id).await?;
    println!("  Artifact: {artifact_id}");

    println!("\nBuild completed successfully!");
    println!("  Artifact ID: {artifact_id}");

    Ok(())
}

fn get_git_info() -> Result<GitInfo, PushError> {
    let is_git_repo = run_git_command(&["rev-parse", "--is-inside-work-tree"])
        .map(|s| s.trim() == "true")
        .unwrap_or(false);

    if !is_git_repo {
        return Err(PushError::NotGitRepo);
    }

    let remote_url = run_git_command(&["config", "--get", "remote.origin.url"])?
        .trim()
        .to_owned();

    if remote_url.is_empty() {
        return Err(PushError::NoGitRemote);
    }

    let branch = run_git_command(&["rev-parse", "--abbrev-ref", "HEAD"])?
        .trim()
        .to_owned();

    let commit_sha = run_git_command(&["rev-parse", "HEAD"])?.trim().to_owned();

    let repo_root = run_git_command(&["rev-parse", "--show-toplevel"])?
        .trim()
        .to_owned();
    let current_dir = std::env::current_dir().map_err(|e| PushError::GitError(e.to_string()))?;
    let subdir = current_dir
        .strip_prefix(&repo_root)
        .ok()
        .filter(|p| !p.as_os_str().is_empty())
        .map(|p| p.to_string_lossy().to_string());

    Ok(GitInfo {
        remote_url,
        branch,
        commit_sha,
        subdir,
    })
}

fn run_git_command(args: &[&str]) -> Result<String, PushError> {
    let output = Command::new("git")
        .args(args)
        .output()
        .map_err(|e| PushError::GitError(e.to_string()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(PushError::GitError(stderr.to_string()));
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

async fn submit_build(
    client: &reqwest::Client,
    base_url: &str,
    project_id: &str,
    git_info: &GitInfo,
    environment: &str,
) -> Result<String, PushError> {
    let request = SubmitBuildRequest {
        project_id: project_id.to_owned(),
        repo_url: git_info.remote_url.clone(),
        branch: git_info.branch.clone(),
        commit_sha: git_info.commit_sha.clone(),
        path: git_info.subdir.clone(),
        environment: Some(environment.to_owned()),
        callback_url: None,
    };

    let response = client
        .post(format!("{base_url}/api/builds"))
        .json(&request)
        .send()
        .await?;

    if !response.status().is_success() {
        let error: ErrorResponse = response
            .json()
            .await
            .map_err(|_| PushError::BuildFailed("Failed to parse error response".to_owned()))?;
        return Err(PushError::BuildFailed(error.error));
    }

    let result: SubmitBuildResponse = response.json().await?;
    Ok(result.id)
}

async fn poll_build_status(
    client: &reqwest::Client,
    base_url: &str,
    build_id: &str,
) -> Result<String, PushError> {
    let mut last_status = String::new();

    loop {
        let response = client
            .get(format!("{base_url}/api/builds/{build_id}"))
            .send()
            .await?;

        if !response.status().is_success() {
            let error: ErrorResponse = response
                .json()
                .await
                .map_err(|_| PushError::BuildFailed("Failed to parse error response".to_owned()))?;
            return Err(PushError::BuildFailed(error.error));
        }

        let status: BuildStatusResponse = response.json().await?;

        if status.status != last_status {
            let detail = status.details.as_deref().unwrap_or("");
            if detail.is_empty() {
                println!("  Status: {}", status.status);
            } else {
                println!("  Status: {} - {}", status.status, detail);
            }
            last_status = status.status.clone();
        }

        if status.is_terminal {
            if let Some(artifact_id) = status.artifact_id {
                return Ok(artifact_id);
            }
            if let Some(error) = status.error {
                return Err(PushError::BuildFailed(error));
            }
            return Err(PushError::BuildFailed(format!(
                "Build ended with status: {}",
                status.status
            )));
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_id_from_ssh_url() {
        let info = GitInfo {
            remote_url: "git@github.com:djvcom/sidereal.git".to_owned(),
            branch: "main".to_owned(),
            commit_sha: "abc123".to_owned(),
            subdir: None,
        };
        assert_eq!(info.project_id(), "github.com/djvcom/sidereal");
    }

    #[test]
    fn project_id_from_https_url() {
        let info = GitInfo {
            remote_url: "https://github.com/djvcom/sidereal.git".to_owned(),
            branch: "main".to_owned(),
            commit_sha: "abc123".to_owned(),
            subdir: None,
        };
        assert_eq!(info.project_id(), "github.com/djvcom/sidereal");
    }

    #[test]
    fn project_id_from_https_url_no_git_suffix() {
        let info = GitInfo {
            remote_url: "https://github.com/djvcom/sidereal".to_owned(),
            branch: "main".to_owned(),
            commit_sha: "abc123".to_owned(),
            subdir: None,
        };
        assert_eq!(info.project_id(), "github.com/djvcom/sidereal");
    }
}
