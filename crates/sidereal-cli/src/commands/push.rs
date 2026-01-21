//! Push command - deploy a project to the Sidereal platform.

use serde::{Deserialize, Serialize};
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PushError {
    #[error("Configuration not found: sidereal.toml")]
    ConfigNotFound,

    #[error("Not a git repository")]
    NotGitRepo,

    #[error("No git remote configured")]
    NoGitRemote,

    #[error("Failed to get git info: {0}")]
    GitError(String),

    #[error("Build failed: {0}")]
    BuildFailed(String),

    #[error("Deployment failed: {0}")]
    DeploymentFailed(String),

    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub struct PushArgs {
    pub url: String,
    pub environment: String,
}

#[derive(Debug, Deserialize)]
struct SiderealConfig {
    project: ProjectConfig,
}

#[derive(Debug, Deserialize)]
struct ProjectConfig {
    name: String,
}

#[derive(Debug, Serialize)]
struct SubmitBuildRequest {
    project_id: String,
    repo_url: String,
    branch: String,
    commit_sha: String,
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

#[derive(Debug, Serialize)]
struct CreateDeploymentRequest {
    project_id: String,
    environment: String,
    commit_sha: String,
    artifact_url: String,
    functions: Vec<FunctionMetadata>,
}

#[derive(Debug, Serialize)]
struct FunctionMetadata {
    name: String,
    handler: String,
}

#[derive(Debug, Deserialize)]
struct CreateDeploymentResponse {
    id: String,
    #[serde(rename = "state")]
    _state: String,
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    error: String,
}

struct GitInfo {
    remote_url: String,
    branch: String,
    commit_sha: String,
}

pub async fn run(args: PushArgs) -> Result<(), PushError> {
    let config = load_config()?;
    let git_info = get_git_info()?;

    println!("Pushing {} to {}", config.project.name, args.url);
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
    let build_id = submit_build(&client, &args.url, &config, &git_info, &args.environment).await?;
    println!("  Build ID: {build_id}");

    // Poll for build completion
    println!("\nBuilding...");
    let artifact_id = poll_build_status(&client, &args.url, &build_id).await?;
    println!("  Artifact: {artifact_id}");

    // Create deployment
    println!("\nDeploying...");
    let deployment_id = create_deployment(
        &client,
        &args.url,
        &config,
        &git_info,
        &args.environment,
        &artifact_id,
    )
    .await?;
    println!("  Deployment ID: {deployment_id}");

    println!("\nDeployment initiated successfully!");
    println!(
        "Function URL: {}/functions/{}",
        args.url, config.project.name
    );

    Ok(())
}

fn load_config() -> Result<SiderealConfig, PushError> {
    let config_path = Path::new("sidereal.toml");
    if !config_path.exists() {
        return Err(PushError::ConfigNotFound);
    }

    let content = std::fs::read_to_string(config_path)?;
    toml::from_str(&content).map_err(|e| {
        PushError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            e.to_string(),
        ))
    })
}

fn get_git_info() -> Result<GitInfo, PushError> {
    if !Path::new(".git").exists() {
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

    Ok(GitInfo {
        remote_url,
        branch,
        commit_sha,
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
    config: &SiderealConfig,
    git_info: &GitInfo,
    environment: &str,
) -> Result<String, PushError> {
    let request = SubmitBuildRequest {
        project_id: config.project.name.clone(),
        repo_url: git_info.remote_url.clone(),
        branch: git_info.branch.clone(),
        commit_sha: git_info.commit_sha.clone(),
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

async fn create_deployment(
    client: &reqwest::Client,
    base_url: &str,
    config: &SiderealConfig,
    git_info: &GitInfo,
    environment: &str,
    artifact_id: &str,
) -> Result<String, PushError> {
    let request = CreateDeploymentRequest {
        project_id: config.project.name.clone(),
        environment: environment.to_owned(),
        commit_sha: git_info.commit_sha.clone(),
        artifact_url: format!("artifact://{artifact_id}"),
        functions: vec![FunctionMetadata {
            name: config.project.name.clone(),
            handler: "handler".to_owned(),
        }],
    };

    let response = client
        .post(format!("{base_url}/api/deployments"))
        .json(&request)
        .send()
        .await?;

    if !response.status().is_success() {
        let error: ErrorResponse = response.json().await.map_err(|_| {
            PushError::DeploymentFailed("Failed to parse error response".to_owned())
        })?;
        return Err(PushError::DeploymentFailed(error.error));
    }

    let result: CreateDeploymentResponse = response.json().await?;
    Ok(result.id)
}
