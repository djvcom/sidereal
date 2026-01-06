//! Implementation of the `sidereal init` command.

use std::fs;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum InitError {
    #[error("Directory already exists: {0}")]
    DirectoryExists(PathBuf),

    #[error("Invalid project name: {0}")]
    InvalidName(String),

    #[error("Failed to create directory: {0}")]
    CreateDir(#[from] std::io::Error),
}

const CARGO_TOML_TEMPLATE: &str = r#"[package]
name = "{{name}}"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "{{name}}"
path = "src/main.rs"

[dependencies]
sidereal-sdk = "0.1"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["full"] }
"#;

const MAIN_RS_TEMPLATE: &str = r#"use sidereal_sdk::prelude::*;
use std::time::Duration;

// HTTP trigger types
#[derive(Serialize, Deserialize)]
pub struct GreetRequest {
    pub name: String,
}

#[derive(Serialize, Deserialize)]
pub struct GreetResponse {
    pub message: String,
}

// Queue message type (queue name derived: "greeting-event")
#[derive(Serialize, Deserialize)]
pub struct GreetingEvent {
    pub name: String,
    pub greeting: String,
}

/// HTTP-triggered function (POST /greet)
#[sidereal_sdk::function]
pub async fn greet(
    req: HttpRequest<GreetRequest>,
    ctx: Context,
) -> HttpResponse<GreetResponse> {
    ctx.log().info("Processing greet request", &[("name", &req.body.name)]);

    HttpResponse::ok(GreetResponse {
        message: format!("Hello, {}!", req.body.name),
    })
}

/// Queue-triggered function (consumes from "greeting-event" queue)
#[sidereal_sdk::function]
pub async fn process_greeting(
    msg: QueueMessage<GreetingEvent>,
    ctx: Context,
) -> Result<(), String> {
    ctx.log().info(
        "Processing greeting event",
        &[("name", &msg.body.name), ("greeting", &msg.body.greeting)],
    );

    // Process the event...

    Ok(())
}

/// Background service - runs continuously until shutdown
#[sidereal_sdk::service]
pub async fn heartbeat(ctx: Context, cancel: CancellationToken) -> Result<(), ServiceError> {
    ctx.log().info("Heartbeat service started", &[]);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                ctx.log().info("Heartbeat service stopping", &[]);
                break;
            }
            _ = tokio::time::sleep(Duration::from_secs(30)) => {
                ctx.log().info("Heartbeat", &[]);
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    let config = sidereal_sdk::ServerConfig::default();
    sidereal_sdk::run(config).await;
}
"#;

const SIDEREAL_TOML_TEMPLATE: &str = r#"[project]
name = "{{name}}"
version = "0.1.0"

[dev]
port = 7850

# Queue resource for the process_greeting function
[resources.queue.greeting-event]
retention = "7d"
dead_letter = true
"#;

/// Validate a project name is a valid Rust crate name.
fn validate_name(name: &str) -> Result<(), InitError> {
    if name.is_empty() {
        return Err(InitError::InvalidName("name cannot be empty".to_string()));
    }

    // Must start with a letter or underscore
    let first = name.chars().next().unwrap();
    if !first.is_ascii_alphabetic() && first != '_' {
        return Err(InitError::InvalidName(
            "name must start with a letter or underscore".to_string(),
        ));
    }

    // Must contain only alphanumeric, underscore, or hyphen
    for ch in name.chars() {
        if !ch.is_ascii_alphanumeric() && ch != '_' && ch != '-' {
            return Err(InitError::InvalidName(format!(
                "name contains invalid character: '{}'",
                ch
            )));
        }
    }

    Ok(())
}

fn render_template(template: &str, name: &str) -> String {
    template.replace("{{name}}", name)
}

pub async fn run(name: &str, path: Option<PathBuf>) -> Result<(), InitError> {
    validate_name(name)?;

    let project_dir = path.unwrap_or_else(|| PathBuf::from(name));

    if project_dir.exists() {
        return Err(InitError::DirectoryExists(project_dir));
    }

    // Create directory structure
    fs::create_dir_all(project_dir.join("src"))?;

    // Write Cargo.toml
    fs::write(
        project_dir.join("Cargo.toml"),
        render_template(CARGO_TOML_TEMPLATE, name),
    )?;

    // Write src/main.rs
    fs::write(
        project_dir.join("src/main.rs"),
        render_template(MAIN_RS_TEMPLATE, name),
    )?;

    // Write sidereal.toml
    fs::write(
        project_dir.join("sidereal.toml"),
        render_template(SIDEREAL_TOML_TEMPLATE, name),
    )?;

    println!("Created project '{}' at {}", name, project_dir.display());
    println!();
    println!("Next steps:");
    println!("  cd {}", project_dir.display());
    println!("  cargo run");
    println!();
    println!("Or for hot reload:");
    println!("  cargo install cargo-watch");
    println!("  cargo watch -x run");

    Ok(())
}
