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

[lib]
crate-type = ["cdylib"]

[dependencies]
sidereal-sdk = "0.1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
"#;

const LIB_RS_TEMPLATE: &str = r#"use sidereal_sdk::prelude::*;

#[derive(Serialize, Deserialize)]
pub struct GreetRequest {
    pub name: String,
}

#[derive(Serialize, Deserialize)]
pub struct GreetResponse {
    pub message: String,
}

#[sidereal_sdk::function]
pub async fn greet(request: GreetRequest) -> GreetResponse {
    GreetResponse {
        message: format!("Hello, {}!", request.name),
    }
}
"#;

const SIDEREAL_TOML_TEMPLATE: &str = r#"[project]
name = "{{name}}"
version = "0.1.0"

[build]
target = "wasm32-wasip1"

[dev]
port = 3000
"#;

/// Validate a project name is a valid Rust crate name.
fn validate_name(name: &str) -> Result<(), InitError> {
    if name.is_empty() {
        return Err(InitError::InvalidName(
            "name cannot be empty".to_string(),
        ));
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

    // Write src/lib.rs
    fs::write(
        project_dir.join("src/lib.rs"),
        render_template(LIB_RS_TEMPLATE, name),
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
    println!("  sidereal dev");

    Ok(())
}
