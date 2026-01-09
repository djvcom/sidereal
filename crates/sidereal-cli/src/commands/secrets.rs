//! CLI commands for secrets management.

use std::io::{self, BufRead, Write};
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use sidereal_secrets::{NativeSecrets, SecretContext, SecretScope, SecretValue, SecretsBackend};

fn default_db_path() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("sidereal")
        .join("secrets.db")
}

fn default_identity_path() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("sidereal")
        .join("identity.age")
}

fn parse_scope(scope: &str, project: Option<&str>, env: Option<&str>) -> Result<SecretScope> {
    match scope {
        "global" => Ok(SecretScope::global()),
        "project" => {
            let project = project.context("--project is required for project scope")?;
            Ok(SecretScope::project(project))
        }
        "env" | "environment" => {
            let project = project.context("--project is required for environment scope")?;
            let env = env.context("--env is required for environment scope")?;
            Ok(SecretScope::environment(project, env))
        }
        _ => bail!("Invalid scope: {scope}. Use global, project, or env"),
    }
}

fn build_context(project: Option<&str>, env: Option<&str>) -> SecretContext {
    let mut ctx = SecretContext::new();
    if let Some(p) = project {
        ctx = ctx.with_project(p);
    }
    if let Some(e) = env {
        ctx = ctx.with_environment(e);
    }
    ctx
}

async fn get_backend() -> Result<NativeSecrets> {
    let db_path = default_db_path();
    let identity_path = default_identity_path();

    NativeSecrets::new_or_create(&db_path, &identity_path)
        .await
        .context("failed to initialise secrets backend")
}

pub async fn get(name: &str, project: Option<&str>, env: Option<&str>) -> Result<()> {
    let backend = get_backend().await?;
    let ctx = build_context(project, env);

    match backend.get(name, &ctx).await? {
        Some(value) => {
            println!("{}", value.expose());
            Ok(())
        }
        None => {
            bail!("Secret '{name}' not found");
        }
    }
}

pub async fn set(
    name: &str,
    value: Option<&str>,
    scope: &str,
    project: Option<&str>,
    env: Option<&str>,
) -> Result<()> {
    let backend = get_backend().await?;
    let scope = parse_scope(scope, project, env)?;

    let secret_value = match value {
        Some(v) => {
            eprintln!(
                "Warning: passing secrets as command-line arguments is insecure \
                (visible in process listings). Consider using stdin instead."
            );
            v.to_string()
        }
        None => {
            eprint!("Enter secret value: ");
            io::stderr().flush()?;

            let mut line = String::new();
            io::stdin().lock().read_line(&mut line)?;
            line.trim_end().to_string()
        }
    };

    if secret_value.is_empty() {
        bail!("Secret value cannot be empty");
    }

    let version = backend
        .set(name, SecretValue::new(secret_value), &scope)
        .await?;

    println!("Secret '{name}' stored at {scope} (version: {version})");
    Ok(())
}

pub async fn delete(
    name: &str,
    scope: &str,
    project: Option<&str>,
    env: Option<&str>,
) -> Result<()> {
    let backend = get_backend().await?;
    let scope = parse_scope(scope, project, env)?;

    let deleted = backend.delete(name, &scope).await?;

    if deleted {
        println!("Secret '{name}' deleted from {scope}");
    } else {
        println!("Secret '{name}' not found at {scope}");
    }
    Ok(())
}

pub async fn list(
    prefix: Option<&str>,
    scope: &str,
    project: Option<&str>,
    env: Option<&str>,
) -> Result<()> {
    let backend = get_backend().await?;
    let scope = parse_scope(scope, project, env)?;
    let prefix = prefix.unwrap_or("");

    let mut cursor = None;
    let mut total = 0;

    loop {
        let (names, next_cursor) = backend.list(prefix, &scope, 100, cursor.as_deref()).await?;

        for name in &names {
            println!("{name}");
            total += 1;
        }

        cursor = next_cursor;
        if cursor.is_none() {
            break;
        }
    }

    if total == 0 {
        eprintln!("No secrets found");
    }

    Ok(())
}

pub async fn versions(
    name: &str,
    scope: &str,
    project: Option<&str>,
    env: Option<&str>,
) -> Result<()> {
    let backend = get_backend().await?;
    let scope = parse_scope(scope, project, env)?;

    let versions = backend.versions(name, &scope, 50).await?;

    if versions.is_empty() {
        bail!("Secret '{name}' not found at {scope}");
    }

    println!("Versions for '{name}' at {scope}:");
    for (i, version) in versions.iter().enumerate() {
        let marker = if i == 0 { " (current)" } else { "" };
        println!("  {version}{marker}");
    }

    Ok(())
}

pub async fn info() -> Result<()> {
    let db_path = default_db_path();
    let identity_path = default_identity_path();

    println!("Secrets configuration:");
    println!("  Database: {}", db_path.display());
    println!("  Identity: {}", identity_path.display());
    println!();

    if identity_path.exists() {
        println!("  Status: Initialised");
    } else {
        println!("  Status: Not initialised (run any secrets command to initialise)");
    }

    Ok(())
}
