//! Sidereal CLI - build and run Sidereal applications.

mod commands;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "sidereal")]
#[command(about = "Build and run Sidereal applications")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a new Sidereal project
    Init {
        /// Project name
        name: String,

        /// Output directory (defaults to project name)
        #[arg(short, long)]
        path: Option<PathBuf>,
    },

    /// Run the local development server
    Dev {
        /// Port to listen on (overrides sidereal.toml)
        #[arg(short, long)]
        port: Option<u16>,
    },

    /// Deploy to local Firecracker VM or remote environment
    Deploy {
        /// Deploy to local Firecracker VM
        #[arg(long, default_value = "true")]
        local: bool,

        /// Keep VM running after deployment (for debugging)
        #[arg(long)]
        keep_alive: bool,

        /// Skip building the project
        #[arg(long)]
        skip_build: bool,
    },

    /// Manage secrets
    Secrets {
        #[command(subcommand)]
        command: SecretsCommands,
    },
}

#[derive(Subcommand)]
enum SecretsCommands {
    /// Get a secret value
    Get {
        /// Secret name
        name: String,

        /// Project scope
        #[arg(long)]
        project: Option<String>,

        /// Environment scope
        #[arg(long)]
        env: Option<String>,
    },

    /// Set a secret value
    Set {
        /// Secret name
        name: String,

        /// Secret value (reads from stdin if not provided)
        value: Option<String>,

        /// Scope to store the secret (global, project, or env)
        #[arg(long, default_value = "global")]
        scope: String,

        /// Project for project/env scope
        #[arg(long)]
        project: Option<String>,

        /// Environment for env scope
        #[arg(long)]
        env: Option<String>,
    },

    /// Delete a secret
    Delete {
        /// Secret name
        name: String,

        /// Scope to delete from (global, project, or env)
        #[arg(long, default_value = "global")]
        scope: String,

        /// Project for project/env scope
        #[arg(long)]
        project: Option<String>,

        /// Environment for env scope
        #[arg(long)]
        env: Option<String>,
    },

    /// List secrets
    List {
        /// Prefix to filter by
        prefix: Option<String>,

        /// Scope to list from (global, project, or env)
        #[arg(long, default_value = "global")]
        scope: String,

        /// Project for project/env scope
        #[arg(long)]
        project: Option<String>,

        /// Environment for env scope
        #[arg(long)]
        env: Option<String>,
    },

    /// Show version history for a secret
    Versions {
        /// Secret name
        name: String,

        /// Scope (global, project, or env)
        #[arg(long, default_value = "global")]
        scope: String,

        /// Project for project/env scope
        #[arg(long)]
        project: Option<String>,

        /// Environment for env scope
        #[arg(long)]
        env: Option<String>,
    },

    /// Show secrets configuration info
    Info,
}

#[tokio::main]
async fn main() {
    let _otel_guard = opentelemetry_configuration::OtelSdkBuilder::new()
        .with_standard_env()
        .service_name(env!("CARGO_PKG_NAME"))
        .build()
        .ok();

    let cli = Cli::parse();

    let result: Result<(), anyhow::Error> = match cli.command {
        Commands::Init { name, path } => commands::init::run(&name, path).map_err(Into::into),
        Commands::Dev { port } => commands::dev::run(port).map_err(Into::into),
        Commands::Deploy {
            local,
            keep_alive,
            skip_build,
        } => {
            let args = commands::deploy::DeployArgs {
                local,
                keep_alive,
                skip_build,
            };
            commands::deploy::run(args).await.map_err(Into::into)
        }
        Commands::Secrets { command } => match command {
            SecretsCommands::Get { name, project, env } => {
                commands::secrets::get(&name, project.as_deref(), env.as_deref()).await
            }
            SecretsCommands::Set {
                name,
                value,
                scope,
                project,
                env,
            } => {
                commands::secrets::set(
                    &name,
                    value.as_deref(),
                    &scope,
                    project.as_deref(),
                    env.as_deref(),
                )
                .await
            }
            SecretsCommands::Delete {
                name,
                scope,
                project,
                env,
            } => commands::secrets::delete(&name, &scope, project.as_deref(), env.as_deref()).await,
            SecretsCommands::List {
                prefix,
                scope,
                project,
                env,
            } => {
                commands::secrets::list(
                    prefix.as_deref(),
                    &scope,
                    project.as_deref(),
                    env.as_deref(),
                )
                .await
            }
            SecretsCommands::Versions {
                name,
                scope,
                project,
                env,
            } => {
                commands::secrets::versions(&name, &scope, project.as_deref(), env.as_deref()).await
            }
            SecretsCommands::Info => commands::secrets::info().await,
        },
    };

    if let Err(e) = result {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
