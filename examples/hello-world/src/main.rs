//! Hello World example for Sidereal.

use sidereal_sdk::prelude::*;
use tracing::info;

#[derive(Serialize, Deserialize)]
pub struct GreetRequest {
    pub name: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct GreetResponse {
    pub message: String,
}

#[derive(Deserialize, Default)]
pub struct GreetingConfig {
    #[serde(default = "default_name")]
    pub default_name: String,
    #[serde(default = "default_max_length")]
    pub max_length: usize,
}

fn default_name() -> String {
    "World".to_string()
}

fn default_max_length() -> usize {
    100
}

#[sidereal_sdk::function]
pub async fn greet(req: HttpRequest<GreetRequest>) -> HttpResponse<GreetResponse> {
    // For a more complete example with extractors:
    // Config(greeting_config): Config<GreetingConfig>,

    let greeting_config = GreetingConfig::default();

    let name = req
        .body
        .name
        .as_deref()
        .unwrap_or(&greeting_config.default_name);
    let truncated_name = if name.len() > greeting_config.max_length {
        &name[..greeting_config.max_length]
    } else {
        name
    };

    info!(name = %truncated_name, "Processing greet request");

    HttpResponse::ok(GreetResponse {
        message: format!("Hello, {}!", truncated_name),
    })
}

#[tokio::main]
async fn main() {
    let config = sidereal_sdk::ServerConfig::default();
    sidereal_sdk::run(config).await;
}
