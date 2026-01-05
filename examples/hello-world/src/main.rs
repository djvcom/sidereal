//! Hello World example for Sidereal.

use sidereal_sdk::prelude::*;

#[derive(Serialize, Deserialize)]
pub struct GreetRequest {
    pub name: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct GreetResponse {
    pub message: String,
}

#[derive(Deserialize)]
pub struct GreetingConfig {
    pub default_name: String,
    pub max_length: usize,
}

#[sidereal_sdk::function]
pub async fn greet(
    req: HttpRequest<GreetRequest>,
    ctx: Context,
) -> HttpResponse<GreetResponse> {
    let greeting_config: GreetingConfig = ctx.config("greeting").unwrap_or_else(|e| {
        ctx.log().warn("Using default greeting config", &[("reason", &e.to_string())]);
        GreetingConfig {
            default_name: "World".to_string(),
            max_length: 100,
        }
    });

    let name = req.body.name.as_deref().unwrap_or(&greeting_config.default_name);
    let truncated_name = if name.len() > greeting_config.max_length {
        &name[..greeting_config.max_length]
    } else {
        name
    };

    ctx.log().info("Processing greet request", &[("name", truncated_name)]);

    HttpResponse::ok(GreetResponse {
        message: format!("Hello, {}!", truncated_name),
    })
}

#[tokio::main]
async fn main() {
    let config = sidereal_sdk::ServerConfig::default();
    sidereal_sdk::run(config).await;
}
