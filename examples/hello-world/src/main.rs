//! Hello World example for Sidereal.

use sidereal_sdk::prelude::*;

#[derive(Serialize, Deserialize)]
pub struct GreetRequest {
    pub name: String,
}

#[derive(Serialize, Deserialize)]
pub struct GreetResponse {
    pub message: String,
}

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

#[tokio::main]
async fn main() {
    let config = sidereal_sdk::ServerConfig::default();
    sidereal_sdk::run(config).await;
}
