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
pub async fn greet(request: GreetRequest) -> GreetResponse {
    GreetResponse {
        message: format!("Hello, {}!", request.name),
    }
}
