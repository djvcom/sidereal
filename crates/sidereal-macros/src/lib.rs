//! Procedural macros for the Sidereal SDK.
//!
//! This crate provides the `#[sidereal::function]` and `#[sidereal::service]`
//! attribute macros for defining deployable functions and services.

mod function;
mod service;

use proc_macro::TokenStream;

/// Marks a function as a Sidereal function.
///
/// The trigger type is determined by the first parameter:
/// - `HttpRequest<T>` - HTTP-triggered function
/// - `QueueMessage<T>` - Queue-triggered function
///
/// # Example
///
/// ```ignore
/// use sidereal_sdk::prelude::*;
///
/// #[sidereal_sdk::function]
/// async fn greet(req: HttpRequest<GreetRequest>, ctx: Context) -> HttpResponse<GreetResponse> {
///     HttpResponse::ok(GreetResponse { message: format!("Hello, {}!", req.body.name) })
/// }
/// ```
#[proc_macro_attribute]
pub fn function(attr: TokenStream, item: TokenStream) -> TokenStream {
    function::expand(attr.into(), item.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

/// Marks a function as a Sidereal service.
///
/// The service kind is determined by the return type:
/// - `Router` - Router service (mounted at a path prefix)
/// - `Result<(), E>` or `()` - Background service (spawned as a task)
///
/// # Attributes
///
/// - `path = "/prefix"` - Path prefix for router services (default: `/{service_name}`)
///
/// # Background Service Example
///
/// ```ignore
/// use sidereal_sdk::prelude::*;
/// use std::time::Duration;
///
/// #[sidereal_sdk::service]
/// async fn metrics_collector(ctx: Context, cancel: CancellationToken) -> Result<(), Error> {
///     loop {
///         tokio::select! {
///             _ = cancel.cancelled() => break,
///             _ = tokio::time::sleep(Duration::from_secs(60)) => {
///                 // Collect metrics...
///             }
///         }
///     }
///     Ok(())
/// }
/// ```
///
/// # Router Service Example
///
/// ```ignore
/// use sidereal_sdk::prelude::*;
/// use axum::{Router, routing::get};
///
/// #[sidereal_sdk::service(path = "/admin")]
/// fn admin_api(ctx: Context) -> Router {
///     Router::new()
///         .route("/health", get(|| async { "OK" }))
/// }
/// ```
#[proc_macro_attribute]
pub fn service(attr: TokenStream, item: TokenStream) -> TokenStream {
    service::expand(attr.into(), item.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}
