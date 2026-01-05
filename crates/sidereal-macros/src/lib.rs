//! Procedural macros for the Sidereal SDK.
//!
//! This crate provides the `#[sidereal::function]` attribute macro
//! for defining deployable functions.

mod function;

use proc_macro::TokenStream;

/// Marks a function as a Sidereal function.
///
/// This macro transforms an async function into a WASM-callable entrypoint
/// with JSON serialisation for input and output.
///
/// # Example
///
/// ```ignore
/// use sidereal_sdk::prelude::*;
///
/// #[sidereal::function]
/// async fn greet(name: String) -> String {
///     format!("Hello, {}!", name)
/// }
/// ```
#[proc_macro_attribute]
pub fn function(attr: TokenStream, item: TokenStream) -> TokenStream {
    function::expand(attr.into(), item.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}
