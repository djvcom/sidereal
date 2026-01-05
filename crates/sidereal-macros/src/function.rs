//! Implementation of the `#[sidereal_sdk::function]` macro.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{parse2, Error, FnArg, ItemFn, Result, Type};

/// Detected trigger kind from the first parameter type.
enum DetectedTrigger {
    Http,
    Queue { inner_type_name: String },
}

pub fn expand(_attr: TokenStream, item: TokenStream) -> Result<TokenStream> {
    let func: ItemFn = parse2(item)?;

    let vis = &func.vis;
    let sig = &func.sig;
    let block = &func.block;
    let fn_name = &sig.ident;
    let fn_name_str = fn_name.to_string();
    let asyncness = &sig.asyncness;

    // Validate: must be async
    if asyncness.is_none() {
        return Err(Error::new_spanned(
            sig,
            "sidereal_sdk::function must be async",
        ));
    }

    // Extract parameters - expect (trigger, ctx) or just (trigger)
    let inputs = &sig.inputs;
    if inputs.is_empty() || inputs.len() > 2 {
        return Err(Error::new_spanned(
            inputs,
            "sidereal_sdk::function must have 1 or 2 parameters: (trigger) or (trigger, ctx)",
        ));
    }

    // First parameter is the trigger type
    let trigger_arg = inputs.first().unwrap();
    let (trigger_pat, trigger_ty) = extract_typed_arg(trigger_arg)?;

    // Detect the trigger kind and extract inner type
    let (detected_trigger, inner_type) = detect_trigger_type(trigger_ty)?;

    // Second parameter (optional) is Context
    let has_context = inputs.len() == 2;
    let ctx_pat = if has_context {
        let ctx_arg = inputs.iter().nth(1).unwrap();
        let (pat, _ty) = extract_typed_arg(ctx_arg)?;
        Some(pat)
    } else {
        None
    };

    // Extract output type
    let output_ty = match &sig.output {
        syn::ReturnType::Default => quote! { () },
        syn::ReturnType::Type(_, ty) => quote! { #ty },
    };

    // Generate names
    let impl_fn_name = format_ident!("__sidereal_impl_{}", fn_name);
    let handler_fn_name = format_ident!("__sidereal_handler_{}", fn_name);

    // Generate the context parameter handling
    let ctx_creation = if has_context {
        quote! { let #ctx_pat = ctx; }
    } else {
        quote! { let _ = ctx; }
    };

    // Generate trigger-specific code
    let (trigger_kind, queue_name_value, trigger_creation, response_handling) = match &detected_trigger {
        DetectedTrigger::Http => {
            let trigger_kind = quote! { ::sidereal_sdk::TriggerKind::Http };
            let queue_name_value = quote! { None };
            let trigger_creation = quote! {
                let trigger = ::sidereal_sdk::HttpRequest::new(body);
            };
            let response_handling = quote! {
                match ::sidereal_sdk::__internal::serde_json::to_vec(&response) {
                    Ok(bytes) => ::sidereal_sdk::FunctionResult::with_status(response.status, bytes),
                    Err(e) => ::sidereal_sdk::FunctionResult::error(
                        500,
                        &format!("serialisation error: {}", e),
                    ),
                }
            };
            (trigger_kind, queue_name_value, trigger_creation, response_handling)
        }
        DetectedTrigger::Queue { inner_type_name } => {
            let queue_name = type_name_to_queue_name(inner_type_name);
            let trigger_kind = quote! { ::sidereal_sdk::TriggerKind::Queue };
            let queue_name_value = quote! { Some(#queue_name) };
            let trigger_creation = quote! {
                let trigger = ::sidereal_sdk::QueueMessage::new(body, #queue_name);
            };
            // Queue functions return Result<(), E> - success is 200, error is 500
            let response_handling = quote! {
                match ::sidereal_sdk::__internal::serde_json::to_vec(&response) {
                    Ok(bytes) => ::sidereal_sdk::FunctionResult::ok(bytes),
                    Err(e) => ::sidereal_sdk::FunctionResult::error(
                        500,
                        &format!("serialisation error: {}", e),
                    ),
                }
            };
            (trigger_kind, queue_name_value, trigger_creation, response_handling)
        }
    };

    let expanded = quote! {
        // Original async function (renamed)
        #[doc(hidden)]
        #vis async fn #impl_fn_name(#trigger_pat: #trigger_ty, ctx: ::sidereal_sdk::Context) -> #output_ty
        #block

        // Handler function that deserialises input and calls the impl
        #[doc(hidden)]
        fn #handler_fn_name(
            bytes: &[u8],
            ctx: ::sidereal_sdk::Context,
        ) -> ::std::pin::Pin<::std::boxed::Box<dyn ::std::future::Future<Output = ::sidereal_sdk::FunctionResult> + Send + '_>> {
            ::std::boxed::Box::pin(async move {
                // Deserialise the request body
                let body: #inner_type = match ::sidereal_sdk::__internal::serde_json::from_slice(bytes) {
                    Ok(b) => b,
                    Err(e) => {
                        return ::sidereal_sdk::FunctionResult::error(
                            400,
                            &format!("deserialisation error: {}", e),
                        );
                    }
                };

                // Create the trigger wrapper
                #trigger_creation

                // Create context binding
                #ctx_creation

                // Call the implementation
                let response = #impl_fn_name(trigger, ctx).await;

                // Handle the response
                #response_handling
            })
        }

        // Register the function with inventory
        ::sidereal_sdk::__internal::inventory::submit! {
            ::sidereal_sdk::FunctionMetadata {
                name: #fn_name_str,
                trigger_kind: #trigger_kind,
                queue_name: #queue_name_value,
                handler: #handler_fn_name,
            }
        }

        // Public wrapper for direct calls (useful for testing)
        #vis async fn #fn_name(#trigger_pat: #trigger_ty, ctx: ::sidereal_sdk::Context) -> #output_ty {
            #impl_fn_name(#trigger_pat, ctx).await
        }
    };

    Ok(expanded)
}

fn extract_typed_arg(arg: &FnArg) -> Result<(&syn::Pat, &Type)> {
    match arg {
        FnArg::Typed(pat_type) => Ok((&pat_type.pat, &pat_type.ty)),
        FnArg::Receiver(_) => Err(Error::new_spanned(
            arg,
            "sidereal_sdk::function cannot have self parameter",
        )),
    }
}

/// Detect the trigger type from the first parameter and extract the inner type.
fn detect_trigger_type(ty: &Type) -> Result<(DetectedTrigger, TokenStream)> {
    if let Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            let type_name = segment.ident.to_string();

            // Extract the inner type T from Wrapper<T>
            let inner_type = if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                if let Some(syn::GenericArgument::Type(inner)) = args.args.first() {
                    quote! { #inner }
                } else {
                    return Err(Error::new_spanned(
                        ty,
                        "Trigger type must have a generic parameter",
                    ));
                }
            } else {
                return Err(Error::new_spanned(
                    ty,
                    "Trigger type must have a generic parameter (e.g., HttpRequest<T>)",
                ));
            };

            // Detect trigger kind based on the wrapper type name
            let detected = match type_name.as_str() {
                "HttpRequest" => DetectedTrigger::Http,
                "QueueMessage" => {
                    // Extract the inner type name for queue name derivation
                    let inner_type_name = extract_type_name_string(ty)?;
                    DetectedTrigger::Queue { inner_type_name }
                }
                other => {
                    return Err(Error::new_spanned(
                        ty,
                        format!(
                            "Unknown trigger type '{}'. Expected HttpRequest<T> or QueueMessage<T>",
                            other
                        ),
                    ));
                }
            };

            return Ok((detected, inner_type));
        }
    }

    Err(Error::new_spanned(
        ty,
        "Could not parse trigger type. Expected HttpRequest<T> or QueueMessage<T>",
    ))
}

/// Extract the inner type name as a string (for queue name derivation).
fn extract_type_name_string(ty: &Type) -> Result<String> {
    if let Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                if let Some(syn::GenericArgument::Type(Type::Path(inner_path))) = args.args.first()
                {
                    // Get the last segment of the inner type path
                    if let Some(inner_segment) = inner_path.path.segments.last() {
                        return Ok(inner_segment.ident.to_string());
                    }
                }
            }
        }
    }

    Err(Error::new_spanned(
        ty,
        "Could not extract inner type name for queue name derivation",
    ))
}

/// Convert a type name to a queue name using kebab-case convention.
///
/// Examples:
/// - `OrderCreated` → `order-created`
/// - `UserNotification` → `user-notification`
fn type_name_to_queue_name(type_name: &str) -> String {
    let mut result = String::with_capacity(type_name.len() + 4);
    let mut chars = type_name.chars().peekable();

    while let Some(c) = chars.next() {
        if c.is_uppercase() {
            if !result.is_empty() {
                let next_is_lower = chars.peek().is_some_and(|n| n.is_lowercase());
                if next_is_lower || result.chars().last().is_some_and(|p| p.is_lowercase()) {
                    result.push('-');
                }
            }
            result.push(c.to_ascii_lowercase());
        } else {
            result.push(c);
        }
    }

    result
}
