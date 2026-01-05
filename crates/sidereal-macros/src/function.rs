//! Implementation of the `#[sidereal_sdk::function]` macro.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{parse2, Error, FnArg, ItemFn, Result, Type};

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

    // Second parameter (optional) is Context
    let has_context = inputs.len() == 2;
    let ctx_pat = if has_context {
        let ctx_arg = inputs.iter().nth(1).unwrap();
        let (pat, _ty) = extract_typed_arg(ctx_arg)?;
        Some(pat)
    } else {
        None
    };

    // Extract the inner type from HttpRequest<T>
    let inner_type = extract_inner_type(trigger_ty)?;

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
                let trigger = ::sidereal_sdk::HttpRequest::new(body);

                // Create context binding
                #ctx_creation

                // Call the implementation
                let response = #impl_fn_name(trigger, ctx).await;

                // Serialise the response
                match ::sidereal_sdk::__internal::serde_json::to_vec(&response) {
                    Ok(bytes) => ::sidereal_sdk::FunctionResult::with_status(response.status, bytes),
                    Err(e) => ::sidereal_sdk::FunctionResult::error(
                        500,
                        &format!("serialisation error: {}", e),
                    ),
                }
            })
        }

        // Register the function with inventory
        ::sidereal_sdk::__internal::inventory::submit! {
            ::sidereal_sdk::FunctionMetadata {
                name: #fn_name_str,
                trigger_kind: ::sidereal_sdk::TriggerKind::Http,
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

fn extract_inner_type(ty: &Type) -> Result<TokenStream> {
    // Try to extract T from HttpRequest<T>
    if let Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                if let Some(syn::GenericArgument::Type(inner)) = args.args.first() {
                    return Ok(quote! { #inner });
                }
            }
        }
    }

    // If we can't extract it, just use the whole type
    // This will likely fail at compile time with a helpful error
    Ok(quote! { #ty })
}
