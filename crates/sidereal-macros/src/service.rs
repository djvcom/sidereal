//! Implementation of the `#[sidereal_sdk::service]` macro.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{parse2, Error, FnArg, ItemFn, Lit, Meta, Result, ReturnType, Type};

/// Detected service kind from the return type.
enum DetectedServiceKind {
    Background,
    Router,
}

/// Parsed macro attributes.
struct ServiceAttributes {
    path: Option<String>,
}

pub fn expand(attr: TokenStream, item: TokenStream) -> Result<TokenStream> {
    let attrs = parse_attributes(attr)?;
    let func: ItemFn = parse2(item)?;

    let fn_name = &func.sig.ident;
    let fn_name_str = fn_name.to_string();

    // Detect service kind from return type
    let detected_kind = detect_service_kind(&func.sig.output)?;

    match detected_kind {
        DetectedServiceKind::Background => expand_background_service(&func, &attrs, &fn_name_str),
        DetectedServiceKind::Router => expand_router_service(&func, &attrs, &fn_name_str),
    }
}

fn parse_attributes(attr: TokenStream) -> Result<ServiceAttributes> {
    let mut attrs = ServiceAttributes { path: None };

    if attr.is_empty() {
        return Ok(attrs);
    }

    // Parse the attribute as a Meta item
    let meta: Meta = syn::parse2(attr)?;

    match &meta {
        Meta::NameValue(nv) if nv.path.is_ident("path") => {
            if let syn::Expr::Lit(syn::ExprLit {
                lit: Lit::Str(lit_str),
                ..
            }) = &nv.value
            {
                attrs.path = Some(lit_str.value());
            } else {
                return Err(Error::new_spanned(&nv.value, "path must be a string literal"));
            }
        }
        _ => {
            return Err(Error::new_spanned(
                &meta,
                "Unknown attribute. Expected: path = \"/prefix\"",
            ));
        }
    }

    Ok(attrs)
}

fn detect_service_kind(output: &ReturnType) -> Result<DetectedServiceKind> {
    match output {
        ReturnType::Default => Ok(DetectedServiceKind::Background),
        ReturnType::Type(_, ty) => {
            if is_router_type(ty) {
                Ok(DetectedServiceKind::Router)
            } else {
                Ok(DetectedServiceKind::Background)
            }
        }
    }
}

fn is_router_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            let name = segment.ident.to_string();
            return name == "Router";
        }
    }
    false
}

fn expand_background_service(
    func: &ItemFn,
    _attrs: &ServiceAttributes,
    fn_name_str: &str,
) -> Result<TokenStream> {
    let vis = &func.vis;
    let sig = &func.sig;
    let block = &func.block;
    let fn_name = &sig.ident;
    let asyncness = &sig.asyncness;

    // Background services must be async
    if asyncness.is_none() {
        return Err(Error::new_spanned(
            sig,
            "Background services must be async",
        ));
    }

    // Validate parameters - expect (ctx, cancel) or just (ctx)
    let inputs = &sig.inputs;
    if inputs.is_empty() || inputs.len() > 2 {
        return Err(Error::new_spanned(
            inputs,
            "Background service must have 1 or 2 parameters: (ctx) or (ctx, cancel)",
        ));
    }

    // First parameter is Context
    let ctx_arg = inputs.first().unwrap();
    let (ctx_pat, _ctx_ty) = extract_typed_arg(ctx_arg)?;

    // Second parameter (optional) is CancellationToken
    let has_cancel = inputs.len() == 2;
    let cancel_pat = if has_cancel {
        let cancel_arg = inputs.iter().nth(1).unwrap();
        let (pat, _ty) = extract_typed_arg(cancel_arg)?;
        Some(pat)
    } else {
        None
    };

    let impl_fn_name = format_ident!("__sidereal_service_impl_{}", fn_name);
    let factory_fn_name = format_ident!("__sidereal_service_factory_{}", fn_name);

    // Extract return type for proper handling
    let output_ty = match &sig.output {
        ReturnType::Default => quote! { () },
        ReturnType::Type(_, ty) => quote! { #ty },
    };

    // Generate the cancel binding if not present
    let cancel_binding = if has_cancel {
        quote! { let #cancel_pat = cancel; }
    } else {
        quote! { let _ = cancel; }
    };

    let expanded = quote! {
        // Original async function (renamed)
        #[doc(hidden)]
        #vis async fn #impl_fn_name(
            #ctx_pat: ::sidereal_sdk::Context,
            cancel: ::sidereal_sdk::__internal::tokio_util::sync::CancellationToken,
        ) -> #output_ty {
            #cancel_binding
            #block
        }

        // Factory function for inventory registration
        #[doc(hidden)]
        fn #factory_fn_name(
            ctx: ::sidereal_sdk::Context,
            cancel: ::sidereal_sdk::__internal::tokio_util::sync::CancellationToken,
        ) -> ::std::pin::Pin<::std::boxed::Box<
            dyn ::std::future::Future<Output = ::std::result::Result<(), ::sidereal_sdk::ServiceError>>
            + ::std::marker::Send + 'static
        >> {
            ::std::boxed::Box::pin(async move {
                let result = #impl_fn_name(ctx, cancel).await;
                ::sidereal_sdk::__internal::convert_service_result(result)
            })
        }

        // Register the service with inventory
        ::sidereal_sdk::__internal::inventory::submit! {
            ::sidereal_sdk::ServiceMetadata {
                name: #fn_name_str,
                kind: ::sidereal_sdk::ServiceKind::Background,
                path_prefix: ::std::option::Option::None,
                factory: ::sidereal_sdk::ServiceFactory::Background(#factory_fn_name),
            }
        }

        // Public wrapper for direct calls (useful for testing)
        #vis async fn #fn_name(
            ctx: ::sidereal_sdk::Context,
            cancel: ::sidereal_sdk::__internal::tokio_util::sync::CancellationToken,
        ) -> #output_ty {
            #impl_fn_name(ctx, cancel).await
        }
    };

    Ok(expanded)
}

fn expand_router_service(
    func: &ItemFn,
    attrs: &ServiceAttributes,
    fn_name_str: &str,
) -> Result<TokenStream> {
    let vis = &func.vis;
    let sig = &func.sig;
    let block = &func.block;
    let fn_name = &sig.ident;

    // Router services must NOT be async
    if sig.asyncness.is_some() {
        return Err(Error::new_spanned(
            sig,
            "Router services must not be async (return Router synchronously)",
        ));
    }

    // Validate parameters - expect just (ctx)
    let inputs = &sig.inputs;
    if inputs.len() != 1 {
        return Err(Error::new_spanned(
            inputs,
            "Router service must have exactly 1 parameter: (ctx: Context)",
        ));
    }

    // First parameter is Context
    let ctx_arg = inputs.first().unwrap();
    let (ctx_pat, _ctx_ty) = extract_typed_arg(ctx_arg)?;

    let impl_fn_name = format_ident!("__sidereal_service_impl_{}", fn_name);
    let factory_fn_name = format_ident!("__sidereal_service_factory_{}", fn_name);

    // Determine path prefix
    let path_prefix = if let Some(path) = &attrs.path {
        quote! { ::std::option::Option::Some(#path) }
    } else {
        let default_path = format!("/{}", fn_name_str);
        quote! { ::std::option::Option::Some(#default_path) }
    };

    let expanded = quote! {
        // Original function (renamed)
        #[doc(hidden)]
        #vis fn #impl_fn_name(#ctx_pat: ::sidereal_sdk::Context) -> ::axum::Router
        #block

        // Factory function for inventory registration
        #[doc(hidden)]
        fn #factory_fn_name(ctx: ::sidereal_sdk::Context) -> ::axum::Router {
            #impl_fn_name(ctx)
        }

        // Register the service with inventory
        ::sidereal_sdk::__internal::inventory::submit! {
            ::sidereal_sdk::ServiceMetadata {
                name: #fn_name_str,
                kind: ::sidereal_sdk::ServiceKind::Router,
                path_prefix: #path_prefix,
                factory: ::sidereal_sdk::ServiceFactory::Router(#factory_fn_name),
            }
        }

        // Public wrapper for direct calls
        #vis fn #fn_name(ctx: ::sidereal_sdk::Context) -> ::axum::Router {
            #impl_fn_name(ctx)
        }
    };

    Ok(expanded)
}

fn extract_typed_arg(arg: &FnArg) -> Result<(&syn::Pat, &Type)> {
    match arg {
        FnArg::Typed(pat_type) => Ok((&pat_type.pat, &pat_type.ty)),
        FnArg::Receiver(_) => Err(Error::new_spanned(
            arg,
            "sidereal_sdk::service cannot have self parameter",
        )),
    }
}
