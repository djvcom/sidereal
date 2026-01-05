//! Implementation of the `#[sidereal::function]` macro.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{parse2, Error, ItemFn, Result};

pub fn expand(_attr: TokenStream, item: TokenStream) -> Result<TokenStream> {
    let func: ItemFn = parse2(item)?;

    let vis = &func.vis;
    let sig = &func.sig;
    let block = &func.block;
    let fn_name = &sig.ident;
    let asyncness = &sig.asyncness;

    // Validate: must be async
    if asyncness.is_none() {
        return Err(Error::new_spanned(
            sig,
            "sidereal::function must be async",
        ));
    }

    // Extract input type (single parameter for now)
    let inputs = &sig.inputs;
    if inputs.len() != 1 {
        return Err(Error::new_spanned(
            inputs,
            "sidereal::function must have exactly one parameter (for now)",
        ));
    }

    let input_arg = inputs.first().unwrap();
    let (input_pat, input_ty) = match input_arg {
        syn::FnArg::Typed(pat_type) => (&pat_type.pat, &pat_type.ty),
        syn::FnArg::Receiver(_) => {
            return Err(Error::new_spanned(
                input_arg,
                "sidereal::function cannot have self parameter",
            ));
        }
    };

    // Extract output type
    let output_ty = match &sig.output {
        syn::ReturnType::Default => quote! { () },
        syn::ReturnType::Type(_, ty) => quote! { #ty },
    };

    // Generate names
    let impl_fn_name = format_ident!("__sidereal_impl_{}", fn_name);
    let call_fn_name = format_ident!("__sidereal_call_{}", fn_name);

    let expanded = quote! {
        // Original function (renamed)
        #[doc(hidden)]
        #vis async fn #impl_fn_name(#input_pat: #input_ty) -> #output_ty
        #block

        // WASM entrypoint
        #[doc(hidden)]
        #[no_mangle]
        pub extern "C" fn #call_fn_name(ptr: *const u8, len: u32) -> u64 {
            ::sidereal_sdk::runtime::invoke_sync::<#input_ty, #output_ty, _>(
                ptr,
                len,
                |input| ::sidereal_sdk::runtime::block_on(#impl_fn_name(input)),
            )
        }

        // Public wrapper that calls the impl (for testing)
        #vis async fn #fn_name(#input_pat: #input_ty) -> #output_ty {
            #impl_fn_name(#input_pat).await
        }
    };

    Ok(expanded)
}
