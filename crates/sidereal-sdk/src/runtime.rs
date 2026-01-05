//! Runtime support for Sidereal functions.
//!
//! This module provides the low-level glue for WASM execution:
//! - Memory allocation exports for the host
//! - Serialisation/deserialisation helpers
//! - Async executor for blocking on futures

use serde::{de::DeserializeOwned, Serialize};
use std::alloc::{alloc, dealloc, Layout};

/// Allocate memory for the host to write input data.
///
/// # Safety
///
/// The returned pointer is valid for `len` bytes and must be freed
/// with `__sidereal_dealloc`.
#[no_mangle]
pub extern "C" fn __sidereal_alloc(len: u32) -> *mut u8 {
    if len == 0 {
        return std::ptr::null_mut();
    }
    let layout = Layout::from_size_align(len as usize, 1).unwrap();
    unsafe { alloc(layout) }
}

/// Free memory previously allocated with `__sidereal_alloc`.
///
/// # Safety
///
/// `ptr` must have been returned by `__sidereal_alloc` with the same `len`.
#[no_mangle]
pub extern "C" fn __sidereal_dealloc(ptr: *mut u8, len: u32) {
    if ptr.is_null() || len == 0 {
        return;
    }
    let layout = Layout::from_size_align(len as usize, 1).unwrap();
    unsafe { dealloc(ptr, layout) }
}

/// Pack a pointer and length into a single u64 for return.
///
/// Format: `(ptr << 32) | len`
fn pack_ptr_len(ptr: *mut u8, len: u32) -> u64 {
    ((ptr as u64) << 32) | (len as u64)
}

/// Invoke a function synchronously with JSON serialisation.
///
/// This is called by the generated WASM entrypoint.
pub fn invoke_sync<I, O, F>(input_ptr: *const u8, input_len: u32, f: F) -> u64
where
    I: DeserializeOwned,
    O: Serialize,
    F: FnOnce(I) -> O,
{
    // Read input bytes from WASM memory
    let input_bytes = if input_ptr.is_null() || input_len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(input_ptr, input_len as usize) }
    };

    // Deserialise input
    let result: Result<O, String> = match serde_json::from_slice(input_bytes) {
        Ok(input) => {
            // Execute the function
            let output = f(input);
            Ok(output)
        }
        Err(e) => Err(format!("deserialisation error: {}", e)),
    };

    // Serialise output (or error)
    let output_json = match result {
        Ok(output) => serde_json::to_vec(&output).unwrap_or_else(|e| {
            serde_json::to_vec(&serde_json::json!({
                "error": format!("serialisation error: {}", e)
            }))
            .unwrap()
        }),
        Err(e) => serde_json::to_vec(&serde_json::json!({ "error": e })).unwrap(),
    };

    // Allocate output buffer and copy data
    let out_len = output_json.len() as u32;
    let out_ptr = __sidereal_alloc(out_len);
    if !out_ptr.is_null() {
        unsafe {
            std::ptr::copy_nonoverlapping(output_json.as_ptr(), out_ptr, output_json.len());
        }
    }

    pack_ptr_len(out_ptr, out_len)
}

/// Block on an async future.
///
/// Uses a simple single-threaded executor suitable for WASM.
pub fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    futures::executor::block_on(fut)
}
