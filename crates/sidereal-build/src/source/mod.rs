//! Source checkout operations using gitoxide (gix).
//!
//! Handles cloning repositories and checking out specific commits
//! for the build process.

mod checkout;

pub use checkout::{SourceCheckout, SourceManager};
