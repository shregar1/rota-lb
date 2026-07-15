#![allow(
    clippy::missing_safety_doc,
    clippy::not_unsafe_ptr_arg_deref,
    missing_docs,
    unsafe_code,
    private_interfaces,
    clippy::cargo_common_metadata
)]

mod api;
mod strategy;
mod types;

#[cfg(test)]
mod tests;

pub use types::RotaVersion;
