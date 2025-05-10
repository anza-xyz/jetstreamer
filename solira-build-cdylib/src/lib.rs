//! force_solira_cdylib/src/lib.rs
//!
//! This crate does nothing except ask the linker for a *dylib* called
//! `solira`, which makes Cargo compile `solira` as a cdylib **before**
//! any build‑scripts run.

// Make rustc *link* to the dynamic version of `solira`
#[link(name = "solira", kind = "dylib")]
unsafe extern "C" {
    unsafe fn _create_plugin();
}
