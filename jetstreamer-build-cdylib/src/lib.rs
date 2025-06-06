#![doc = r#"
This crate exists solely to guarantee the jetstreamer cdylib is built before any consumer/plugin/bin that needs it.
No linking or FFI is performed here.
"#]
