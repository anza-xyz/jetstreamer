// The schema has one source of truth but is compiled against the current
// dependency versions in this facade.
include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../protocol-schema/src/lib.rs"
));
