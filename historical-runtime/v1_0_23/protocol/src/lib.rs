// The schema has one source of truth but is compiled against the historical
// dependency pins in this facade.
include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../protocol-schema/src/lib.rs"
));
