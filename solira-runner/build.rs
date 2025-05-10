//! This script runs after Cargo has built all **build-dependencies**. The helper crate
//! `force_solira_cdylib` is a build-dependency that links to `libsolira` as a *dylib*, which
//! forces Cargo to emit `libsolira.{so|dylib|dll}` **before** this script executes.

use std::{
    env, fs,
    path::{Path, PathBuf},
};

fn main() {
    // Where Cargo places this crate’s build artefacts
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").unwrap());

    // These env-vars tell us the target triple & profile
    let target = env::var("TARGET").unwrap(); // e.g. x86_64-unknown-linux-gnu

    // ── find target/<triple>/<profile>/deps ────────────────────────────────
    let mut deps_dir = out_dir.clone();
    for _ in 0..3 {
        deps_dir.pop();
    } // .../target/<triple>/<profile>
    deps_dir.push("deps");

    // platform-specific prefix / extension for shared libraries
    let (prefix, ext) = if target.contains("windows") {
        ("", "dll")
    } else if target.contains("apple") {
        ("lib", "dylib")
    } else {
        ("lib", "so")
    };
    let stem_prefix = format!("{prefix}solira"); // "libsolira" or "solira"

    // ── locate the cdylib built for solira ─────────────────────────────────
    let cdylib_path = fs::read_dir(&deps_dir)
        .expect("deps dir missing")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| {
            p.extension().map(|x| x == ext).unwrap_or(false)
                && p.file_stem()
                    .and_then(|s| s.to_str())
                    .map(|s| s.starts_with(&stem_prefix))
                    .unwrap_or(false)
        })
        .expect(
            "libsolira.* not found – is `force_solira_cdylib` \
                 listed in [build-dependencies]?",
        );

    // ── write <OUT_DIR>/embed.rs with the bytes of the cdylib ──────────────
    let embed_rs = Path::new(&out_dir).join("embed.rs");
    fs::write(
        &embed_rs,
        format!(
            "/// Raw bytes of libsolira ({} bytes)\n\
             pub const SOLIRA_CDYLIB: &[u8] = include_bytes!(r#\"{}\"#);\n",
            fs::metadata(&cdylib_path).unwrap().len(),
            cdylib_path.display()
        ),
    )
    .unwrap();

    // Re-run script only if it itself changes
    println!("cargo:rerun-if-changed=build.rs");
}
