use std::{env, fs, path::PathBuf, process::Command};

use sha2::{Digest, Sha256};

const SOURCE_PATHS: &[&str] = &[
    "Cargo.toml",
    "Cargo.lock",
    "jetstreamer-firehose",
    "jetstreamer-horizon",
    "jetstreamer-node",
    "historical-runtime",
    "vendor/solana-runtime-3.1.12",
    "vendor/solana-svm",
    "vendor/solana-program-runtime",
];

fn command_output(mut command: Command, what: &str) -> String {
    let output = command
        .output()
        .unwrap_or_else(|err| panic!("failed to run {what}: {err}"));
    assert!(
        output.status.success(),
        "{what} exited unsuccessfully: {}",
        output.status
    );
    String::from_utf8(output.stdout)
        .unwrap_or_else(|err| panic!("{what} returned non-UTF-8 output: {err}"))
        .trim()
        .to_owned()
}

fn try_command_output(mut command: Command) -> Option<String> {
    let output = command.output().ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8(output.stdout).ok()?.trim().to_owned())
}

fn try_command_bytes(mut command: Command) -> Option<Vec<u8>> {
    let output = command.output().ok()?;
    output.status.success().then_some(output.stdout)
}

fn hex_prefix(bytes: &[u8], take: usize) -> String {
    bytes
        .iter()
        .take(take)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn main() {
    let rustc = env::var_os("RUSTC").expect("Cargo did not provide RUSTC");
    let mut rustc_command = Command::new(rustc);
    rustc_command.arg("--version").arg("--verbose");
    let rustc_version = command_output(rustc_command, "rustc --version --verbose")
        .lines()
        .filter(|line| {
            line.starts_with("rustc ")
                || line.starts_with("commit-hash:")
                || line.starts_with("host:")
        })
        .collect::<Vec<_>>()
        .join("; ");
    println!("cargo:rustc-env=JETSTREAMER_BUILD_RUSTC={rustc_version}");
    println!(
        "cargo:rustc-env=JETSTREAMER_BUILD_TARGET={}",
        env::var("TARGET").expect("Cargo did not provide TARGET")
    );

    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let repository = manifest_dir.parent().unwrap_or(&manifest_dir);
    let mut revision_command = Command::new("git");
    revision_command
        .arg("rev-parse")
        .arg("HEAD")
        .current_dir(repository);
    let revision = try_command_output(revision_command)
        .filter(|revision| !revision.is_empty())
        .unwrap_or_else(|| format!("package-{}", env!("CARGO_PKG_VERSION")));

    let mut diff_command = Command::new("git");
    diff_command
        .arg("diff")
        .arg("--binary")
        .arg("HEAD")
        .arg("--")
        .args(SOURCE_PATHS)
        .current_dir(repository);
    let tracked_diff = try_command_bytes(diff_command).unwrap_or_default();

    let mut untracked_command = Command::new("git");
    untracked_command
        .arg("ls-files")
        .arg("--others")
        .arg("--exclude-standard")
        .arg("-z")
        .arg("--")
        .args(SOURCE_PATHS)
        .current_dir(repository);
    let untracked = try_command_bytes(untracked_command).unwrap_or_default();
    let mut untracked_paths = untracked
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .map(|path| String::from_utf8(path.to_vec()).expect("source path is not UTF-8"))
        .collect::<Vec<_>>();
    untracked_paths.sort_unstable();

    let dirty = !tracked_diff.is_empty() || !untracked_paths.is_empty();
    let dirty_suffix = if dirty {
        let mut hash = Sha256::new();
        hash.update(b"tracked-diff\0");
        hash.update(&tracked_diff);
        for relative in untracked_paths {
            hash.update(b"untracked\0");
            hash.update(relative.as_bytes());
            hash.update(b"\0");
            let contents = fs::read(repository.join(&relative))
                .unwrap_or_else(|err| panic!("failed to read untracked source {relative}: {err}"));
            hash.update(contents);
        }
        format!("-dirty-{}", hex_prefix(&hash.finalize(), 12))
    } else {
        String::new()
    };
    println!("cargo:rustc-env=JETSTREAMER_BUILD_REVISION={revision}{dirty_suffix}",);

    println!("cargo:rerun-if-env-changed=RUSTC");
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=../Cargo.lock");
    println!("cargo:rerun-if-changed=../vendor/solana-runtime-3.1.12");
    println!("cargo:rerun-if-changed=../historical-runtime");
    println!("cargo:rerun-if-changed=../.git/HEAD");
    println!("cargo:rerun-if-changed=../.git/index");
}
