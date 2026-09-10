use std::{env, process::Command};

fn main() {
    let rustc = env::var("RUSTC").expect("Cargo did not provide RUSTC");
    let output = Command::new(rustc)
        .arg("--version")
        .arg("--verbose")
        .output()
        .expect("failed to execute rustc --version --verbose");
    assert!(output.status.success(), "rustc --version failed");
    let version = String::from_utf8(output.stdout).expect("rustc version was not UTF-8");
    assert!(
        version.starts_with("rustc 1.45.1 (c367798cf 2020-07-26)\n"),
        "historical v1.3.19 worker requires exact rustc 1.45.1; got {}",
        version.lines().next().unwrap_or("unknown")
    );
    let target = env::var("TARGET").expect("Cargo did not provide TARGET");
    assert_eq!(target, "x86_64-unknown-linux-gnu");
    println!(
        "cargo:rustc-env=HISTORICAL_RUSTC_VERSION={}",
        version.lines().next().unwrap()
    );
    println!("cargo:rustc-env=HISTORICAL_BUILD_TARGET={}", target);
}
