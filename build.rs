use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::{env, fs};

fn main() {
    let bin_dir = Path::new("bin");
    // Check if the bin directory exists, create it if it doesn't
    if !bin_dir.exists() {
        println!("Creating bin directory...");
        fs::create_dir(bin_dir).expect("Failed to create bin directory");
    }

    std::fs::copy("plugin_config.json", bin_dir.join("plugin_config.json")).unwrap();
    println!("cargo:rerun_if-changed=plugin_config.json");

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let artifact_dir = out_dir
        .parent() // build/{pkg}-{hash}/out  → build/{pkg}-{hash}
        .and_then(|p| p.parent()) // build/{pkg}-{hash}   → build
        .and_then(|p| p.parent()) // build                 → {profile}
        .ok_or("failed to derive artifact dir")
        .unwrap();
    println!("cargo:env=OUT_DIR={}", out_dir.display());
    println!("cargo:env=ARTIFACT_DIR={}", artifact_dir.display());
    let mut cfg = String::new();
    fs::File::open("plugin_config.json")
        .unwrap()
        .read_to_string(&mut cfg)
        .unwrap();
    let ext = if cfg!(target_os = "macos") {
        "dylib"
    } else {
        "so"
    };
    cfg = cfg.replace("{{ext}}", ext);
    cfg = cfg.replace(
        "{{full-path}}",
        &artifact_dir.canonicalize().unwrap().to_string_lossy(),
    );
    let cfg_path = artifact_dir.join("plugin_config.json");
    let mut f = fs::File::create(&cfg_path).unwrap();
    f.write_all(cfg.as_bytes()).unwrap();

    println!("cargo:rerun-if-changed=bin/clickhouse");
    let clickhouse_binary = bin_dir.join("clickhouse");

    // Check if the ClickHouse binary exists
    if !clickhouse_binary.exists() {
        println!("ClickHouse binary not found. Downloading ClickHouse...");

        // Run the curl command to download and install ClickHouse
        let status = Command::new("sh")
            .arg("-c")
            .arg("curl https://clickhouse.com/ | sh")
            .current_dir(bin_dir)
            .status()
            .expect("Failed to download and install ClickHouse");

        if !status.success() {
            panic!("ClickHouse installation failed with status: {}", status);
        }

        // Mark the ClickHouse binary as executable
        if clickhouse_binary.exists() {
            println!("Setting ClickHouse binary as executable...");
            let mut permissions = fs::metadata(&clickhouse_binary)
                .expect("Failed to get ClickHouse binary metadata")
                .permissions();
            permissions.set_mode(0o755); // rwxr-xr-x
            fs::set_permissions(&clickhouse_binary, permissions)
                .expect("Failed to set ClickHouse binary as executable");
        } else {
            panic!("ClickHouse binary was not downloaded correctly.");
        }
    } else {
        println!("ClickHouse binary already exists. Skipping installation.");
    }
}
