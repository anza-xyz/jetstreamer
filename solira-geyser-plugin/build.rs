use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::{env, fs};

fn main() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    println!("cargo:rerun-if-changed=bin/clickhouse");
    let clickhouse_binary = out_dir.join("clickhouse");

    // Check if the ClickHouse binary exists
    if !clickhouse_binary.exists() {
        println!("ClickHouse binary not found. Downloading ClickHouse...");

        // Run the curl command to download and install ClickHouse
        let status = Command::new("sh")
            .arg("-c")
            .arg("curl https://clickhouse.com/ | sh")
            .current_dir(&out_dir)
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

    let embed_clickhouse_rs = Path::new(&out_dir).join("embed_clickhouse.rs");
    fs::write(
        &embed_clickhouse_rs,
        format!(
            "/// Raw bytes of clickhouse binary ({} bytes)\n\
             pub const CLICKHOUSE_BINARY: &[u8] = include_bytes!(r#\"{}\"#);\n",
            fs::metadata(&clickhouse_binary).unwrap().len(),
            clickhouse_binary.display()
        ),
    )
    .unwrap();
}
