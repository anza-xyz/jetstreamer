use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;
use std::{env, fs};

fn main() {
    // Get the OUT_DIR and shared library path
    let out_dir = env::var("OUT_DIR").expect("OUT_DIR not set");
    let lib_path = format!("{}/libsolira.so", out_dir);

    // Read the template file
    let template_path = Path::new("plugin_config.template.json");
    let template =
        fs::read_to_string(&template_path).expect("Failed to read plugin_config.template.json");

    // Replace the placeholder with the actual library path
    let config = template.replace("{{LIB_PATH}}", &lib_path);

    // Write the final config to OUT_DIR
    let config_path = Path::new(&out_dir).join("plugin_config.json");
    fs::write(config_path, config).expect("Failed to write plugin_config.json");

    println!("cargo:rerun-if-changed=plugin_config.template.json");

    println!("cargo:rerun-if-changed=bin/clickhouse");
    let bin_dir = Path::new("bin");
    let clickhouse_binary = bin_dir.join("clickhouse");

    // Check if the bin directory exists, create it if it doesn't
    if !bin_dir.exists() {
        println!("Creating bin directory...");
        fs::create_dir(bin_dir).expect("Failed to create bin directory");
    }

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
