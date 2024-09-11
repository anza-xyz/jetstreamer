use std::env;

pub mod dev_mode;

fn main() {
    if env::var("DEV_MODE").is_ok() {
        dev_mode::spawn_local_server();
    }
    println!("Hello, world!");
}
