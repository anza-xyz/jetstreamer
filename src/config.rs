use indoc::indoc;

pub const CONFIG_TEMPLATE: &str = indoc! {r#"
    {
        "libpath": "{{full-path}}/libsolira.{{ext}}",
        "name": "GeyserPluginSolira",
        "clickhouse": {
            "host": "127.0.0.1",
            "port": 8123,
            "database": "default",
            "username": "default",
            "password": ""
        },
        "log_level": "info"
    }
"#};

pub async fn generate_geyser_plugin_config() -> tokio::io::Result<()> {
    todo!();
}
