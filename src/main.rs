use solira::SoliraRunner;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    SoliraRunner::default()
        .with_log_level("info")
        .parse_cli_args()?
        .with_automatic_geyser_config()
        .await
        .run()
        .await
}
