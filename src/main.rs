use solira::SoliraRunner;
use solira_plugin::plugins::program_tracking::ProgramTrackingPlugin;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    Ok(SoliraRunner::default()
        .with_log_level("info")
        .parse_cli_args()?
        .with_plugin(Box::new(ProgramTrackingPlugin::default()))
        .with_solira_geyser_config()
        .await
        .run()
        .await?)
}
