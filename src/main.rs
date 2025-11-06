use jetstreamer::JetstreamerRunner;
use jetstreamer_plugin::plugins::instruction_tracking::InstructionTrackingPlugin;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    JetstreamerRunner::default()
        .with_log_level("info")
        .parse_cli_args()?
        .with_plugin(Box::new(InstructionTrackingPlugin))
        .run()
        .map_err(|err| -> Box<dyn std::error::Error> { Box::new(err) })?;
    Ok(())
}
