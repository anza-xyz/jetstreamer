use jetstreamer::JetstreamerRunner;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    JetstreamerRunner::default()
        .with_log_level("info")
        .parse_cli_args()?
        .run()
        .map_err(|err| -> Box<dyn std::error::Error> { Box::new(err) })?;
    Ok(())
}
