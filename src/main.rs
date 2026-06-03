use jetstreamer::{BuiltinPlugin, JetstreamerInvocation, JetstreamerRunner};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    match JetstreamerRunner::default()
        .with_log_level("info")
        .parse_cli_args()?
    {
        JetstreamerInvocation::Run(runner) => runner
            .run()
            .map_err(|err| -> Box<dyn std::error::Error> { Box::new(err) })?,
        JetstreamerInvocation::ListPlugins => {
            for plugin in BuiltinPlugin::ALL {
                println!("{}", plugin.name());
            }
        }
    }
    Ok(())
}
