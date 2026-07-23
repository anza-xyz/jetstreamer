use jetstreamer::{BuiltinPlugin, JetstreamerInvocation, JetstreamerRunner};

// jemalloc, for the same reason as jetstreamer-node: glibc malloc handles
// this workload's huge short-lived allocations poorly (kernel churn, memory
// never returned to the OS).
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

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
