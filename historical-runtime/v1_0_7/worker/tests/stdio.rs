use jetstreamer_historical_protocol::{
    read_frame, write_frame, BackendQualification, Request, RequestBody, Response, ResponseBody,
    PROTOCOL_VERSION,
};
use std::{
    io,
    process::{Command, Stdio},
};

#[test]
fn worker_remains_alive_across_framed_requests() -> io::Result<()> {
    // Cargo 1.42 predates the compile-time CARGO_BIN_EXE_* variable used by
    // the v1.0.24 workspace. Integration tests live in target/debug/deps;
    // Cargo still builds the package binary next to that directory.
    let mut executable = std::env::current_exe()?;
    executable.pop();
    if executable.ends_with("deps") {
        executable.pop();
    }
    executable.push("jetstreamer-historical-worker-v1-0-7");
    let mut child = Command::new(&executable)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    let mut input = child.stdout.take().unwrap();
    let mut output = child.stdin.take().unwrap();

    write_frame(
        &mut output,
        &Request {
            id: 1,
            body: RequestBody::Hello {
                protocol_version: PROTOCOL_VERSION,
            },
        },
    )?;
    let response: Response = read_frame(&mut input)?.unwrap();
    match response.body {
        ResponseBody::Handshake(handshake) => {
            assert_eq!(handshake.backend_id, "solana-v1.0.7");
            assert_eq!(handshake.qualification, BackendQualification::Candidate);
        }
        other => panic!("unexpected handshake response: {:?}", other),
    }

    write_frame(
        &mut output,
        &Request {
            id: 2,
            body: RequestBody::Ping,
        },
    )?;
    let response: Response = read_frame(&mut input)?.unwrap();
    assert_eq!(response.request_id, 2);
    assert_eq!(response.body, ResponseBody::Pong);

    write_frame(
        &mut output,
        &Request {
            id: 3,
            body: RequestBody::Shutdown,
        },
    )?;
    let response: Response = read_frame(&mut input)?.unwrap();
    assert_eq!(response.body, ResponseBody::ShuttingDown);
    drop(output);
    assert!(child.wait()?.success());
    Ok(())
}
