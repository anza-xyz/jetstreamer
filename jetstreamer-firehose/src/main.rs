use {
    jetstreamer_firehose::{firehose::firehose_geyser, index::get_index_base_url},
    reqwest::Client,
    std::{env::args, sync::Arc},
};

fn main() {
    solana_logger::setup_with_default("info");
    let client = Client::new();
    let index_base_url =
        get_index_base_url().expect("failed to resolve remote slot offset index location");
    let first_arg = args().nth(1).expect("no first argument given");
    let slot_range = if first_arg.contains(':') {
        let (slot_a, slot_b) = first_arg
            .split_once(':')
            .expect("failed to parse slot range, expected format: <start>:<end> or a single epoch");
        let slot_a: u64 = slot_a.parse().expect("failed to parse first slot");
        let slot_b: u64 = slot_b.parse().expect("failed to parse second slot");
        slot_a..(slot_b + 1)
    } else {
        let epoch: u64 = first_arg.parse().expect("failed to parse epoch");
        log::info!("epoch: {}", epoch);
        let (start_slot, end_slot) = jetstreamer_firehose::epochs::epoch_to_slot_range(epoch);
        start_slot..end_slot
    };
    let geyser_config_files = &[std::path::PathBuf::from(args().nth(2).unwrap())];
    log::info!("slot index base url: {}", index_base_url);
    log::info!("geyser config files: {:?}", geyser_config_files);
    firehose_geyser(
        Arc::new(tokio::runtime::Runtime::new().unwrap()),
        slot_range,
        Some(geyser_config_files),
        &index_base_url,
        &client,
        async { Ok(()) },
        1,
    )
    .unwrap();
}
