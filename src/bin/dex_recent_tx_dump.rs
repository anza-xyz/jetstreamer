use std::{
    collections::{HashMap, HashSet},
    fs,
    path::PathBuf,
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use solana_message::{
    VersionedMessage, compiled_instruction::CompiledInstruction, v0::LoadedAddresses,
};
use solana_sdk::{pubkey::Pubkey, transaction::VersionedTransaction};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, InnerInstruction, InnerInstructions,
    TransactionStatusMeta, TransactionTokenBalance, UiCompiledInstruction, UiInnerInstructions,
    UiInstruction, UiLoadedAddresses, UiTransactionStatusMeta, UiTransactionTokenBalance,
    option_serializer::OptionSerializer,
};
use spl_token_2022_interface::instruction::TokenInstruction as SplToken2022Instruction;
use spl_token_interface::instruction::TokenInstruction as SplTokenInstruction;
use tokio::{fs as tokio_fs, time::{Instant, sleep}};

static RAYDIUM_CPMM_SWAP_BASE_INPUT: LazyLock<[u8; 8]> =
    LazyLock::new(|| anchor_discriminator("swap_base_input"));
static RAYDIUM_CPMM_SWAP_BASE_OUTPUT: LazyLock<[u8; 8]> =
    LazyLock::new(|| anchor_discriminator("swap_base_output"));
static RAYDIUM_CLMM_SWAP: LazyLock<[u8; 8]> =
    LazyLock::new(|| anchor_discriminator("swap"));
static RAYDIUM_CLMM_SWAP_V2: LazyLock<[u8; 8]> =
    LazyLock::new(|| anchor_discriminator("swap_v2"));

static RAYDIUM_AMM_V4_PROGRAM: LazyLock<Pubkey> =
    LazyLock::new(|| pubkey("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"));
static RAYDIUM_CPMM_PROGRAM: LazyLock<Pubkey> =
    LazyLock::new(|| pubkey("CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C"));
static RAYDIUM_CLMM_PROGRAM: LazyLock<Pubkey> =
    LazyLock::new(|| pubkey("CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK"));
static WHIRLPOOL_AMM_PROGRAM: LazyLock<Pubkey> =
    LazyLock::new(|| pubkey("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"));
static METEORA_DLMM_PROGRAM: LazyLock<Pubkey> =
    LazyLock::new(|| pubkey("LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"));
static METEORA_DAMM_PROGRAM: LazyLock<Pubkey> =
    LazyLock::new(|| pubkey("cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG"));
static PUMP_AMM_PROGRAM: LazyLock<Pubkey> =
    LazyLock::new(|| pubkey("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"));

const RAYDIUM_V4_SWAP_BASE_IN: u8 = 9;
const RAYDIUM_V4_SWAP_MAX_AMOUNT_IN: u8 = 11;

const WHIRLPOOL_SWAP: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];
const WHIRLPOOL_SWAP2: [u8; 8] = [43, 4, 237, 11, 26, 201, 30, 98];

const METEORA_DLMM_SWAP: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];
const METEORA_DLMM_SWAP2: [u8; 8] = [65, 75, 63, 76, 235, 91, 91, 136];
const METEORA_DLMM_SWAP_EXACT_OUT: [u8; 8] = [250, 73, 101, 33, 38, 207, 75, 184];
const METEORA_DLMM_SWAP_EXACT_OUT2: [u8; 8] = [43, 215, 247, 132, 137, 60, 243, 81];
const METEORA_DLMM_SWAP_WITH_PRICE_IMPACT: [u8; 8] = [56, 173, 230, 208, 173, 228, 156, 205];
const METEORA_DLMM_SWAP_WITH_PRICE_IMPACT2: [u8; 8] = [74, 98, 192, 214, 177, 51, 75, 51];

const METEORA_DAMM_SWAP: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];
const METEORA_DAMM_SWAP2: [u8; 8] = [65, 75, 63, 76, 235, 91, 91, 136];
const PUMP_AMM_BUY_DISCRIMINATOR: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
const PUMP_AMM_SELL_DISCRIMINATOR: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];
const PUMP_AMM_BUY_EXACT_QUOTE_IN_DISCRIMINATOR: [u8; 8] = [198, 46, 21, 82, 180, 217, 232, 112];

const MAX_RPC_PAGE_SIZE: usize = 1_000;
const DEFAULT_TX_CONCURRENCY: usize = 4;
const DEFAULT_TX_BATCH_SIZE: usize = 1;
const DEFAULT_TX_BATCH_DELAY_MS: u64 = 50;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
enum Venue {
    RaydiumV4,
    RaydiumCpmm,
    RaydiumClmm,
    Whirlpool,
    MeteoraDlmm,
    MeteoraDamm,
    PumpAmm,
}

impl Venue {
    fn as_str(&self) -> &'static str {
        match self {
            Venue::RaydiumV4 => "RaydiumV4",
            Venue::RaydiumCpmm => "RaydiumCpmm",
            Venue::RaydiumClmm => "RaydiumClmm",
            Venue::Whirlpool => "Whirlpool",
            Venue::MeteoraDlmm => "MeteoraDlmm",
            Venue::MeteoraDamm => "MeteoraDamm",
            Venue::PumpAmm => "PumpAmm",
        }
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "dex_recent_tx_dump",
    about = "Fetch the most recent DEX transactions over RPC, parse swap inner instructions, and emit per-tx JSON."
)]
struct Args {
    #[arg(
        long,
        env = "SOLANA_RPC_URL",
        default_value = "https://solana-rpc.publicnode.com"
    )]
    rpc_url: String,

    #[arg(long, default_value_t = 100_000)]
    per_dex: usize,

    #[arg(long, default_value_t = 100)]
    signature_page_delay_ms: u64,

    #[arg(long)]
    threads: Option<usize>,

    #[arg(long, default_value_t = DEFAULT_TX_BATCH_SIZE)]
    tx_batch_size: usize,

    #[arg(long, default_value_t = DEFAULT_TX_BATCH_DELAY_MS)]
    tx_batch_delay_ms: u64,

    #[arg(long)]
    out_dir: Option<PathBuf>,

    #[arg(long, default_value_t = 1_000)]
    progress_every: usize,
}

#[derive(Debug, Clone)]
struct DexTarget {
    slug: &'static str,
    venue: Venue,
    program_id: Pubkey,
}

#[derive(Debug, Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

#[derive(Debug, Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Debug, Deserialize)]
struct RpcBatchResponse {
    id: u64,
    result: Option<serde_json::Value>,
    error: Option<RpcError>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct SignatureRecord {
    signature: String,
    slot: u64,
    err: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
struct FetchedDexHistory {
    signatures: Vec<SignatureRecord>,
}

#[derive(Debug)]
struct SharedState {
    output_root: PathBuf,
    targets: Vec<DexTarget>,
    signature_to_targets: HashMap<String, Vec<usize>>,
    total_target_pairs: usize,
    total_signatures: usize,
    progress_every: usize,
    progress: Mutex<ProgressState>,
}

#[derive(Debug, Default)]
struct ProgressState {
    per_target: Vec<TargetProgress>,
    completed_target_pairs: usize,
    processed_signatures: usize,
    missing_transactions: usize,
    last_logged_processed_signatures: usize,
}

#[derive(Debug, Default, Clone, Serialize)]
struct TargetProgress {
    fetched_signatures: usize,
    written_files: usize,
    parse_misses: usize,
}

#[derive(Debug, Clone)]
struct ParsedSwapHint {
    venue: Venue,
    pool_address: Pubkey,
    signer: Pubkey,
    user_a: Pubkey,
    user_b: Pubkey,
    vault_a: Pubkey,
    vault_b: Pubkey,
    user_input_hint: Option<Pubkey>,
    user_output_hint: Option<Pubkey>,
    vault_input_hint: Option<Pubkey>,
    vault_output_hint: Option<Pubkey>,
    instruction_amount_hint: Option<u64>,
    instruction_scope: InstructionScope,
    outer_instruction_index: usize,
    inner_instruction_index: Option<usize>,
    stack_height: Option<u32>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum InstructionScope {
    TopLevel,
    Inner,
}

#[derive(Debug, Clone)]
struct TokenBalanceContext {
    pre_by_account: HashMap<Pubkey, TokenBalanceSnapshot>,
    post_by_account: HashMap<Pubkey, TokenBalanceSnapshot>,
}

#[derive(Debug, Clone)]
struct TokenBalanceSnapshot {
    mint: Pubkey,
    owner: String,
    program_id: String,
    raw_amount: u64,
    decimals: u8,
}

#[derive(Debug, Clone, Serialize)]
struct ParsedInnerTransfer {
    instruction_group_index: u8,
    inner_instruction_index: usize,
    stack_height: Option<u32>,
    token_program: String,
    instruction_kind: String,
    source: String,
    destination: String,
    authority: Option<String>,
    mint: Option<String>,
    decimals: Option<u8>,
    amount_raw: String,
    amount_ui: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct TxVenueDump {
    signature: String,
    slot: u64,
    transaction_index: Option<usize>,
    is_vote: bool,
    status: String,
    venue: String,
    target_program: String,
    swap_count: usize,
    swaps: Vec<ResolvedSwap>,
    related_inner_transfers: Vec<ParsedInnerTransfer>,
}

#[derive(Debug, Clone, Serialize)]
struct ResolvedSwap {
    #[serde(skip_serializing)]
    venue_slug: String,
    instruction_scope: InstructionScope,
    outer_instruction_index: usize,
    inner_instruction_index: Option<usize>,
    stack_height: Option<u32>,
    pool_address: String,
    signer: String,
    user_input_account: Option<String>,
    user_output_account: Option<String>,
    vault_input_account: Option<String>,
    vault_output_account: Option<String>,
    input_token: Option<TokenEndpoint>,
    output_token: Option<TokenEndpoint>,
    instruction_amount_hint_raw: Option<String>,
    amounts: AmountSummary,
    balance_deltas: BalanceDeltaSummary,
    matched_input_transfers: Vec<ParsedInnerTransfer>,
    matched_output_transfers: Vec<ParsedInnerTransfer>,
}

#[derive(Debug, Clone, Serialize)]
struct TokenEndpoint {
    account: String,
    mint: String,
    owner: String,
    program_id: String,
    decimals: u8,
}

#[derive(Debug, Clone, Serialize, Default)]
struct AmountSummary {
    input_raw: Option<String>,
    input_ui: Option<String>,
    output_raw: Option<String>,
    output_ui: Option<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
struct BalanceDeltaSummary {
    user_input_raw: Option<String>,
    user_output_raw: Option<String>,
    vault_input_raw: Option<String>,
    vault_output_raw: Option<String>,
}

#[derive(Debug, Serialize)]
struct RunSummary {
    rpc_url: String,
    per_dex_requested: usize,
    unique_signatures: usize,
    missing_transactions: usize,
    output_root: String,
    slot_range_start: u64,
    slot_range_end_inclusive: u64,
    started_at_unix_ms: u128,
    finished_at_unix_ms: u128,
    duration_seconds: f64,
    per_target: Vec<RunSummaryTarget>,
}

#[derive(Debug, Serialize)]
struct RunSummaryTarget {
    venue: String,
    program_id: String,
    fetched_signatures: usize,
    written_files: usize,
    parse_misses: usize,
}

#[derive(Debug)]
struct TargetedTransaction {
    slot: u64,
    transaction_index: Option<usize>,
    signature: String,
    is_vote: bool,
    transaction_status_meta: TransactionStatusMeta,
    transaction: VersionedTransaction,
}

#[tokio::main]
async fn main() -> Result<()> {
    solana_logger::setup_with_default("info");
    let args = Args::parse();
    let started_at = SystemTime::now();
    let started_instant = Instant::now();
    let targets = dex_targets();
    let output_root = resolve_output_root(args.out_dir.clone())?;

    fs::create_dir_all(&output_root)
        .with_context(|| format!("failed to create {}", output_root.display()))?;
    for target in &targets {
        fs::create_dir_all(output_root.join(target.slug)).with_context(|| {
            format!(
                "failed to create venue output directory {}",
                output_root.join(target.slug).display()
            )
        })?;
    }

    log::info!(
        "fetching {} recent signatures for {} DEX targets via {}",
        args.per_dex,
        targets.len(),
        args.rpc_url
    );
    let client = reqwest::Client::builder()
        .user_agent("jetstreamer-dex-recent-tx-dump/1.0")
        .build()
        .context("failed to build reqwest client")?;

    let mut fetched_histories = Vec::with_capacity(targets.len());
    for (index, target) in targets.iter().enumerate() {
        let signatures = fetch_recent_signatures(
            &client,
            &args.rpc_url,
            target.clone(),
            args.per_dex,
            Duration::from_millis(args.signature_page_delay_ms),
        )
        .await
        .with_context(|| format!("failed to fetch recent signatures for {}", target.slug))?;
        log::info!(
            "fetched {} signature(s) for {} ({})",
            signatures.len(),
            target.slug,
            target.program_id
        );
        fetched_histories.push(FetchedDexHistory {
            signatures,
        });
        if index + 1 < targets.len() {
            sleep(Duration::from_secs(2)).await;
        }
    }

    let (signature_to_targets, ordered_signatures, slot_range_start, slot_range_end_inclusive) =
        build_lookup_tables(&fetched_histories)?;
    log::info!(
        "processing {} unique signatures across slots {}..={} to resolve {} targeted signature/venue pairs",
        ordered_signatures.len(),
        slot_range_start,
        slot_range_end_inclusive,
        signature_to_targets.values().map(Vec::len).sum::<usize>()
    );

    let shared = Arc::new(SharedState {
        output_root: output_root.clone(),
        targets: targets.clone(),
        total_target_pairs: signature_to_targets.values().map(Vec::len).sum(),
        total_signatures: ordered_signatures.len(),
        progress_every: args.progress_every.max(1),
        signature_to_targets,
        progress: Mutex::new(ProgressState {
            per_target: fetched_histories
                .iter()
                .map(|history| TargetProgress {
                    fetched_signatures: history.signatures.len(),
                    written_files: 0,
                    parse_misses: 0,
                })
                .collect(),
            completed_target_pairs: 0,
            processed_signatures: 0,
            missing_transactions: 0,
            last_logged_processed_signatures: 0,
        }),
    });

    fetch_transactions_by_signature(
        client,
        &args.rpc_url,
        shared.clone(),
        ordered_signatures,
        args.threads.unwrap_or(DEFAULT_TX_CONCURRENCY).max(1),
        args.tx_batch_size.max(1),
        Duration::from_millis(args.tx_batch_delay_ms),
    )
    .await?;

    let finished_at = SystemTime::now();
    let duration = started_instant.elapsed();
    let summary = build_summary(
        &args,
        &shared,
        slot_range_start,
        slot_range_end_inclusive,
        started_at,
        finished_at,
        duration,
    )?;

    let summary_path = output_root.join("summary.json");
    tokio_fs::write(&summary_path, serde_json::to_vec_pretty(&summary)?)
        .await
        .with_context(|| format!("failed to write {}", summary_path.display()))?;

    log::info!("summary written to {}", summary_path.display());
    println!("{}", summary_path.display());
    Ok(())
}

fn dex_targets() -> Vec<DexTarget> {
    vec![
        DexTarget {
            slug: "raydium_v4",
            venue: Venue::RaydiumV4,
            program_id: *RAYDIUM_AMM_V4_PROGRAM,
        },
        DexTarget {
            slug: "raydium_cpmm",
            venue: Venue::RaydiumCpmm,
            program_id: *RAYDIUM_CPMM_PROGRAM,
        },
        DexTarget {
            slug: "raydium_clmm",
            venue: Venue::RaydiumClmm,
            program_id: *RAYDIUM_CLMM_PROGRAM,
        },
        DexTarget {
            slug: "orca_whirlpool",
            venue: Venue::Whirlpool,
            program_id: *WHIRLPOOL_AMM_PROGRAM,
        },
        DexTarget {
            slug: "meteora_dlmm",
            venue: Venue::MeteoraDlmm,
            program_id: *METEORA_DLMM_PROGRAM,
        },
        DexTarget {
            slug: "meteora_damm",
            venue: Venue::MeteoraDamm,
            program_id: *METEORA_DAMM_PROGRAM,
        },
        DexTarget {
            slug: "pump_amm",
            venue: Venue::PumpAmm,
            program_id: *PUMP_AMM_PROGRAM,
        },
    ]
}

fn resolve_output_root(path: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(path) = path {
        return Ok(path);
    }
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_secs();
    Ok(PathBuf::from(format!("artifacts/dex_recent_tx_dump_{}", ts)))
}

async fn fetch_recent_signatures(
    client: &reqwest::Client,
    rpc_url: &str,
    target: DexTarget,
    wanted: usize,
    page_delay: Duration,
) -> Result<Vec<SignatureRecord>> {
    let mut before: Option<String> = None;
    let mut collected = Vec::with_capacity(wanted);

    while collected.len() < wanted {
        let limit = (wanted - collected.len()).min(MAX_RPC_PAGE_SIZE);
        let mut params = serde_json::json!([
            target.program_id.to_string(),
            {
                "limit": limit,
                "commitment": "confirmed",
            }
        ]);
        if let Some(ref before_sig) = before {
            params[1]["before"] = serde_json::json!(before_sig);
        }

        let payload: RpcResponse<Vec<SignatureRecord>> = rpc_post_with_retry(
            client,
            rpc_url,
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getSignaturesForAddress",
                "params": params,
            }),
            &target.program_id.to_string(),
        )
        .await?;

        if let Some(error) = payload.error {
            return Err(anyhow!(
                "RPC error {} for {}: {}",
                error.code,
                target.program_id,
                error.message
            ));
        }

        let page = payload.result.unwrap_or_default();
        if page.is_empty() {
            break;
        }

        before = page.last().map(|item| item.signature.clone());
        collected.extend(page.into_iter());

        if collected.len() < wanted {
            sleep(page_delay).await;
        }
    }

    collected.truncate(wanted);
    Ok(collected)
}

async fn rpc_post_with_retry<T>(
    client: &reqwest::Client,
    rpc_url: &str,
    body: serde_json::Value,
    label: &str,
) -> Result<T>
where
    T: DeserializeOwned,
{
    let mut attempt: u32 = 0;
    loop {
        let response = client
            .post(rpc_url)
            .json(&body)
            .send()
            .await
            .with_context(|| format!("rpc request failed for {label}"))?;
        let status = response.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
            let body_text = response.text().await.unwrap_or_default();
            if attempt >= 8 {
                return Err(anyhow!(
                    "RPC {} after {} attempt(s) for {}: {}",
                    status,
                    attempt + 1,
                    label,
                    body_text
                ));
            }
            let backoff_secs = 2u64.saturating_pow(attempt.min(5));
            log::warn!(
                "RPC {} for {} attempt {}. backing off {}s",
                status,
                label,
                attempt + 1,
                backoff_secs
            );
            sleep(Duration::from_secs(backoff_secs)).await;
            attempt += 1;
            continue;
        }

        let response = response.error_for_status()?;
        return response
            .json()
            .await
            .context("failed to decode RPC response");
    }
}

fn build_lookup_tables(
    fetched: &[FetchedDexHistory],
) -> Result<(HashMap<String, Vec<usize>>, Vec<String>, u64, u64)> {
    let mut signature_to_targets = HashMap::<String, Vec<usize>>::new();
    let mut signature_slots = HashMap::<String, u64>::new();
    let mut min_slot: Option<u64> = None;
    let mut max_slot: Option<u64> = None;

    for (target_idx, history) in fetched.iter().enumerate() {
        for record in &history.signatures {
            signature_to_targets
                .entry(record.signature.clone())
                .or_default()
                .push(target_idx);
            signature_slots
                .entry(record.signature.clone())
                .and_modify(|slot| *slot = (*slot).max(record.slot))
                .or_insert(record.slot);
            min_slot = Some(min_slot.map(|slot| slot.min(record.slot)).unwrap_or(record.slot));
            max_slot = Some(max_slot.map(|slot| slot.max(record.slot)).unwrap_or(record.slot));
        }
    }

    let start = min_slot.ok_or_else(|| anyhow!("no signatures were fetched"))?;
    let end = max_slot.ok_or_else(|| anyhow!("no signatures were fetched"))?;
    let mut ordered_signatures = signature_slots.into_iter().collect::<Vec<_>>();
    ordered_signatures.sort_unstable_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(&right.0)));
    Ok((
        signature_to_targets,
        ordered_signatures
            .into_iter()
            .map(|(signature, _slot)| signature)
            .collect(),
        start,
        end,
    ))
}

async fn fetch_transactions_by_signature(
    client: reqwest::Client,
    rpc_url: &str,
    shared: Arc<SharedState>,
    signatures: Vec<String>,
    concurrency: usize,
    batch_size: usize,
    batch_delay: Duration,
) -> Result<()> {
    let signatures = Arc::new(signatures);
    let next_batch_start = Arc::new(AtomicUsize::new(0));
    let mut workers = Vec::with_capacity(concurrency);

    for worker_id in 0..concurrency {
        let client = client.clone();
        let rpc_url = rpc_url.to_string();
        let shared = shared.clone();
        let signatures = signatures.clone();
        let next_batch_start = next_batch_start.clone();
        workers.push(tokio::spawn(async move {
            loop {
                let start = next_batch_start.fetch_add(batch_size, Ordering::Relaxed);
                if start >= signatures.len() {
                    break;
                }
                let end = (start + batch_size).min(signatures.len());
                process_signature_batch(
                    &client,
                    &rpc_url,
                    shared.clone(),
                    &signatures[start..end],
                    worker_id,
                    start,
                    end,
                )
                .await?;

                if !batch_delay.is_zero() && end < signatures.len() {
                    sleep(batch_delay).await;
                }
            }

            Ok::<(), anyhow::Error>(())
        }));
    }

    for worker in workers {
        worker.await.context("transaction worker panicked")??;
    }

    Ok(())
}

async fn process_signature_batch(
    client: &reqwest::Client,
    rpc_url: &str,
    shared: Arc<SharedState>,
    signatures: &[String],
    worker_id: usize,
    start: usize,
    end: usize,
) -> Result<()> {
    let batch = fetch_transaction_batch(client, rpc_url, signatures, worker_id, start, end).await?;

    for (signature, encoded_tx) in signatures.iter().zip(batch.into_iter()) {
        let Some(encoded_tx) = encoded_tx else {
            complete_signature_as_miss(
                &shared,
                signature,
                true,
                "transaction missing from RPC getTransaction response",
            )?;
            continue;
        };

        let tx = match decode_rpc_transaction(signature, encoded_tx) {
            Ok(tx) => tx,
            Err(err) => {
                log::warn!("failed to decode transaction {}: {}", signature, err);
                complete_signature_as_miss(
                    &shared,
                    signature,
                    true,
                    "transaction could not be decoded from RPC payload",
                )?;
                continue;
            }
        };

        handle_targeted_transaction(shared.clone(), tx).await?;
    }

    log::debug!("worker {} processed signatures [{}..{})", worker_id, start, end);
    Ok(())
}

async fn fetch_transaction_batch(
    client: &reqwest::Client,
    rpc_url: &str,
    signatures: &[String],
    worker_id: usize,
    start: usize,
    end: usize,
) -> Result<Vec<Option<EncodedConfirmedTransactionWithStatusMeta>>> {
    let requests = signatures
        .iter()
        .enumerate()
        .map(|(index, signature)| {
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": index,
                "method": "getTransaction",
                "params": [
                    signature,
                    {
                        "encoding": "base64",
                        "commitment": "confirmed",
                        "maxSupportedTransactionVersion": 0
                    }
                ],
            })
        })
        .collect::<Vec<_>>();

    let label = format!("getTransaction batch worker={} range=[{}..{})", worker_id, start, end);
    let responses: Vec<RpcBatchResponse> =
        rpc_post_with_retry(client, rpc_url, serde_json::Value::Array(requests), &label).await?;

    let mut aligned = (0..signatures.len()).map(|_| None).collect::<Vec<_>>();
    for response in responses {
        let response_index = response.id as usize;
        let Some(signature) = signatures.get(response_index) else {
            log::warn!(
                "ignoring batch response with out-of-range id {} for worker {} [{}..{})",
                response.id,
                worker_id,
                start,
                end
            );
            continue;
        };

        if let Some(error) = response.error {
            log::warn!(
                "RPC getTransaction error {} for {}: {}",
                error.code,
                signature,
                error.message
            );
            continue;
        }

        let Some(value) = response.result else {
            continue;
        };
        if value.is_null() {
            continue;
        }

        aligned[response_index] = Some(
            serde_json::from_value(value).with_context(|| {
                format!(
                    "failed to deserialize getTransaction response for {}",
                    signature
                )
            })?,
        );
    }

    Ok(aligned)
}

async fn handle_targeted_transaction(shared: Arc<SharedState>, tx: TargetedTransaction) -> Result<()> {
    let signature = tx.signature.clone();
    let Some(target_indices) = shared.signature_to_targets.get(&signature).cloned() else {
        return Ok(());
    };

    let parsed = match parse_targeted_transaction(&tx) {
        Ok(parsed) => parsed,
        Err(err) => {
            log::warn!("failed to parse targeted transaction {}: {}", signature, err);
            complete_signature_as_miss(
                &shared,
                &signature,
                false,
                "transaction failed DEX parsing",
            )?;
            return Ok(());
        }
    };
    let by_venue = group_swaps_by_venue(&parsed.swaps);

    for target_idx in target_indices {
        let target = shared
            .targets
            .get(target_idx)
            .ok_or_else(|| anyhow!("missing target index {}", target_idx))?;
        let Some(swaps) = by_venue.get(target.slug) else {
            let mut progress = shared
                .progress
                .lock()
                .map_err(|_| anyhow!("progress mutex poisoned"))?;
            progress.per_target[target_idx].parse_misses += 1;
            progress.completed_target_pairs += 1;
            continue;
        };

        let related_transfers = related_transfers_for_swaps(swaps, &parsed.inner_transfers);
        let output = TxVenueDump {
            signature: signature.clone(),
            slot: tx.slot,
            transaction_index: tx.transaction_index,
            is_vote: tx.is_vote,
            status: format!("{:?}", tx.transaction_status_meta.status),
            venue: target.venue.as_str().to_string(),
            target_program: target.program_id.to_string(),
            swap_count: swaps.len(),
            swaps: swaps.clone(),
            related_inner_transfers: related_transfers,
        };

        let file_path = shared
            .output_root
            .join(target.slug)
            .join(format!("{}.json", signature));
        let payload = serde_json::to_vec_pretty(&output)
            .with_context(|| format!("failed to serialize output for {}", file_path.display()))?;
        tokio_fs::write(&file_path, payload)
            .await
            .with_context(|| format!("failed to write {}", file_path.display()))?;

        let mut progress = shared
            .progress
            .lock()
            .map_err(|_| anyhow!("progress mutex poisoned"))?;
        progress.per_target[target_idx].written_files += 1;
        progress.completed_target_pairs += 1;
    }

    let mut progress = shared
        .progress
        .lock()
        .map_err(|_| anyhow!("progress mutex poisoned"))?;
    progress.processed_signatures += 1;
    maybe_log_progress(&shared, &mut progress);

    Ok(())
}

fn complete_signature_as_miss(
    shared: &SharedState,
    signature: &str,
    missing_transaction: bool,
    reason: &str,
) -> Result<()> {
    log::warn!("{}: {}", reason, signature);
    let Some(target_indices) = shared.signature_to_targets.get(signature) else {
        return Ok(());
    };

    let mut progress = shared
        .progress
        .lock()
        .map_err(|_| anyhow!("progress mutex poisoned"))?;
    for target_idx in target_indices {
        progress.per_target[*target_idx].parse_misses += 1;
        progress.completed_target_pairs += 1;
    }
    progress.processed_signatures += 1;
    if missing_transaction {
        progress.missing_transactions += 1;
    }
    maybe_log_progress(shared, &mut progress);
    Ok(())
}

fn maybe_log_progress(shared: &SharedState, progress: &mut ProgressState) {
    let should_log = progress.processed_signatures == shared.total_signatures
        || progress.processed_signatures
            >= progress
                .last_logged_processed_signatures
                .saturating_add(shared.progress_every);
    if !should_log {
        return;
    }

    progress.last_logged_processed_signatures = progress.processed_signatures;
    let written_files: usize = progress.per_target.iter().map(|item| item.written_files).sum();
    log::info!(
        "rpc progress: signatures={}/{} targeted_pairs={}/{} files_written={} missing_transactions={}",
        progress.processed_signatures,
        shared.total_signatures,
        progress.completed_target_pairs,
        shared.total_target_pairs,
        written_files,
        progress.missing_transactions
    );
}

#[derive(Debug)]
struct ParsedTransaction {
    swaps: Vec<ResolvedSwap>,
    inner_transfers: Vec<ParsedInnerTransfer>,
}

fn parse_targeted_transaction(tx: &TargetedTransaction) -> Result<ParsedTransaction> {
    let account_keys = build_account_keys(&tx.transaction.message, &tx.transaction_status_meta);
    let balances = build_token_balance_context(&account_keys, &tx.transaction_status_meta);
    let inner_transfers = parse_inner_transfers(
        &account_keys,
        tx.transaction_status_meta.inner_instructions.as_deref(),
        &balances,
    );
    let mut swaps = Vec::new();

    match &tx.transaction.message {
        VersionedMessage::Legacy(message) => {
            for (outer_index, instruction) in message.instructions.iter().enumerate() {
                if let Some(hint) = parse_swap_hint(
                    &account_keys,
                    instruction.program_id_index as usize,
                    &instruction.accounts,
                    &instruction.data,
                    InstructionScope::TopLevel,
                    outer_index,
                    None,
                    None,
                ) {
                    swaps.push(resolve_swap(hint, &balances, &inner_transfers));
                }
            }
        }
        VersionedMessage::V0(message) => {
            for (outer_index, instruction) in message.instructions.iter().enumerate() {
                if let Some(hint) = parse_swap_hint(
                    &account_keys,
                    instruction.program_id_index as usize,
                    &instruction.accounts,
                    &instruction.data,
                    InstructionScope::TopLevel,
                    outer_index,
                    None,
                    None,
                ) {
                    swaps.push(resolve_swap(hint, &balances, &inner_transfers));
                }
            }
        }
    }

    if let Some(inner_groups) = tx.transaction_status_meta.inner_instructions.as_ref() {
        for group in inner_groups {
            for (inner_index, inner_instruction) in group.instructions.iter().enumerate() {
                let CompiledInstruction {
                    program_id_index,
                    ref accounts,
                    ref data,
                } = inner_instruction.instruction;
                if let Some(hint) = parse_swap_hint(
                    &account_keys,
                    program_id_index as usize,
                    accounts,
                    data,
                    InstructionScope::Inner,
                    group.index as usize,
                    Some(inner_index),
                    inner_instruction.stack_height,
                ) {
                    swaps.push(resolve_swap(hint, &balances, &inner_transfers));
                }
            }
        }
    }

    Ok(ParsedTransaction {
        swaps,
        inner_transfers,
    })
}

fn decode_rpc_transaction(
    signature: &str,
    encoded: EncodedConfirmedTransactionWithStatusMeta,
) -> Result<TargetedTransaction> {
    let transaction = encoded
        .transaction
        .transaction
        .decode()
        .ok_or_else(|| anyhow!("failed to decode base64 transaction body"))?;
    let meta = encoded
        .transaction
        .meta
        .ok_or_else(|| anyhow!("missing transaction metadata"))?;

    Ok(TargetedTransaction {
        slot: encoded.slot,
        transaction_index: None,
        signature: signature.to_string(),
        is_vote: false,
        transaction_status_meta: decode_ui_transaction_status_meta(meta)?,
        transaction,
    })
}

fn decode_ui_transaction_status_meta(meta: UiTransactionStatusMeta) -> Result<TransactionStatusMeta> {
    let inner_instructions = match meta.inner_instructions {
        OptionSerializer::Some(groups) => Some(
            groups
                .into_iter()
                .map(decode_ui_inner_instructions)
                .collect::<Result<Vec<_>>>()?,
        ),
        OptionSerializer::None | OptionSerializer::Skip => None,
    };

    let pre_token_balances = decode_ui_token_balances(meta.pre_token_balances);
    let post_token_balances = decode_ui_token_balances(meta.post_token_balances);
    let loaded_addresses = match meta.loaded_addresses {
        OptionSerializer::Some(addresses) => decode_ui_loaded_addresses(addresses)?,
        OptionSerializer::None | OptionSerializer::Skip => LoadedAddresses::default(),
    };

    Ok(TransactionStatusMeta {
        status: meta.status.map_err(Into::into),
        fee: meta.fee,
        pre_balances: meta.pre_balances,
        post_balances: meta.post_balances,
        inner_instructions,
        log_messages: meta.log_messages.into(),
        pre_token_balances,
        post_token_balances,
        rewards: meta.rewards.into(),
        loaded_addresses,
        return_data: None,
        compute_units_consumed: meta.compute_units_consumed.into(),
        cost_units: meta.cost_units.into(),
    })
}

fn decode_ui_inner_instructions(group: UiInnerInstructions) -> Result<InnerInstructions> {
    let mut instructions = Vec::with_capacity(group.instructions.len());
    for instruction in group.instructions {
        let UiInstruction::Compiled(compiled) = instruction else {
            continue;
        };
        let stack_height = compiled.stack_height;
        instructions.push(InnerInstruction {
            instruction: decode_ui_compiled_instruction(compiled)?,
            stack_height,
        });
    }

    Ok(InnerInstructions {
        index: group.index,
        instructions,
    })
}

fn decode_ui_compiled_instruction(instruction: UiCompiledInstruction) -> Result<CompiledInstruction> {
    Ok(CompiledInstruction {
        program_id_index: instruction.program_id_index,
        accounts: instruction.accounts,
        data: bs58::decode(instruction.data)
            .into_vec()
            .context("failed to decode base58 instruction data")?,
    })
}

fn decode_ui_token_balances(
    balances: OptionSerializer<Vec<UiTransactionTokenBalance>>,
) -> Option<Vec<TransactionTokenBalance>> {
    match balances {
        OptionSerializer::Some(balances) => Some(
            balances
                .into_iter()
                .map(|balance| TransactionTokenBalance {
                    account_index: balance.account_index,
                    mint: balance.mint,
                    ui_token_amount: balance.ui_token_amount,
                    owner: match balance.owner {
                        OptionSerializer::Some(owner) => owner,
                        OptionSerializer::None | OptionSerializer::Skip => String::new(),
                    },
                    program_id: match balance.program_id {
                        OptionSerializer::Some(program_id) => program_id,
                        OptionSerializer::None | OptionSerializer::Skip => String::new(),
                    },
                })
                .collect(),
        ),
        OptionSerializer::None | OptionSerializer::Skip => None,
    }
}

fn decode_ui_loaded_addresses(addresses: UiLoadedAddresses) -> Result<LoadedAddresses> {
    Ok(LoadedAddresses {
        writable: addresses
            .writable
            .into_iter()
            .map(|value| {
                value
                    .parse::<Pubkey>()
                    .with_context(|| format!("invalid writable loaded address {}", value))
            })
            .collect::<Result<Vec<_>>>()?,
        readonly: addresses
            .readonly
            .into_iter()
            .map(|value| {
                value
                    .parse::<Pubkey>()
                    .with_context(|| format!("invalid readonly loaded address {}", value))
            })
            .collect::<Result<Vec<_>>>()?,
    })
}

fn build_account_keys(message: &VersionedMessage, meta: &TransactionStatusMeta) -> Vec<Pubkey> {
    let static_keys: Vec<Pubkey> = match message {
        VersionedMessage::Legacy(legacy) => legacy
            .account_keys
            .iter()
            .map(address_to_pubkey)
            .collect(),
        VersionedMessage::V0(v0) => v0.account_keys.iter().map(address_to_pubkey).collect(),
    };

    let mut keys = Vec::with_capacity(
        static_keys.len()
            + meta.loaded_addresses.writable.len()
            + meta.loaded_addresses.readonly.len(),
    );
    keys.extend(static_keys);
    keys.extend(meta.loaded_addresses.writable.iter().map(address_to_pubkey));
    keys.extend(meta.loaded_addresses.readonly.iter().map(address_to_pubkey));
    keys
}

fn address_to_pubkey<T: AsRef<[u8]>>(address: &T) -> Pubkey {
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(address.as_ref());
    Pubkey::new_from_array(bytes)
}

fn build_token_balance_context(
    account_keys: &[Pubkey],
    meta: &TransactionStatusMeta,
) -> TokenBalanceContext {
    TokenBalanceContext {
        pre_by_account: build_balance_map(account_keys, meta.pre_token_balances.as_deref()),
        post_by_account: build_balance_map(account_keys, meta.post_token_balances.as_deref()),
    }
}

fn build_balance_map(
    account_keys: &[Pubkey],
    balances: Option<&[TransactionTokenBalance]>,
) -> HashMap<Pubkey, TokenBalanceSnapshot> {
    let mut map = HashMap::new();
    let Some(balances) = balances else {
        return map;
    };
    for balance in balances {
        let index = balance.account_index as usize;
        let Some(account) = account_keys.get(index) else {
            continue;
        };
        let Ok(mint) = balance.mint.parse::<Pubkey>() else {
            continue;
        };
        let Ok(raw_amount) = balance.ui_token_amount.amount.parse::<u64>() else {
            continue;
        };
        map.insert(
            *account,
            TokenBalanceSnapshot {
                mint,
                owner: balance.owner.clone(),
                program_id: balance.program_id.clone(),
                raw_amount,
                decimals: balance.ui_token_amount.decimals,
            },
        );
    }
    map
}

fn parse_inner_transfers(
    account_keys: &[Pubkey],
    inner_groups: Option<&[InnerInstructions]>,
    balances: &TokenBalanceContext,
) -> Vec<ParsedInnerTransfer> {
    let Some(inner_groups) = inner_groups else {
        return Vec::new();
    };
    let mut parsed = Vec::new();
    for group in inner_groups {
        for (inner_index, InnerInstruction { instruction, stack_height }) in
            group.instructions.iter().enumerate()
        {
            let Some(program_id) = account_keys.get(instruction.program_id_index as usize).copied()
            else {
                continue;
            };
            if program_id == spl_token_interface::id() {
                if let Some(transfer) = parse_spl_token_transfer(
                    account_keys,
                    balances,
                    group.index,
                    inner_index,
                    *stack_height,
                    instruction,
                ) {
                    parsed.push(transfer);
                }
            } else if program_id == spl_token_2022_interface::id()
                && let Some(transfer) = parse_spl_token_2022_transfer(
                    account_keys,
                    balances,
                    group.index,
                    inner_index,
                    *stack_height,
                    instruction,
                )
            {
                parsed.push(transfer);
            }
        }
    }
    parsed
}

fn parse_spl_token_transfer(
    account_keys: &[Pubkey],
    balances: &TokenBalanceContext,
    group_index: u8,
    inner_index: usize,
    stack_height: Option<u32>,
    instruction: &CompiledInstruction,
) -> Option<ParsedInnerTransfer> {
    let source = resolve_ix_account(account_keys, &instruction.accounts, 0)?;
    let decoded = SplTokenInstruction::unpack(&instruction.data).ok()?;
    match decoded {
        SplTokenInstruction::Transfer { amount } => {
            let destination = resolve_ix_account(account_keys, &instruction.accounts, 1)?;
            let authority = resolve_ix_account(account_keys, &instruction.accounts, 2);
            let token_meta = token_meta_for_account(balances, &source)
                .or_else(|| token_meta_for_account(balances, &destination));
            Some(build_transfer_record(
                group_index,
                inner_index,
                stack_height,
                "spl_token",
                "transfer",
                source,
                destination,
                authority,
                token_meta,
                amount,
            ))
        }
        SplTokenInstruction::TransferChecked { amount, decimals } => {
            let mint = resolve_ix_account(account_keys, &instruction.accounts, 1)?;
            let destination = resolve_ix_account(account_keys, &instruction.accounts, 2)?;
            let authority = resolve_ix_account(account_keys, &instruction.accounts, 3);
            Some(build_transfer_record(
                group_index,
                inner_index,
                stack_height,
                "spl_token",
                "transfer_checked",
                source,
                destination,
                authority,
                Some(TokenBalanceSnapshot {
                    mint,
                    owner: String::new(),
                    program_id: spl_token_interface::id().to_string(),
                    raw_amount: 0,
                    decimals,
                }),
                amount,
            ))
        }
        _ => None,
    }
}

#[allow(deprecated)]
fn parse_spl_token_2022_transfer(
    account_keys: &[Pubkey],
    balances: &TokenBalanceContext,
    group_index: u8,
    inner_index: usize,
    stack_height: Option<u32>,
    instruction: &CompiledInstruction,
) -> Option<ParsedInnerTransfer> {
    let source = resolve_ix_account(account_keys, &instruction.accounts, 0)?;
    let decoded = SplToken2022Instruction::unpack(&instruction.data).ok()?;
    match decoded {
        SplToken2022Instruction::Transfer { amount } => {
            let destination = resolve_ix_account(account_keys, &instruction.accounts, 1)?;
            let authority = resolve_ix_account(account_keys, &instruction.accounts, 2);
            let token_meta = token_meta_for_account(balances, &source)
                .or_else(|| token_meta_for_account(balances, &destination));
            Some(build_transfer_record(
                group_index,
                inner_index,
                stack_height,
                "spl_token_2022",
                "transfer",
                source,
                destination,
                authority,
                token_meta,
                amount,
            ))
        }
        SplToken2022Instruction::TransferChecked { amount, decimals } => {
            let mint = resolve_ix_account(account_keys, &instruction.accounts, 1)?;
            let destination = resolve_ix_account(account_keys, &instruction.accounts, 2)?;
            let authority = resolve_ix_account(account_keys, &instruction.accounts, 3);
            Some(build_transfer_record(
                group_index,
                inner_index,
                stack_height,
                "spl_token_2022",
                "transfer_checked",
                source,
                destination,
                authority,
                Some(TokenBalanceSnapshot {
                    mint,
                    owner: String::new(),
                    program_id: spl_token_2022_interface::id().to_string(),
                    raw_amount: 0,
                    decimals,
                }),
                amount,
            ))
        }
        _ => None,
    }
}

fn build_transfer_record(
    group_index: u8,
    inner_index: usize,
    stack_height: Option<u32>,
    token_program: &str,
    instruction_kind: &str,
    source: Pubkey,
    destination: Pubkey,
    authority: Option<Pubkey>,
    token_meta: Option<TokenBalanceSnapshot>,
    amount: u64,
) -> ParsedInnerTransfer {
    let (mint, decimals, amount_ui) = token_meta
        .as_ref()
        .map(|meta| {
            (
                Some(meta.mint.to_string()),
                Some(meta.decimals),
                Some(format_token_amount(amount, meta.decimals)),
            )
        })
        .unwrap_or((None, None, None));

    ParsedInnerTransfer {
        instruction_group_index: group_index,
        inner_instruction_index: inner_index,
        stack_height,
        token_program: token_program.to_string(),
        instruction_kind: instruction_kind.to_string(),
        source: source.to_string(),
        destination: destination.to_string(),
        authority: authority.map(|value| value.to_string()),
        mint,
        decimals,
        amount_raw: amount.to_string(),
        amount_ui,
    }
}

fn parse_swap_hint(
    account_keys: &[Pubkey],
    program_id_index: usize,
    accounts: &[u8],
    data: &[u8],
    instruction_scope: InstructionScope,
    outer_instruction_index: usize,
    inner_instruction_index: Option<usize>,
    stack_height: Option<u32>,
) -> Option<ParsedSwapHint> {
    let program_id = *account_keys.get(program_id_index)?;
    let mut hint = if program_id == *RAYDIUM_AMM_V4_PROGRAM {
        parse_raydium_v4(account_keys, accounts, data)?
    } else if program_id == *RAYDIUM_CPMM_PROGRAM {
        parse_raydium_cpmm(account_keys, accounts, data)?
    } else if program_id == *RAYDIUM_CLMM_PROGRAM {
        parse_raydium_clmm(account_keys, accounts, data)?
    } else if program_id == *WHIRLPOOL_AMM_PROGRAM {
        parse_whirlpool(account_keys, accounts, data)?
    } else if program_id == *METEORA_DLMM_PROGRAM {
        parse_meteora_dlmm(account_keys, accounts, data)?
    } else if program_id == *METEORA_DAMM_PROGRAM {
        parse_meteora_damm(account_keys, accounts, data)?
    } else if program_id == *PUMP_AMM_PROGRAM {
        parse_pump_amm(account_keys, accounts, data)?
    } else {
        return None;
    };

    hint.instruction_scope = instruction_scope;
    hint.outer_instruction_index = outer_instruction_index;
    hint.inner_instruction_index = inner_instruction_index;
    hint.stack_height = stack_height;
    Some(hint)
}

// These account layouts mirror the parsers in ../anvil/crates/dex-core/src/*_instruction_parser.rs
fn parse_raydium_v4(account_keys: &[Pubkey], accounts: &[u8], data: &[u8]) -> Option<ParsedSwapHint> {
    if data.len() != 17 {
        return None;
    }
    if !matches!(data.first().copied(), Some(RAYDIUM_V4_SWAP_BASE_IN | RAYDIUM_V4_SWAP_MAX_AMOUNT_IN))
    {
        return None;
    }
    let (coin_vault_index, pc_vault_index) = match accounts.len() {
        17 => (4, 5),
        18 => (5, 6),
        _ => return None,
    };
    let pool_address = resolve_ix_account(account_keys, accounts, 1)?;
    let vault_a = resolve_ix_account(account_keys, accounts, pc_vault_index)?;
    let vault_b = resolve_ix_account(account_keys, accounts, coin_vault_index)?;
    let user_input = resolve_ix_account(account_keys, accounts, accounts.len().checked_sub(3)?)?;
    let user_output = resolve_ix_account(account_keys, accounts, accounts.len().checked_sub(2)?)?;
    let signer = resolve_ix_account(account_keys, accounts, accounts.len().checked_sub(1)?)?;

    Some(ParsedSwapHint {
        venue: Venue::RaydiumV4,
        pool_address,
        signer,
        user_a: user_input,
        user_b: user_output,
        vault_a,
        vault_b,
        user_input_hint: Some(user_input),
        user_output_hint: Some(user_output),
        vault_input_hint: None,
        vault_output_hint: None,
        instruction_amount_hint: Some(u64::from_le_bytes(data.get(1..9)?.try_into().ok()?)),
        instruction_scope: InstructionScope::TopLevel,
        outer_instruction_index: 0,
        inner_instruction_index: None,
        stack_height: None,
    })
}

fn parse_raydium_cpmm(account_keys: &[Pubkey], accounts: &[u8], data: &[u8]) -> Option<ParsedSwapHint> {
    let discriminator = data.get(..8)?;
    if discriminator != &*RAYDIUM_CPMM_SWAP_BASE_INPUT
        && discriminator != &*RAYDIUM_CPMM_SWAP_BASE_OUTPUT
    {
        return None;
    }
    let pool_address = resolve_ix_account(account_keys, accounts, 3)?;
    let user_input = resolve_ix_account(account_keys, accounts, 4)?;
    let user_output = resolve_ix_account(account_keys, accounts, 5)?;
    let vault_input = resolve_ix_account(account_keys, accounts, 6)?;
    let vault_output = resolve_ix_account(account_keys, accounts, 7)?;
    let signer = resolve_ix_account(account_keys, accounts, 0)?;

    Some(ParsedSwapHint {
        venue: Venue::RaydiumCpmm,
        pool_address,
        signer,
        user_a: user_input,
        user_b: user_output,
        vault_a: vault_input,
        vault_b: vault_output,
        user_input_hint: Some(user_input),
        user_output_hint: Some(user_output),
        vault_input_hint: Some(vault_input),
        vault_output_hint: Some(vault_output),
        instruction_amount_hint: Some(u64::from_le_bytes(data.get(8..16)?.try_into().ok()?)),
        instruction_scope: InstructionScope::TopLevel,
        outer_instruction_index: 0,
        inner_instruction_index: None,
        stack_height: None,
    })
}

fn parse_raydium_clmm(account_keys: &[Pubkey], accounts: &[u8], data: &[u8]) -> Option<ParsedSwapHint> {
    let discriminator = data.get(..8)?;
    if discriminator != &*RAYDIUM_CLMM_SWAP && discriminator != &*RAYDIUM_CLMM_SWAP_V2 {
        return None;
    }

    Some(ParsedSwapHint {
        venue: Venue::RaydiumClmm,
        pool_address: resolve_ix_account(account_keys, accounts, 2)?,
        signer: resolve_ix_account(account_keys, accounts, 0)?,
        user_a: resolve_ix_account(account_keys, accounts, 3)?,
        user_b: resolve_ix_account(account_keys, accounts, 4)?,
        vault_a: resolve_ix_account(account_keys, accounts, 5)?,
        vault_b: resolve_ix_account(account_keys, accounts, 6)?,
        user_input_hint: None,
        user_output_hint: None,
        vault_input_hint: None,
        vault_output_hint: None,
        instruction_amount_hint: Some(u64::from_le_bytes(data.get(8..16)?.try_into().ok()?)),
        instruction_scope: InstructionScope::TopLevel,
        outer_instruction_index: 0,
        inner_instruction_index: None,
        stack_height: None,
    })
}

fn parse_whirlpool(account_keys: &[Pubkey], accounts: &[u8], data: &[u8]) -> Option<ParsedSwapHint> {
    let discriminator = data.get(..8)?;
    let is_a_to_b = data.get(41).copied()? == 1;
    let (pool_index, signer_index, user_a_index, vault_a_index, user_b_index, vault_b_index) =
        if discriminator == WHIRLPOOL_SWAP {
            (2, 1, 3, 4, 5, 6)
        } else if discriminator == WHIRLPOOL_SWAP2 {
            (4, 3, 7, 8, 9, 10)
        } else {
            return None;
        };

    let user_a = resolve_ix_account(account_keys, accounts, user_a_index)?;
    let user_b = resolve_ix_account(account_keys, accounts, user_b_index)?;
    let vault_a = resolve_ix_account(account_keys, accounts, vault_a_index)?;
    let vault_b = resolve_ix_account(account_keys, accounts, vault_b_index)?;

    Some(ParsedSwapHint {
        venue: Venue::Whirlpool,
        pool_address: resolve_ix_account(account_keys, accounts, pool_index)?,
        signer: resolve_ix_account(account_keys, accounts, signer_index)?,
        user_a,
        user_b,
        vault_a,
        vault_b,
        user_input_hint: Some(if is_a_to_b { user_a } else { user_b }),
        user_output_hint: Some(if is_a_to_b { user_b } else { user_a }),
        vault_input_hint: Some(if is_a_to_b { vault_a } else { vault_b }),
        vault_output_hint: Some(if is_a_to_b { vault_b } else { vault_a }),
        instruction_amount_hint: Some(u64::from_le_bytes(data.get(8..16)?.try_into().ok()?)),
        instruction_scope: InstructionScope::TopLevel,
        outer_instruction_index: 0,
        inner_instruction_index: None,
        stack_height: None,
    })
}

fn parse_meteora_dlmm(account_keys: &[Pubkey], accounts: &[u8], data: &[u8]) -> Option<ParsedSwapHint> {
    let discriminator = data.get(..8)?;
    if discriminator != METEORA_DLMM_SWAP
        && discriminator != METEORA_DLMM_SWAP2
        && discriminator != METEORA_DLMM_SWAP_EXACT_OUT
        && discriminator != METEORA_DLMM_SWAP_EXACT_OUT2
        && discriminator != METEORA_DLMM_SWAP_WITH_PRICE_IMPACT
        && discriminator != METEORA_DLMM_SWAP_WITH_PRICE_IMPACT2
    {
        return None;
    }

    Some(ParsedSwapHint {
        venue: Venue::MeteoraDlmm,
        pool_address: resolve_ix_account(account_keys, accounts, 0)?,
        signer: resolve_ix_account(account_keys, accounts, 10)?,
        user_a: resolve_ix_account(account_keys, accounts, 4)?,
        user_b: resolve_ix_account(account_keys, accounts, 5)?,
        vault_a: resolve_ix_account(account_keys, accounts, 2)?,
        vault_b: resolve_ix_account(account_keys, accounts, 3)?,
        user_input_hint: None,
        user_output_hint: None,
        vault_input_hint: None,
        vault_output_hint: None,
        instruction_amount_hint: Some(u64::from_le_bytes(data.get(8..16)?.try_into().ok()?)),
        instruction_scope: InstructionScope::TopLevel,
        outer_instruction_index: 0,
        inner_instruction_index: None,
        stack_height: None,
    })
}

fn parse_meteora_damm(account_keys: &[Pubkey], accounts: &[u8], data: &[u8]) -> Option<ParsedSwapHint> {
    let discriminator = data.get(..8)?;
    if discriminator != METEORA_DAMM_SWAP && discriminator != METEORA_DAMM_SWAP2 {
        return None;
    }

    Some(ParsedSwapHint {
        venue: Venue::MeteoraDamm,
        pool_address: resolve_ix_account(account_keys, accounts, 1)?,
        signer: resolve_ix_account(account_keys, accounts, 8)?,
        user_a: resolve_ix_account(account_keys, accounts, 2)?,
        user_b: resolve_ix_account(account_keys, accounts, 3)?,
        vault_a: resolve_ix_account(account_keys, accounts, 4)?,
        vault_b: resolve_ix_account(account_keys, accounts, 5)?,
        user_input_hint: None,
        user_output_hint: None,
        vault_input_hint: None,
        vault_output_hint: None,
        instruction_amount_hint: Some(u64::from_le_bytes(data.get(8..16)?.try_into().ok()?)),
        instruction_scope: InstructionScope::TopLevel,
        outer_instruction_index: 0,
        inner_instruction_index: None,
        stack_height: None,
    })
}

fn parse_pump_amm(account_keys: &[Pubkey], accounts: &[u8], data: &[u8]) -> Option<ParsedSwapHint> {
    let discriminator = data.get(..8)?;
    let (user_input, user_output, vault_input, vault_output) =
        if discriminator == PUMP_AMM_BUY_DISCRIMINATOR
            || discriminator == PUMP_AMM_BUY_EXACT_QUOTE_IN_DISCRIMINATOR
        {
            (
                resolve_ix_account(account_keys, accounts, 6)?,
                resolve_ix_account(account_keys, accounts, 5)?,
                resolve_ix_account(account_keys, accounts, 8)?,
                resolve_ix_account(account_keys, accounts, 7)?,
            )
        } else if discriminator == PUMP_AMM_SELL_DISCRIMINATOR {
            (
                resolve_ix_account(account_keys, accounts, 5)?,
                resolve_ix_account(account_keys, accounts, 6)?,
                resolve_ix_account(account_keys, accounts, 7)?,
                resolve_ix_account(account_keys, accounts, 8)?,
            )
        } else {
            return None;
        };

    Some(ParsedSwapHint {
        venue: Venue::PumpAmm,
        pool_address: resolve_ix_account(account_keys, accounts, 0)?,
        signer: resolve_ix_account(account_keys, accounts, 1)?,
        user_a: resolve_ix_account(account_keys, accounts, 5)?,
        user_b: resolve_ix_account(account_keys, accounts, 6)?,
        vault_a: resolve_ix_account(account_keys, accounts, 7)?,
        vault_b: resolve_ix_account(account_keys, accounts, 8)?,
        user_input_hint: Some(user_input),
        user_output_hint: Some(user_output),
        vault_input_hint: Some(vault_input),
        vault_output_hint: Some(vault_output),
        instruction_amount_hint: Some(u64::from_le_bytes(data.get(8..16)?.try_into().ok()?)),
        instruction_scope: InstructionScope::TopLevel,
        outer_instruction_index: 0,
        inner_instruction_index: None,
        stack_height: None,
    })
}

fn pubkey(value: &str) -> Pubkey {
    value.parse().unwrap_or_else(|_| panic!("invalid pubkey literal: {value}"))
}

fn anchor_discriminator(name: &str) -> [u8; 8] {
    let mut hasher = Sha256::new();
    hasher.update(format!("global:{name}"));
    let hash = hasher.finalize();
    let mut discriminator = [0u8; 8];
    discriminator.copy_from_slice(&hash[..8]);
    discriminator
}

fn resolve_ix_account(account_keys: &[Pubkey], accounts: &[u8], position: usize) -> Option<Pubkey> {
    let account_index = *accounts.get(position)? as usize;
    account_keys.get(account_index).copied()
}

fn resolve_swap(
    hint: ParsedSwapHint,
    balances: &TokenBalanceContext,
    inner_transfers: &[ParsedInnerTransfer],
) -> ResolvedSwap {
    let user_input = choose_user_input(&hint, balances, inner_transfers);
    let user_output = choose_other_account(user_input, hint.user_a, hint.user_b)
        .or(hint.user_output_hint)
        .or(Some(hint.user_b));
    let vault_input = choose_vault_input(&hint, user_input, balances, inner_transfers);
    let vault_output = choose_other_account(vault_input, hint.vault_a, hint.vault_b)
        .or(hint.vault_output_hint)
        .or(Some(hint.vault_b));

    let matched_input_transfers = match_transfer_direction(
        inner_transfers,
        user_input,
        vault_input,
        hint.outer_instruction_index,
    );
    let matched_output_transfers = match_transfer_direction(
        inner_transfers,
        vault_output,
        user_output,
        hint.outer_instruction_index,
    );

    let user_input_delta = user_input.and_then(|account| user_spend_delta(balances, &account));
    let user_output_delta = user_output.and_then(|account| user_receive_delta(balances, &account));
    let vault_input_delta = vault_input.and_then(|account| vault_receive_delta(balances, &account));
    let vault_output_delta = vault_output.and_then(|account| vault_spend_delta(balances, &account));

    let input_raw = user_input_delta
        .or_else(|| sum_transfer_amounts(&matched_input_transfers))
        .or(vault_input_delta)
        .or(hint.instruction_amount_hint);
    let output_raw = user_output_delta
        .or_else(|| sum_transfer_amounts(&matched_output_transfers))
        .or(vault_output_delta);

    let input_meta = user_input
        .and_then(|account| token_meta_for_account(balances, &account))
        .or_else(|| vault_input.and_then(|account| token_meta_for_account(balances, &account)))
        .or_else(|| transfer_meta(inner_transfers, user_input, vault_input));
    let output_meta = user_output
        .and_then(|account| token_meta_for_account(balances, &account))
        .or_else(|| vault_output.and_then(|account| token_meta_for_account(balances, &account)))
        .or_else(|| transfer_meta(inner_transfers, vault_output, user_output));

    ResolvedSwap {
        venue_slug: slug_for_venue(&hint.venue).to_string(),
        instruction_scope: hint.instruction_scope,
        outer_instruction_index: hint.outer_instruction_index,
        inner_instruction_index: hint.inner_instruction_index,
        stack_height: hint.stack_height,
        pool_address: hint.pool_address.to_string(),
        signer: hint.signer.to_string(),
        user_input_account: user_input.map(|value| value.to_string()),
        user_output_account: user_output.map(|value| value.to_string()),
        vault_input_account: vault_input.map(|value| value.to_string()),
        vault_output_account: vault_output.map(|value| value.to_string()),
        input_token: endpoint_from_meta(user_input, input_meta.as_ref()),
        output_token: endpoint_from_meta(user_output, output_meta.as_ref()),
        instruction_amount_hint_raw: hint.instruction_amount_hint.map(|value| value.to_string()),
        amounts: AmountSummary {
            input_raw: input_raw.map(|value| value.to_string()),
            input_ui: input_raw.zip(input_meta.as_ref()).map(|(raw, meta)| {
                format_token_amount(raw, meta.decimals)
            }),
            output_raw: output_raw.map(|value| value.to_string()),
            output_ui: output_raw.zip(output_meta.as_ref()).map(|(raw, meta)| {
                format_token_amount(raw, meta.decimals)
            }),
        },
        balance_deltas: BalanceDeltaSummary {
            user_input_raw: user_input_delta.map(|value| value.to_string()),
            user_output_raw: user_output_delta.map(|value| value.to_string()),
            vault_input_raw: vault_input_delta.map(|value| value.to_string()),
            vault_output_raw: vault_output_delta.map(|value| value.to_string()),
        },
        matched_input_transfers,
        matched_output_transfers,
    }
}

fn choose_user_input(
    hint: &ParsedSwapHint,
    balances: &TokenBalanceContext,
    inner_transfers: &[ParsedInnerTransfer],
) -> Option<Pubkey> {
    if let Some(user_input) = hint.user_input_hint {
        return Some(user_input);
    }

    let transfers = inner_transfers.iter().filter_map(|transfer| {
        let Ok(source) = transfer.source.parse::<Pubkey>() else {
            return None;
        };
        let Ok(destination) = transfer.destination.parse::<Pubkey>() else {
            return None;
        };
        if (source == hint.user_a || source == hint.user_b)
            && (destination == hint.vault_a || destination == hint.vault_b)
        {
            Some((source, transfer.amount_raw.parse::<u64>().unwrap_or_default()))
        } else {
            None
        }
    });

    let mut best: Option<(Pubkey, u64)> = None;
    for candidate in transfers {
        best = Some(match best {
            Some(existing) if existing.1 >= candidate.1 => existing,
            _ => candidate,
        });
    }
    if let Some((source, _)) = best {
        return Some(source);
    }

    let a_delta = user_spend_delta(balances, &hint.user_a).unwrap_or(0);
    let b_delta = user_spend_delta(balances, &hint.user_b).unwrap_or(0);
    if a_delta == 0 && b_delta == 0 {
        None
    } else if a_delta >= b_delta {
        Some(hint.user_a)
    } else {
        Some(hint.user_b)
    }
}

fn choose_vault_input(
    hint: &ParsedSwapHint,
    user_input: Option<Pubkey>,
    balances: &TokenBalanceContext,
    inner_transfers: &[ParsedInnerTransfer],
) -> Option<Pubkey> {
    if let Some(vault_input) = hint.vault_input_hint {
        return Some(vault_input);
    }

    if let Some(user_input) = user_input {
        let mut best: Option<(Pubkey, u64)> = None;
        for transfer in inner_transfers {
            let Ok(source) = transfer.source.parse::<Pubkey>() else {
                continue;
            };
            let Ok(destination) = transfer.destination.parse::<Pubkey>() else {
                continue;
            };
            if source == user_input && (destination == hint.vault_a || destination == hint.vault_b)
            {
                let amount = transfer.amount_raw.parse::<u64>().unwrap_or_default();
                best = Some(match best {
                    Some(existing) if existing.1 >= amount => existing,
                    _ => (destination, amount),
                });
            }
        }
        if let Some((destination, _)) = best {
            return Some(destination);
        }

        if let Some(user_meta) = token_meta_for_account(balances, &user_input) {
            let vault_a_meta = token_meta_for_account(balances, &hint.vault_a);
            let vault_b_meta = token_meta_for_account(balances, &hint.vault_b);
            if vault_a_meta.as_ref().map(|meta| meta.mint) == Some(user_meta.mint) {
                return Some(hint.vault_a);
            }
            if vault_b_meta.as_ref().map(|meta| meta.mint) == Some(user_meta.mint) {
                return Some(hint.vault_b);
            }
        }
    }

    let a_delta = vault_receive_delta(balances, &hint.vault_a).unwrap_or(0);
    let b_delta = vault_receive_delta(balances, &hint.vault_b).unwrap_or(0);
    if a_delta == 0 && b_delta == 0 {
        None
    } else if a_delta >= b_delta {
        Some(hint.vault_a)
    } else {
        Some(hint.vault_b)
    }
}

fn choose_other_account(selected: Option<Pubkey>, a: Pubkey, b: Pubkey) -> Option<Pubkey> {
    match selected {
        Some(value) if value == a => Some(b),
        Some(value) if value == b => Some(a),
        _ => None,
    }
}

fn token_meta_for_account(
    balances: &TokenBalanceContext,
    account: &Pubkey,
) -> Option<TokenBalanceSnapshot> {
    balances
        .post_by_account
        .get(account)
        .cloned()
        .or_else(|| balances.pre_by_account.get(account).cloned())
}

fn transfer_meta(
    transfers: &[ParsedInnerTransfer],
    source: Option<Pubkey>,
    destination: Option<Pubkey>,
) -> Option<TokenBalanceSnapshot> {
    let (Some(source), Some(destination)) = (source, destination) else {
        return None;
    };
    for transfer in transfers {
        let Ok(transfer_source) = transfer.source.parse::<Pubkey>() else {
            continue;
        };
        let Ok(transfer_destination) = transfer.destination.parse::<Pubkey>() else {
            continue;
        };
        if transfer_source == source && transfer_destination == destination {
            let mint = transfer.mint.as_ref()?.parse::<Pubkey>().ok()?;
            return Some(TokenBalanceSnapshot {
                mint,
                owner: String::new(),
                program_id: transfer.token_program.clone(),
                raw_amount: 0,
                decimals: transfer.decimals.unwrap_or(0),
            });
        }
    }
    None
}

fn endpoint_from_meta(account: Option<Pubkey>, meta: Option<&TokenBalanceSnapshot>) -> Option<TokenEndpoint> {
    Some(TokenEndpoint {
        account: account?.to_string(),
        mint: meta?.mint.to_string(),
        owner: meta?.owner.clone(),
        program_id: meta?.program_id.clone(),
        decimals: meta?.decimals,
    })
}

fn match_transfer_direction(
    transfers: &[ParsedInnerTransfer],
    source: Option<Pubkey>,
    destination: Option<Pubkey>,
    outer_instruction_index: usize,
) -> Vec<ParsedInnerTransfer> {
    let (Some(source), Some(destination)) = (source, destination) else {
        return Vec::new();
    };
    transfers
        .iter()
        .filter(|transfer| {
            transfer.instruction_group_index as usize == outer_instruction_index
                && transfer.source == source.to_string()
                && transfer.destination == destination.to_string()
        })
        .cloned()
        .collect()
}

fn related_transfers_for_swaps(
    swaps: &[ResolvedSwap],
    transfers: &[ParsedInnerTransfer],
) -> Vec<ParsedInnerTransfer> {
    let mut keys = HashSet::new();
    for swap in swaps {
        for transfer in &swap.matched_input_transfers {
            keys.insert((transfer.instruction_group_index, transfer.inner_instruction_index));
        }
        for transfer in &swap.matched_output_transfers {
            keys.insert((transfer.instruction_group_index, transfer.inner_instruction_index));
        }
    }
    let mut related = transfers
        .iter()
        .filter(|transfer| keys.contains(&(transfer.instruction_group_index, transfer.inner_instruction_index)))
        .cloned()
        .collect::<Vec<_>>();
    related.sort_by_key(|transfer| {
        (transfer.instruction_group_index, transfer.inner_instruction_index)
    });
    related
}

fn group_swaps_by_venue(swaps: &[ResolvedSwap]) -> HashMap<String, Vec<ResolvedSwap>> {
    let mut grouped = HashMap::<String, Vec<ResolvedSwap>>::new();
    for swap in swaps {
        grouped.entry(swap.venue_slug.clone()).or_default().push(swap.clone());
    }
    grouped
}

fn slug_for_venue(venue: &Venue) -> &'static str {
    match venue {
        Venue::RaydiumV4 => "raydium_v4",
        Venue::RaydiumCpmm => "raydium_cpmm",
        Venue::RaydiumClmm => "raydium_clmm",
        Venue::Whirlpool => "orca_whirlpool",
        Venue::MeteoraDlmm => "meteora_dlmm",
        Venue::MeteoraDamm => "meteora_damm",
        Venue::PumpAmm => "pump_amm",
    }
}

fn user_spend_delta(balances: &TokenBalanceContext, account: &Pubkey) -> Option<u64> {
    positive_delta(
        balances.pre_by_account.get(account).map(|value| value.raw_amount),
        balances.post_by_account.get(account).map(|value| value.raw_amount),
    )
}

fn user_receive_delta(balances: &TokenBalanceContext, account: &Pubkey) -> Option<u64> {
    positive_delta(
        balances.post_by_account.get(account).map(|value| value.raw_amount),
        balances.pre_by_account.get(account).map(|value| value.raw_amount),
    )
}

fn vault_receive_delta(balances: &TokenBalanceContext, account: &Pubkey) -> Option<u64> {
    positive_delta(
        balances.post_by_account.get(account).map(|value| value.raw_amount),
        balances.pre_by_account.get(account).map(|value| value.raw_amount),
    )
}

fn vault_spend_delta(balances: &TokenBalanceContext, account: &Pubkey) -> Option<u64> {
    positive_delta(
        balances.pre_by_account.get(account).map(|value| value.raw_amount),
        balances.post_by_account.get(account).map(|value| value.raw_amount),
    )
}

fn positive_delta(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    let left = left.unwrap_or_default();
    let right = right.unwrap_or_default();
    left.checked_sub(right).filter(|value| *value > 0)
}

fn sum_transfer_amounts(transfers: &[ParsedInnerTransfer]) -> Option<u64> {
    let mut total = 0u64;
    let mut any = false;
    for transfer in transfers {
        if let Ok(amount) = transfer.amount_raw.parse::<u64>() {
            total = total.saturating_add(amount);
            any = true;
        }
    }
    any.then_some(total)
}

fn format_token_amount(amount: u64, decimals: u8) -> String {
    if decimals == 0 {
        return amount.to_string();
    }
    let width = decimals as usize;
    let mut digits = amount.to_string();
    if digits.len() <= width {
        let padding = "0".repeat(width + 1 - digits.len());
        digits = format!("{}{}", padding, digits);
    }
    let split = digits.len() - width;
    let whole = &digits[..split];
    let frac = digits[split..].trim_end_matches('0');
    if frac.is_empty() {
        whole.to_string()
    } else {
        format!("{}.{}", whole, frac)
    }
}

fn build_summary(
    args: &Args,
    shared: &SharedState,
    slot_range_start: u64,
    slot_range_end_inclusive: u64,
    started_at: SystemTime,
    finished_at: SystemTime,
    duration: Duration,
) -> Result<RunSummary> {
    let progress = shared
        .progress
        .lock()
        .map_err(|_| anyhow!("progress mutex poisoned"))?;
    Ok(RunSummary {
        rpc_url: args.rpc_url.clone(),
        per_dex_requested: args.per_dex,
        unique_signatures: shared.total_signatures,
        missing_transactions: progress.missing_transactions,
        output_root: shared.output_root.display().to_string(),
        slot_range_start,
        slot_range_end_inclusive,
        started_at_unix_ms: started_at
            .duration_since(UNIX_EPOCH)
            .context("start time is before unix epoch")?
            .as_millis(),
        finished_at_unix_ms: finished_at
            .duration_since(UNIX_EPOCH)
            .context("finish time is before unix epoch")?
            .as_millis(),
        duration_seconds: duration.as_secs_f64(),
        per_target: shared
            .targets
            .iter()
            .enumerate()
            .map(|(index, target)| RunSummaryTarget {
                venue: target.venue.as_str().to_string(),
                program_id: target.program_id.to_string(),
                fetched_signatures: progress.per_target[index].fetched_signatures,
                written_files: progress.per_target[index].written_files,
                parse_misses: progress.per_target[index].parse_misses,
            })
            .collect(),
    })
}
