//! Upload verified Horizon archives to Cloudflare R2, restore verified copies,
//! and retire local copies.
//!
//! The remote API surface is intentionally append-only: this program contains
//! no DeleteObject operation. A local archive is removed only after its exact
//! SHA-256 sidecar, length, multipart ETag, and immutable metadata have been
//! observed in R2 and a private receipt has been fsynced. The sidecar is
//! published last and serves as the remote completion marker.

use {
    anyhow::{Context, Result, anyhow, bail, ensure},
    aws_credential_types::{Credentials, provider::SharedCredentialsProvider},
    aws_sdk_s3::{
        Client,
        config::{BehaviorVersion, Builder as S3ConfigBuilder, RequestChecksumCalculation},
        primitives::ByteStream,
        types::{ChecksumMode, ChecksumType, CompletedMultipartUpload, CompletedPart},
    },
    aws_types::region::Region,
    base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD},
    md5::Md5,
    serde::{Deserialize, Serialize},
    sha2::{Digest as _, Sha256},
    std::{
        collections::BTreeSet,
        env,
        fs::{self, File, OpenOptions},
        io::{Read, Seek, SeekFrom, Write},
        os::unix::{fs::MetadataExt as _, fs::OpenOptionsExt as _},
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    },
    url::Url,
};

const DEFAULT_PART_SIZE: u64 = 64 * 1024 * 1024;
const DEFAULT_CONCURRENCY: usize = 8;
const MAX_SIDECAR_BYTES: u64 = 512;
const MAX_PART_ATTEMPTS: usize = 8;
const MAX_REMOTE_READ_ATTEMPTS: usize = 64;
const RECEIPT_SCHEMA: &str = "jetstreamer-horizon-r2-receipt-v1";

#[derive(Debug)]
struct Config {
    command: Command,
    directory: PathBuf,
    epochs: BTreeSet<u64>,
    receipt_directory: PathBuf,
    delete_local: bool,
    part_size: u64,
    legacy_part_size: Option<u64>,
    concurrency: usize,
    legacy_etag_only: bool,
    overwrite_existing: bool,
    repair_orphaned_archive: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Command {
    Sync,
    Verify,
    Restore,
}

#[derive(Clone, Debug)]
struct R2Config {
    endpoint: String,
    bucket: String,
    access_key: String,
    secret_key: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileIdentity {
    device: u64,
    inode: u64,
    length: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
}

#[derive(Debug)]
struct LocalArchive {
    epoch: u64,
    archive_path: PathBuf,
    sidecar_path: PathBuf,
    identity: FileIdentity,
    sidecar_identity: FileIdentity,
    length: u64,
    sha256_hex: String,
    sidecar: Vec<u8>,
}

#[derive(Debug)]
struct LocalProof {
    sha256_hex: String,
    multipart_etag: String,
    composite_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Receipt {
    schema: String,
    bucket: String,
    epoch: u64,
    archive_key: String,
    checksum_key: String,
    archive_length: u64,
    archive_sha256: String,
    archive_etag: String,
    r2_composite_sha256: Option<String>,
    #[serde(default)]
    remote_sha256_readback: bool,
    multipart_part_size: u64,
    verified_unix_seconds: u64,
}

fn usage() -> ! {
    eprintln!(concat!(
        "usage: horizon-r2 <sync|verify|restore> DIRECTORY [--epochs START-END] ",
        "--receipt-directory DIRECTORY [--delete-local] [--part-size-mib N] ",
        "[--legacy-part-size-mib N] [--legacy-etag-only] [--overwrite-existing] ",
        "[--repair-orphaned-archive] ",
        "[--concurrency N]"
    ));
    std::process::exit(2);
}

fn parse_args() -> Result<Config> {
    let mut args = env::args_os().skip(1);
    let command = match args
        .next()
        .and_then(|value| value.into_string().ok())
        .as_deref()
    {
        Some("sync") => Command::Sync,
        Some("verify") => Command::Verify,
        Some("restore") => Command::Restore,
        _ => usage(),
    };
    let directory = args.next().map(PathBuf::from).unwrap_or_else(|| usage());
    let mut epochs = None;
    let mut receipt_directory = None;
    let mut delete_local = false;
    let mut part_size = DEFAULT_PART_SIZE;
    let mut legacy_part_size = None;
    let mut concurrency = DEFAULT_CONCURRENCY;
    let mut legacy_etag_only = false;
    let mut overwrite_existing = false;
    let mut repair_orphaned_archive = false;
    while let Some(argument) = args.next() {
        let argument = argument
            .into_string()
            .map_err(|_| anyhow!("argument is not UTF-8"))?;
        let mut value = || {
            args.next()
                .ok_or_else(|| anyhow!("missing value for {argument}"))?
                .into_string()
                .map_err(|_| anyhow!("value for {argument} is not UTF-8"))
        };
        match argument.as_str() {
            "--epochs" => epochs = Some(parse_epoch_range(&value()?)?),
            "--receipt-directory" => receipt_directory = Some(PathBuf::from(value()?)),
            "--delete-local" => delete_local = true,
            "--part-size-mib" => part_size = parse_mib(&value()?)?,
            "--legacy-part-size-mib" => legacy_part_size = Some(parse_mib(&value()?)?),
            "--legacy-etag-only" => legacy_etag_only = true,
            "--overwrite-existing" => overwrite_existing = true,
            "--repair-orphaned-archive" => repair_orphaned_archive = true,
            "--concurrency" => {
                concurrency = value()?.parse().context("invalid --concurrency")?;
                ensure!(
                    (1..=64).contains(&concurrency),
                    "concurrency must be 1..=64"
                );
            }
            _ => bail!("unknown argument: {argument}"),
        }
    }
    ensure!(
        directory.is_absolute(),
        "archive directory must be absolute"
    );
    let receipt_directory =
        receipt_directory.ok_or_else(|| anyhow!("--receipt-directory is required"))?;
    ensure!(
        receipt_directory.is_absolute(),
        "receipt directory must be absolute"
    );
    ensure!(
        !delete_local || command == Command::Sync,
        "--delete-local is valid only with sync"
    );
    ensure!(
        !overwrite_existing || command == Command::Sync,
        "--overwrite-existing is valid only with sync"
    );
    ensure!(
        !repair_orphaned_archive || command == Command::Sync,
        "--repair-orphaned-archive is valid only with sync"
    );
    ensure!(
        !repair_orphaned_archive || !overwrite_existing,
        "--repair-orphaned-archive and --overwrite-existing are mutually exclusive"
    );
    let epochs = match (command, epochs) {
        (Command::Restore, None) => bail!("restore requires --epochs START-END"),
        (_, Some(epochs)) => epochs,
        (_, None) => discover_local_epochs(&directory)?,
    };
    ensure!(
        !epochs.is_empty(),
        "no complete local Horizon archive pairs found"
    );
    Ok(Config {
        command,
        directory,
        epochs,
        receipt_directory,
        delete_local,
        part_size,
        legacy_part_size,
        concurrency,
        legacy_etag_only,
        overwrite_existing,
        repair_orphaned_archive,
    })
}

fn discover_local_epochs(directory: &Path) -> Result<BTreeSet<u64>> {
    let mut epochs = BTreeSet::new();
    for entry in fs::read_dir(directory)
        .with_context(|| format!("failed to read archive directory {}", directory.display()))?
    {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(epoch) = name
            .strip_prefix("epoch-")
            .and_then(|value| value.strip_suffix(".jet"))
            .and_then(|value| value.parse::<u64>().ok())
        else {
            continue;
        };
        if directory
            .join(format!("epoch-{epoch}.jet.sha256"))
            .is_file()
        {
            epochs.insert(epoch);
        }
    }
    Ok(epochs)
}

fn parse_epoch_range(value: &str) -> Result<BTreeSet<u64>> {
    let (first, last) = value
        .split_once('-')
        .ok_or_else(|| anyhow!("epoch range must be START-END"))?;
    let first: u64 = first.parse().context("invalid first epoch")?;
    let last: u64 = last.parse().context("invalid last epoch")?;
    ensure!(first <= last, "epoch range is reversed");
    ensure!(last - first < 10_000, "epoch range is unreasonably large");
    Ok((first..=last).collect())
}

fn parse_mib(value: &str) -> Result<u64> {
    let mib: u64 = value.parse().context("invalid MiB value")?;
    ensure!(
        (5..=5 * 1024).contains(&mib),
        "part size must be 5..=5120 MiB"
    );
    mib.checked_mul(1024 * 1024)
        .ok_or_else(|| anyhow!("part size overflow"))
}

fn r2_config_from_env() -> Result<R2Config> {
    let raw = env::var("HORIZON_S3_ENDPOINT").context("HORIZON_S3_ENDPOINT is unset")?;
    let url = Url::parse(&raw).context("invalid HORIZON_S3_ENDPOINT")?;
    ensure!(url.scheme() == "https", "R2 endpoint must use HTTPS");
    ensure!(
        url.query().is_none() && url.fragment().is_none(),
        "R2 endpoint has query/fragment"
    );
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("R2 endpoint has no host"))?;
    ensure!(
        host.ends_with(".r2.cloudflarestorage.com"),
        "unexpected R2 endpoint host"
    );
    let bucket = url.path().trim_matches('/').to_string();
    ensure!(
        !bucket.is_empty() && !bucket.contains('/'),
        "R2 endpoint must contain one bucket path"
    );
    let endpoint = format!("{}://{}", url.scheme(), host);
    Ok(R2Config {
        endpoint,
        bucket,
        access_key: env::var("HORIZON_ACCESS_KEY_ID").context("HORIZON_ACCESS_KEY_ID is unset")?,
        secret_key: env::var("HORIZON_SECRET_ACCESS_KEY")
            .context("HORIZON_SECRET_ACCESS_KEY is unset")?,
    })
}

fn s3_client(config: &R2Config) -> Client {
    let credentials = Credentials::new(
        config.access_key.clone(),
        config.secret_key.clone(),
        None,
        None,
        "horizon-r2",
    );
    let sdk = S3ConfigBuilder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("auto"))
        .credentials_provider(SharedCredentialsProvider::new(credentials))
        .endpoint_url(&config.endpoint)
        .force_path_style(true)
        // R2 rejects a PutObject request that combines our explicit
        // Content-MD5 with the SDK's optional default CRC32 checksum.
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .build();
    Client::from_conf(sdk)
}

fn identity(metadata: &fs::Metadata) -> FileIdentity {
    FileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
        length: metadata.len(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
    }
}

fn regular_identity(path: &Path) -> Result<FileIdentity> {
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("failed to stat {}", path.display()))?;
    ensure!(
        metadata.file_type().is_file(),
        "not a regular file: {}",
        path.display()
    );
    ensure!(
        metadata.nlink() == 1,
        "file has multiple links: {}",
        path.display()
    );
    Ok(identity(&metadata))
}

fn read_local_archive(directory: &Path, epoch: u64) -> Result<LocalArchive> {
    let archive_path = directory.join(format!("epoch-{epoch}.jet"));
    let sidecar_path = directory.join(format!("epoch-{epoch}.jet.sha256"));
    let identity = regular_identity(&archive_path)?;
    let sidecar_identity = regular_identity(&sidecar_path)?;
    ensure!(
        sidecar_identity.length <= MAX_SIDECAR_BYTES,
        "oversized checksum sidecar"
    );
    let sidecar = fs::read(&sidecar_path).context("failed to read checksum sidecar")?;
    let expected_suffix = format!("  epoch-{epoch}.jet\n");
    ensure!(
        sidecar.len() == 64 + expected_suffix.len(),
        "noncanonical checksum sidecar"
    );
    ensure!(
        &sidecar[64..] == expected_suffix.as_bytes(),
        "checksum names wrong archive"
    );
    let digest = std::str::from_utf8(&sidecar[..64]).context("checksum is not ASCII")?;
    ensure!(
        digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()),
        "checksum is not lowercase SHA-256"
    );
    ensure!(
        regular_identity(&sidecar_path)? == sidecar_identity,
        "sidecar changed while read"
    );
    Ok(LocalArchive {
        epoch,
        archive_path,
        sidecar_path,
        identity,
        sidecar_identity,
        length: identity.length,
        sha256_hex: digest.to_owned(),
        sidecar,
    })
}

fn hash_and_etag(path: &Path, expected: FileIdentity, part_size: u64) -> Result<LocalProof> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    ensure!(
        identity(&file.metadata()?) == expected,
        "archive changed before hashing"
    );
    let mut sha256 = Sha256::new();
    let mut part_digests = Vec::new();
    let mut part_sha256_digests = Vec::new();
    let mut remaining = expected.length;
    let mut buffer = vec![0u8; usize::try_from(part_size.min(8 * 1024 * 1024))?];
    while remaining > 0 {
        let this_part = remaining.min(part_size);
        let mut part_remaining = this_part;
        let mut md5 = Md5::new();
        let mut part_sha256 = Sha256::new();
        while part_remaining > 0 {
            let take = usize::try_from(part_remaining.min(buffer.len() as u64))?;
            file.read_exact(&mut buffer[..take])?;
            sha256.update(&buffer[..take]);
            md5.update(&buffer[..take]);
            part_sha256.update(&buffer[..take]);
            part_remaining -= take as u64;
        }
        part_digests.push(md5.finalize().to_vec());
        part_sha256_digests.push(part_sha256.finalize().to_vec());
        remaining -= this_part;
    }
    ensure!(
        identity(&file.metadata()?) == expected,
        "archive changed while hashing"
    );
    ensure!(
        regular_identity(path)? == expected,
        "archive namespace changed while hashing"
    );
    let sha256_hex = format!("{:x}", sha256.finalize());
    Ok(LocalProof {
        sha256_hex,
        multipart_etag: multipart_etag(&part_digests),
        composite_sha256: composite_sha256(&part_sha256_digests),
    })
}

fn multipart_etag(part_digests: &[Vec<u8>]) -> String {
    let mut md5 = Md5::new();
    for digest in part_digests {
        md5.update(digest);
    }
    format!("{:x}-{}", md5.finalize(), part_digests.len())
}

fn composite_sha256(part_digests: &[Vec<u8>]) -> String {
    let mut sha256 = Sha256::new();
    for digest in part_digests {
        sha256.update(digest);
    }
    BASE64_STANDARD.encode(sha256.finalize())
}

fn composite_checksum_matches(observed: &str, expected: &str, parts: u64) -> bool {
    observed == expected || observed == format!("{expected}-{parts}")
}

fn normalize_etag(value: &str) -> &str {
    value
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .unwrap_or(value)
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

async fn remote_sidecar(client: &Client, bucket: &str, key: &str) -> Result<Option<Vec<u8>>> {
    match client.get_object().bucket(bucket).key(key).send().await {
        Ok(response) => {
            let bytes = response.body.collect().await?.into_bytes();
            ensure!(
                bytes.len() <= MAX_SIDECAR_BYTES as usize,
                "remote sidecar is oversized"
            );
            Ok(Some(bytes.to_vec()))
        }
        Err(error) if error.as_service_error().is_some_and(|e| e.is_no_such_key()) => Ok(None),
        Err(error) => Err(error).context("failed to fetch remote sidecar"),
    }
}

#[derive(Debug)]
struct RemoteArchive {
    length: u64,
    etag: String,
    metadata_sha256: Option<String>,
    composite_sha256: Option<String>,
    checksum_type: Option<ChecksumType>,
    multipart_part_size: Option<u64>,
}

async fn head_archive(client: &Client, bucket: &str, key: &str) -> Result<Option<RemoteArchive>> {
    match client
        .head_object()
        .bucket(bucket)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
    {
        Ok(response) => {
            let length = u64::try_from(response.content_length().unwrap_or(-1))
                .context("remote archive has invalid length")?;
            let etag = response
                .e_tag()
                .ok_or_else(|| anyhow!("remote archive has no ETag"))?;
            Ok(Some(RemoteArchive {
                length,
                etag: normalize_etag(etag).to_owned(),
                metadata_sha256: response.metadata().and_then(|m| m.get("sha256").cloned()),
                composite_sha256: response.checksum_sha256().map(str::to_owned),
                checksum_type: response.checksum_type().cloned(),
                multipart_part_size: response
                    .metadata()
                    .and_then(|metadata| metadata.get("multipart-part-size"))
                    .map(|value| value.parse::<u64>())
                    .transpose()
                    .context("remote multipart-part-size metadata is invalid")?,
            }))
        }
        Err(error) if error.as_service_error().is_some_and(|e| e.is_not_found()) => Ok(None),
        Err(error) => Err(error).context("failed to stat remote archive"),
    }
}

fn read_part(
    path: &Path,
    expected: FileIdentity,
    offset: u64,
    length: usize,
) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)?;
    ensure!(
        identity(&file.metadata()?) == expected,
        "archive changed before part read"
    );
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = vec![0u8; length];
    file.read_exact(&mut bytes)?;
    ensure!(
        identity(&file.metadata()?) == expected,
        "archive changed during part read"
    );
    let digest = Md5::digest(&bytes).to_vec();
    let sha256 = Sha256::digest(&bytes).to_vec();
    Ok((bytes, digest, sha256))
}

async fn upload_archive(
    client: &Client,
    bucket: &str,
    local: &LocalArchive,
    part_size: u64,
    concurrency: usize,
    overwrite_existing: bool,
) -> Result<(String, String)> {
    let key = format!("epoch-{}.jet", local.epoch);
    let created = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(&key)
        .content_type("application/octet-stream")
        .metadata("sha256", &local.sha256_hex)
        .metadata("horizon-format", "jet")
        .metadata("multipart-part-size", part_size.to_string())
        .send()
        .await
        .context("failed to create multipart upload")?;
    let upload_id = created
        .upload_id()
        .ok_or_else(|| anyhow!("R2 returned no upload ID"))?
        .to_owned();
    let operation = async {
        let part_count = local.length.div_ceil(part_size);
        ensure!(
            part_count > 0 && part_count <= 10_000,
            "invalid multipart part count"
        );
        let mut tasks = tokio::task::JoinSet::new();
        let mut next_part = 0u64;
        let mut uploaded = Vec::with_capacity(usize::try_from(part_count)?);
        while next_part < part_count || !tasks.is_empty() {
            while next_part < part_count && tasks.len() < concurrency {
                let part_index = next_part;
                next_part += 1;
                let client = client.clone();
                let bucket = bucket.to_owned();
                let key = key.clone();
                let upload_id = upload_id.clone();
                let path = local.archive_path.clone();
                let expected = local.identity;
                let offset = part_index * part_size;
                let length = usize::try_from((local.length - offset).min(part_size))?;
                tasks.spawn(async move {
                    let (bytes, digest, sha256) = tokio::task::spawn_blocking(move || {
                        read_part(&path, expected, offset, length)
                    })
                    .await??;
                    let content_md5 = BASE64_STANDARD.encode(&digest);
                    let part_number = i32::try_from(part_index + 1)?;
                    let mut response = None;
                    let mut last_error = None;
                    for attempt in 1..=MAX_PART_ATTEMPTS {
                        match client
                            .upload_part()
                            .bucket(&bucket)
                            .key(&key)
                            .upload_id(&upload_id)
                            .part_number(part_number)
                            .content_length(i64::try_from(length)?)
                            .content_md5(&content_md5)
                            .body(ByteStream::from(bytes.clone()))
                            .send()
                            .await
                        {
                            Ok(output) => {
                                response = Some(output);
                                break;
                            }
                            Err(error) => {
                                last_error = Some(format!("{error:?}"));
                                if attempt < MAX_PART_ATTEMPTS {
                                    tokio::time::sleep(std::time::Duration::from_secs(
                                        (1u64 << (attempt - 1)).min(30),
                                    ))
                                    .await;
                                }
                            }
                        }
                    }
                    let response = response.ok_or_else(|| {
                        anyhow!(
                            "R2 part {part_number} failed after {MAX_PART_ATTEMPTS} attempts: {}",
                            last_error.unwrap_or_else(|| "unknown error".to_owned())
                        )
                    })?;
                    let etag = response
                        .e_tag()
                        .ok_or_else(|| anyhow!("R2 part has no ETag"))?;
                    let digest_hex = hex_lower(&digest);
                    // R2 documents each part ETag as the MD5 of that part.
                    ensure!(normalize_etag(etag) == digest_hex, "R2 part ETag mismatch");
                    Ok::<_, anyhow::Error>((part_index, etag.to_owned(), digest, sha256))
                });
            }
            if let Some(result) = tasks.join_next().await {
                uploaded.push(result??);
            }
        }
        uploaded.sort_by_key(|item| item.0);
        let expected_etag = multipart_etag(
            &uploaded
                .iter()
                .map(|item| item.2.clone())
                .collect::<Vec<_>>(),
        );
        let expected_composite_sha256 = composite_sha256(
            &uploaded
                .iter()
                .map(|item| item.3.clone())
                .collect::<Vec<_>>(),
        );
        let parts = uploaded
            .into_iter()
            .map(|(index, etag, _, _)| {
                CompletedPart::builder()
                    .part_number(i32::try_from(index + 1).expect("part count bounded"))
                    .e_tag(etag)
                    .build()
            })
            .collect::<Vec<_>>();
        let request = client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(&key)
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            );
        let request = if overwrite_existing {
            request
        } else {
            request.if_none_match("*")
        };
        let response = request
            .send()
            .await
            .map_err(|error| anyhow!("failed to complete multipart upload: {error:?}"))?;
        let actual = response
            .e_tag()
            .ok_or_else(|| anyhow!("completed object has no ETag"))?;
        ensure!(
            normalize_etag(actual) == expected_etag,
            "completed multipart ETag mismatch"
        );
        Ok::<_, anyhow::Error>((expected_etag, expected_composite_sha256))
    }
    .await;
    if operation.is_err() {
        // This only discards an incomplete multipart staging area. It can never
        // remove or alter a completed R2 object.
        if let Err(error) = client
            .abort_multipart_upload()
            .bucket(bucket)
            .key(&key)
            .upload_id(&upload_id)
            .send()
            .await
        {
            eprintln!("warning: failed to abort incomplete multipart upload for {key}: {error:?}");
        }
    }
    operation
}

async fn put_sidecar(
    client: &Client,
    bucket: &str,
    local: &LocalArchive,
    overwrite_existing: bool,
) -> Result<()> {
    let key = format!("epoch-{}.jet.sha256", local.epoch);
    let content_md5 = BASE64_STANDARD.encode(Md5::digest(&local.sidecar));
    let request = client
        .put_object()
        .bucket(bucket)
        .key(&key)
        .content_type("text/plain; charset=us-ascii")
        .content_md5(content_md5)
        .body(ByteStream::from(local.sidecar.clone()));
    let request = if overwrite_existing {
        request
    } else {
        request.if_none_match("*")
    };
    match request.send().await {
        Ok(_) => Ok(()),
        Err(error) => {
            // A racing append-only writer may have won. It is acceptable only
            // if the canonical object is already byte-for-byte identical.
            if remote_sidecar(client, bucket, &key).await?.as_deref() == Some(&local.sidecar) {
                Ok(())
            } else {
                Err(error).context("failed to create immutable checksum sidecar")
            }
        }
    }
}

fn receipt_path(directory: &Path, epoch: u64) -> PathBuf {
    directory.join(format!("epoch-{epoch}.r2.json"))
}

fn write_receipt(directory: &Path, receipt: &Receipt, overwrite_existing: bool) -> Result<PathBuf> {
    fs::create_dir_all(directory)?;
    fs::set_permissions(
        directory,
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )?;
    let final_path = receipt_path(directory, receipt.epoch);
    if final_path.exists() {
        let existing: Receipt = serde_json::from_slice(&fs::read(&final_path)?)?;
        if same_receipt_evidence(&existing, receipt) {
            return Ok(final_path);
        }
        ensure!(
            overwrite_existing,
            "existing R2 receipt differs for epoch {}",
            receipt.epoch
        );
    }
    let temporary = directory.join(format!(
        ".epoch-{}.r2.{}.tmp",
        receipt.epoch,
        std::process::id()
    ));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)?;
    serde_json::to_writer_pretty(&mut file, receipt)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(&temporary, &final_path)?;
    File::open(directory)?.sync_all()?;
    Ok(final_path)
}

fn same_receipt_evidence(left: &Receipt, right: &Receipt) -> bool {
    left.schema == right.schema
        && left.bucket == right.bucket
        && left.epoch == right.epoch
        && left.archive_key == right.archive_key
        && left.checksum_key == right.checksum_key
        && left.archive_length == right.archive_length
        && left.archive_sha256 == right.archive_sha256
        && left.archive_etag == right.archive_etag
        && left.r2_composite_sha256 == right.r2_composite_sha256
        && left.remote_sha256_readback == right.remote_sha256_readback
        && left.multipart_part_size == right.multipart_part_size
}

fn read_restore_receipt(directory: &Path, bucket: &str, epoch: u64) -> Result<Receipt> {
    let path = receipt_path(directory, epoch);
    let before = regular_identity(&path)?;
    ensure!(before.length <= 16 * 1024, "oversized R2 receipt");
    let receipt: Receipt = serde_json::from_slice(&fs::read(&path)?)?;
    ensure!(
        regular_identity(&path)? == before,
        "R2 receipt changed while read"
    );
    ensure!(
        receipt.schema == RECEIPT_SCHEMA,
        "unsupported R2 receipt schema"
    );
    ensure!(
        receipt.bucket == bucket,
        "R2 receipt names a different bucket"
    );
    ensure!(receipt.epoch == epoch, "R2 receipt names a different epoch");
    ensure!(
        receipt.archive_key == format!("epoch-{epoch}.jet")
            && receipt.checksum_key == format!("epoch-{epoch}.jet.sha256"),
        "R2 receipt contains noncanonical object keys"
    );
    ensure!(
        receipt.archive_length > 0,
        "R2 receipt has an empty archive"
    );
    ensure!(
        receipt.archive_sha256.len() == 64
            && receipt
                .archive_sha256
                .bytes()
                .all(|byte| { byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase() }),
        "R2 receipt has an invalid archive SHA-256"
    );
    ensure!(
        receipt.multipart_part_size >= 5 * 1024 * 1024,
        "R2 receipt has an invalid multipart part size"
    );
    Ok(receipt)
}

fn canonical_sidecar(receipt: &Receipt) -> Vec<u8> {
    format!("{}  epoch-{}.jet\n", receipt.archive_sha256, receipt.epoch).into_bytes()
}

fn validate_restored_archive(path: &Path, receipt: &Receipt) -> Result<()> {
    let identity = regular_identity(path)?;
    ensure!(
        identity.length == receipt.archive_length,
        "restored archive length mismatch"
    );
    let proof = hash_and_etag(path, identity, receipt.multipart_part_size)?;
    ensure!(
        proof.sha256_hex == receipt.archive_sha256,
        "restored archive SHA-256 mismatch"
    );
    ensure!(
        proof.multipart_etag == receipt.archive_etag,
        "restored archive multipart ETag mismatch"
    );
    Ok(())
}

fn validate_restored_sidecar(path: &Path, expected: &[u8]) -> Result<()> {
    let identity = regular_identity(path)?;
    ensure!(
        identity.length <= MAX_SIDECAR_BYTES,
        "restored sidecar is oversized"
    );
    ensure!(fs::read(path)? == expected, "restored sidecar mismatch");
    ensure!(
        regular_identity(path)? == identity,
        "restored sidecar changed while read"
    );
    Ok(())
}

fn validate_remote_against_receipt(remote: &RemoteArchive, receipt: &Receipt) -> Result<()> {
    ensure!(
        remote.length == receipt.archive_length,
        "remote archive length differs from receipt"
    );
    ensure!(
        remote.etag == receipt.archive_etag,
        "remote archive ETag differs from receipt"
    );
    if let Some(metadata_sha256) = &remote.metadata_sha256 {
        ensure!(
            metadata_sha256 == &receipt.archive_sha256,
            "remote archive SHA-256 metadata differs from receipt"
        );
    }
    if let Some(expected) = &receipt.r2_composite_sha256 {
        ensure!(
            remote.checksum_type == Some(ChecksumType::Composite)
                && remote.composite_sha256.as_ref() == Some(expected),
            "remote composite SHA-256 differs from receipt"
        );
    }
    Ok(())
}

fn persist_noclobber(mut temporary: tempfile::NamedTempFile, destination: &Path) -> Result<()> {
    temporary.as_file_mut().sync_all()?;
    fs::set_permissions(
        temporary.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o440),
    )?;
    temporary.persist_noclobber(destination).map_err(|error| {
        anyhow!(
            "failed to publish {}: {}",
            destination.display(),
            error.error
        )
    })?;
    Ok(())
}

async fn read_remote_sha256_resumable<W: Write>(
    client: &Client,
    bucket: &str,
    key: &str,
    etag: &str,
    expected_length: u64,
    expected_sha256: &str,
    writer: &mut W,
) -> Result<()> {
    use tokio::io::AsyncReadExt as _;

    let mut digest = Sha256::new();
    let mut received = 0u64;
    let mut failures = 0usize;
    let mut buffer = vec![0u8; 8 * 1024 * 1024];
    while received < expected_length {
        let offset = received;
        let request = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .if_match(format!("\"{etag}\""));
        let request = if offset == 0 {
            request
        } else {
            request.range(format!("bytes={offset}-"))
        };
        let response = match request.send().await {
            Ok(response) => response,
            Err(error) => {
                failures += 1;
                ensure!(
                    failures <= MAX_REMOTE_READ_ATTEMPTS,
                    "remote read failed after {MAX_REMOTE_READ_ATTEMPTS} attempts: {error:?}"
                );
                eprintln!(
                    "warning: remote read retry {failures}/{MAX_REMOTE_READ_ATTEMPTS} at byte {offset}: {error:?}"
                );
                tokio::time::sleep(std::time::Duration::from_secs(
                    (1u64 << failures.saturating_sub(1).min(5)).min(30),
                ))
                .await;
                continue;
            }
        };
        let expected_response_length = expected_length - offset;
        ensure!(
            response
                .content_length()
                .and_then(|value| u64::try_from(value).ok())
                == Some(expected_response_length),
            "remote ranged read length mismatch at byte {offset}"
        );
        ensure!(
            response.e_tag().map(normalize_etag) == Some(etag),
            "remote ranged read ETag mismatch"
        );

        let mut reader = response.body.into_async_read();
        let mut stream_error = None;
        loop {
            match reader.read(&mut buffer).await {
                Ok(0) => break,
                Ok(count) => {
                    writer.write_all(&buffer[..count])?;
                    digest.update(&buffer[..count]);
                    received = received
                        .checked_add(u64::try_from(count)?)
                        .ok_or_else(|| anyhow!("remote read length overflow"))?;
                    ensure!(
                        received <= expected_length,
                        "remote read exceeded expected length"
                    );
                }
                Err(error) => {
                    stream_error = Some(error.to_string());
                    break;
                }
            }
        }
        if received == expected_length {
            break;
        }

        failures += 1;
        ensure!(
            failures <= MAX_REMOTE_READ_ATTEMPTS,
            "remote stream remained incomplete after {MAX_REMOTE_READ_ATTEMPTS} attempts"
        );
        eprintln!(
            "warning: remote stream retry {failures}/{MAX_REMOTE_READ_ATTEMPTS} at byte {received}: {}",
            stream_error.unwrap_or_else(|| "stream ended before expected length".to_owned())
        );
        tokio::time::sleep(std::time::Duration::from_secs(
            (1u64 << failures.saturating_sub(1).min(5)).min(30),
        ))
        .await;
    }
    ensure!(received == expected_length, "remote read was truncated");
    ensure!(
        format!("{:x}", digest.finalize()) == expected_sha256,
        "remote SHA-256 mismatch"
    );
    Ok(())
}

async fn restore_epoch(client: &Client, r2: &R2Config, config: &Config, epoch: u64) -> Result<()> {
    let receipt = read_restore_receipt(&config.receipt_directory, &r2.bucket, epoch)?;
    let archive_path = config.directory.join(&receipt.archive_key);
    let sidecar_path = config.directory.join(&receipt.checksum_key);
    let expected_sidecar = canonical_sidecar(&receipt);

    let remote = head_archive(client, &r2.bucket, &receipt.archive_key)
        .await?
        .ok_or_else(|| anyhow!("remote archive is missing"))?;
    validate_remote_against_receipt(&remote, &receipt)?;
    ensure!(
        remote_sidecar(client, &r2.bucket, &receipt.checksum_key)
            .await?
            .as_deref()
            == Some(expected_sidecar.as_slice()),
        "remote checksum sidecar differs from receipt"
    );

    let archive_exists = archive_path
        .try_exists()
        .context("failed to inspect restore destination")?;
    let sidecar_exists = sidecar_path
        .try_exists()
        .context("failed to inspect restore destination")?;
    if archive_exists {
        validate_restored_archive(&archive_path, &receipt)?;
    }
    if sidecar_exists {
        validate_restored_sidecar(&sidecar_path, &expected_sidecar)?;
    }
    if archive_exists && sidecar_exists {
        eprintln!("epoch {epoch}: verified local restore already exists");
        return Ok(());
    }

    if !archive_exists {
        eprintln!(
            "epoch {epoch}: restoring {} bytes from R2",
            receipt.archive_length
        );
        let mut temporary = tempfile::Builder::new()
            .prefix(&format!(".epoch-{epoch}.restore."))
            .tempfile_in(&config.directory)?;
        read_remote_sha256_resumable(
            client,
            &r2.bucket,
            &receipt.archive_key,
            &receipt.archive_etag,
            receipt.archive_length,
            &receipt.archive_sha256,
            temporary.as_file_mut(),
        )
        .await
        .context("failed to restore and hash R2 archive")?;
        persist_noclobber(temporary, &archive_path)?;
    }

    if !sidecar_exists {
        let mut temporary = tempfile::Builder::new()
            .prefix(&format!(".epoch-{epoch}.sidecar.restore."))
            .tempfile_in(&config.directory)?;
        temporary.as_file_mut().write_all(&expected_sidecar)?;
        persist_noclobber(temporary, &sidecar_path)?;
    }
    File::open(&config.directory)?.sync_all()?;

    validate_restored_archive(&archive_path, &receipt)?;
    validate_restored_sidecar(&sidecar_path, &expected_sidecar)?;
    let remote = head_archive(client, &r2.bucket, &receipt.archive_key)
        .await?
        .ok_or_else(|| anyhow!("remote archive disappeared after restore"))?;
    validate_remote_against_receipt(&remote, &receipt)?;
    ensure!(
        remote_sidecar(client, &r2.bucket, &receipt.checksum_key)
            .await?
            .as_deref()
            == Some(expected_sidecar.as_slice()),
        "remote checksum sidecar changed during restore"
    );
    eprintln!(
        "epoch {epoch}: restored and verified in {}",
        config.directory.display()
    );
    Ok(())
}

async fn verify_remote_sha256(
    client: &Client,
    bucket: &str,
    key: &str,
    etag: &str,
    expected_length: u64,
    expected_sha256: &str,
) -> Result<()> {
    read_remote_sha256_resumable(
        client,
        bucket,
        key,
        etag,
        expected_length,
        expected_sha256,
        &mut std::io::sink(),
    )
    .await
    .context("failed remote SHA-256 readback")
}

fn revalidate_local(local: &LocalArchive) -> Result<()> {
    ensure!(
        regular_identity(&local.archive_path)? == local.identity,
        "archive changed before retirement"
    );
    ensure!(
        regular_identity(&local.sidecar_path)? == local.sidecar_identity,
        "sidecar changed before retirement"
    );
    ensure!(
        fs::read(&local.sidecar_path)? == local.sidecar,
        "sidecar bytes changed before retirement"
    );
    Ok(())
}

fn retire_local(local: &LocalArchive) -> Result<()> {
    revalidate_local(local)?;
    fs::remove_file(&local.archive_path)?;
    fs::remove_file(&local.sidecar_path)?;
    File::open(
        local
            .archive_path
            .parent()
            .unwrap_or_else(|| Path::new(".")),
    )?
    .sync_all()?;
    Ok(())
}

fn validate_remote_pair_state(
    has_archive: bool,
    remote_sidecar: Option<&[u8]>,
    local_sidecar: &[u8],
    repair_orphaned_archive: bool,
) -> Result<bool> {
    if has_archive || remote_sidecar.is_none() {
        return Ok(false);
    }
    ensure!(
        repair_orphaned_archive,
        "remote completion sidecar exists without its archive"
    );
    ensure!(
        remote_sidecar == Some(local_sidecar),
        "orphaned remote completion sidecar differs from the verified local sidecar"
    );
    Ok(true)
}

async fn sync_epoch(client: &Client, r2: &R2Config, config: &Config, epoch: u64) -> Result<()> {
    let local = read_local_archive(&config.directory, epoch)?;
    let archive_key = format!("epoch-{epoch}.jet");
    let checksum_key = format!("epoch-{epoch}.jet.sha256");
    let existing = head_archive(client, &r2.bucket, &archive_key).await?;
    let existing_sidecar = remote_sidecar(client, &r2.bucket, &checksum_key).await?;
    let repairing_orphan = validate_remote_pair_state(
        existing.is_some(),
        existing_sidecar.as_deref(),
        &local.sidecar,
        config.repair_orphaned_archive,
    )
    .with_context(|| format!("invalid remote pair state for epoch {epoch}"))?;
    let was_existing = existing.is_some() && !config.overwrite_existing;
    let effective_part_size = if let Some(remote) = &existing {
        if config.overwrite_existing {
            config.part_size
        } else {
            remote
                .multipart_part_size
                .or(config.legacy_part_size)
                .ok_or_else(|| {
                    anyhow!("epoch {epoch} already exists but has no part-size metadata and --legacy-part-size-mib was not supplied")
                })?
        }
    } else {
        config.part_size
    };
    eprintln!("epoch {epoch}: hashing {}", local.archive_path.display());
    let path = local.archive_path.clone();
    let expected = local.identity;
    let proof =
        tokio::task::spawn_blocking(move || hash_and_etag(&path, expected, effective_part_size))
            .await??;
    ensure!(
        proof.sha256_hex == local.sha256_hex,
        "local SHA-256 mismatch for epoch {epoch}"
    );
    let (etag, _upload_composite_sha256) = if let Some(remote) =
        existing.filter(|_| !config.overwrite_existing)
    {
        ensure!(
            remote.length == local.length,
            "remote length mismatch for epoch {epoch}"
        );
        ensure!(
            remote.etag == proof.multipart_etag,
            "remote ETag mismatch for epoch {epoch}"
        );
        if let Some(metadata_sha256) = remote.metadata_sha256 {
            ensure!(
                metadata_sha256 == local.sha256_hex,
                "remote SHA-256 metadata mismatch"
            );
        }
        if let Some(checksum) = remote.composite_sha256.as_deref() {
            ensure!(
                remote.checksum_type == Some(ChecksumType::Composite),
                "unexpected remote checksum type"
            );
            ensure!(
                composite_checksum_matches(
                    checksum,
                    &proof.composite_sha256,
                    local.length.div_ceil(effective_part_size)
                ),
                "remote composite SHA-256 mismatch for epoch {epoch}"
            );
        }
        (remote.etag, remote.composite_sha256)
    } else {
        ensure!(
            config.command == Command::Sync,
            "remote archive is missing for epoch {epoch}"
        );
        if repairing_orphan {
            eprintln!("epoch {epoch}: repairing missing archive bound by matching remote sidecar");
        }
        eprintln!("epoch {epoch}: uploading {} bytes", local.length);
        let (etag, _locally_expected_composite_checksum) = upload_archive(
            client,
            &r2.bucket,
            &local,
            config.part_size,
            config.concurrency,
            config.overwrite_existing,
        )
        .await?;
        (etag, None)
    };
    let remote = head_archive(client, &r2.bucket, &archive_key)
        .await?
        .ok_or_else(|| anyhow!("remote archive disappeared for epoch {epoch}"))?;
    ensure!(
        remote.length == local.length && remote.etag == etag,
        "remote archive changed after verification"
    );
    if remote.metadata_sha256.is_some() {
        ensure!(
            remote.metadata_sha256.as_deref() == Some(local.sha256_hex.as_str()),
            "remote metadata changed"
        );
    }
    if let Some(checksum) = remote.composite_sha256.as_deref() {
        ensure!(
            remote.checksum_type == Some(ChecksumType::Composite),
            "remote checksum type changed"
        );
        ensure!(
            composite_checksum_matches(
                checksum,
                &proof.composite_sha256,
                local.length.div_ceil(effective_part_size),
            ),
            "remote composite SHA-256 changed after verification"
        );
    }
    let native_sha256_proven = remote.composite_sha256.is_some();
    let remote_sha256_readback =
        if native_sha256_proven || (was_existing && config.legacy_etag_only) {
            false
        } else {
            eprintln!("epoch {epoch}: reading the completed R2 object back for whole-file SHA-256");
            verify_remote_sha256(
                client,
                &r2.bucket,
                &archive_key,
                &etag,
                local.length,
                &local.sha256_hex,
            )
            .await?;
            true
        };
    revalidate_local(&local)?;

    // The sidecar is the public completion marker. Publish it only after the
    // complete archive has passed every available remote integrity proof and
    // the source files have been revalidated. Consumers may therefore treat a
    // canonical sidecar as meaning that the corresponding archive was fully
    // uploaded and verified by this publisher.
    match remote_sidecar(client, &r2.bucket, &checksum_key).await? {
        Some(_) if config.overwrite_existing => {
            put_sidecar(client, &r2.bucket, &local, true).await?
        }
        Some(bytes) => ensure!(
            bytes == local.sidecar,
            "remote sidecar mismatch for epoch {epoch}"
        ),
        None if config.command == Command::Sync => {
            put_sidecar(client, &r2.bucket, &local, false).await?
        }
        None => bail!("remote sidecar is missing for epoch {epoch}"),
    }
    ensure!(
        remote_sidecar(client, &r2.bucket, &checksum_key)
            .await?
            .as_deref()
            == Some(&local.sidecar),
        "remote sidecar readback failed"
    );
    let final_remote = head_archive(client, &r2.bucket, &archive_key)
        .await?
        .ok_or_else(|| anyhow!("remote archive disappeared after sidecar publication"))?;
    ensure!(
        final_remote.length == local.length && final_remote.etag == etag,
        "remote archive changed during sidecar publication"
    );
    if final_remote.metadata_sha256.is_some() {
        ensure!(
            final_remote.metadata_sha256.as_deref() == Some(local.sha256_hex.as_str()),
            "remote metadata changed during sidecar publication"
        );
    }
    if native_sha256_proven {
        ensure!(
            final_remote.checksum_type == Some(ChecksumType::Composite)
                && final_remote
                    .composite_sha256
                    .as_deref()
                    .is_some_and(|checksum| composite_checksum_matches(
                        checksum,
                        &proof.composite_sha256,
                        local.length.div_ceil(effective_part_size),
                    )),
            "remote composite SHA-256 changed during sidecar publication"
        );
    }
    let receipt = Receipt {
        schema: RECEIPT_SCHEMA.to_owned(),
        bucket: r2.bucket.clone(),
        epoch,
        archive_key,
        checksum_key,
        archive_length: local.length,
        archive_sha256: local.sha256_hex.clone(),
        archive_etag: etag,
        r2_composite_sha256: final_remote.composite_sha256,
        remote_sha256_readback,
        multipart_part_size: effective_part_size,
        verified_unix_seconds: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
    };
    let receipt_path = write_receipt(
        &config.receipt_directory,
        &receipt,
        config.overwrite_existing,
    )?;
    if config.delete_local {
        retire_local(&local)?;
        eprintln!(
            "epoch {epoch}: verified in R2, receipt {}, local pair retired",
            receipt_path.display()
        );
    } else {
        eprintln!(
            "epoch {epoch}: verified in R2, receipt {}",
            receipt_path.display()
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = parse_args()?;
    ensure!(
        config.directory.is_dir(),
        "archive directory does not exist"
    );
    let r2 = r2_config_from_env()?;
    let client = s3_client(&r2);
    for &epoch in &config.epochs {
        match config.command {
            Command::Restore => restore_epoch(&client, &r2, &config, epoch)
                .await
                .with_context(|| format!("epoch {epoch} R2 restore failed"))?,
            Command::Sync | Command::Verify => sync_epoch(&client, &r2, &config, epoch)
                .await
                .with_context(|| format!("epoch {epoch} R2 delivery failed"))?,
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ranges_and_mib() {
        assert_eq!(parse_epoch_range("7-9").unwrap(), BTreeSet::from([7, 8, 9]));
        assert!(parse_epoch_range("9-7").is_err());
        assert_eq!(parse_mib("64").unwrap(), 64 * 1024 * 1024);
        assert!(parse_mib("4").is_err());
    }

    #[test]
    fn discovers_sparse_local_pairs_only() {
        let directory = tempfile::tempdir().unwrap();
        fs::write(directory.path().join("epoch-1.jet"), b"one").unwrap();
        fs::write(directory.path().join("epoch-1.jet.sha256"), b"sidecar").unwrap();
        fs::write(directory.path().join("epoch-2.jet"), b"partial").unwrap();
        fs::write(directory.path().join("unrelated"), b"ignored").unwrap();
        assert_eq!(
            discover_local_epochs(directory.path()).unwrap(),
            BTreeSet::from([1])
        );
    }

    #[test]
    fn orphaned_remote_sidecar_repair_requires_an_exact_local_match() {
        let sidecar = b"digest  epoch-7.jet\n";
        assert!(!validate_remote_pair_state(true, Some(sidecar), sidecar, false).unwrap());
        assert!(!validate_remote_pair_state(false, None, sidecar, false).unwrap());
        assert!(validate_remote_pair_state(false, Some(sidecar), sidecar, false).is_err());
        assert!(validate_remote_pair_state(false, Some(b"other"), sidecar, true).is_err());
        assert!(validate_remote_pair_state(false, Some(sidecar), sidecar, true).unwrap());
    }

    #[test]
    fn receipt_time_is_not_part_of_identity() {
        let receipt = Receipt {
            schema: RECEIPT_SCHEMA.to_owned(),
            bucket: "bucket".to_owned(),
            epoch: 7,
            archive_key: "epoch-7.jet".to_owned(),
            checksum_key: "epoch-7.jet.sha256".to_owned(),
            archive_length: 10,
            archive_sha256: "ab".repeat(32),
            archive_etag: "etag".to_owned(),
            r2_composite_sha256: None,
            remote_sha256_readback: false,
            multipart_part_size: 5 * 1024 * 1024,
            verified_unix_seconds: 1,
        };
        let mut later = receipt.clone();
        later.verified_unix_seconds = 2;
        assert!(same_receipt_evidence(&receipt, &later));
    }

    #[test]
    fn receipt_replacement_requires_explicit_overwrite() {
        let directory = tempfile::tempdir().unwrap();
        let receipt = Receipt {
            schema: RECEIPT_SCHEMA.to_owned(),
            bucket: "bucket".to_owned(),
            epoch: 7,
            archive_key: "epoch-7.jet".to_owned(),
            checksum_key: "epoch-7.jet.sha256".to_owned(),
            archive_length: 10,
            archive_sha256: "ab".repeat(32),
            archive_etag: "old-etag".to_owned(),
            r2_composite_sha256: None,
            remote_sha256_readback: true,
            multipart_part_size: 5 * 1024 * 1024,
            verified_unix_seconds: 1,
        };
        write_receipt(directory.path(), &receipt, false).unwrap();

        let mut replacement = receipt.clone();
        replacement.archive_etag = "new-etag".to_owned();
        replacement.verified_unix_seconds = 2;
        assert!(write_receipt(directory.path(), &replacement, false).is_err());
        write_receipt(directory.path(), &replacement, true).unwrap();

        let stored: Receipt = serde_json::from_slice(
            &fs::read(receipt_path(directory.path(), replacement.epoch)).unwrap(),
        )
        .unwrap();
        assert_eq!(stored, replacement);
    }

    #[test]
    fn restore_receipt_accepts_legacy_evidence_for_fresh_readback() {
        let directory = tempfile::tempdir().unwrap();
        let receipt = Receipt {
            schema: RECEIPT_SCHEMA.to_owned(),
            bucket: "bucket".to_owned(),
            epoch: 7,
            archive_key: "epoch-7.jet".to_owned(),
            checksum_key: "epoch-7.jet.sha256".to_owned(),
            archive_length: 10,
            archive_sha256: "ab".repeat(32),
            archive_etag: "etag".to_owned(),
            r2_composite_sha256: None,
            remote_sha256_readback: false,
            multipart_part_size: 5 * 1024 * 1024,
            verified_unix_seconds: 1,
        };
        fs::write(
            receipt_path(directory.path(), receipt.epoch),
            serde_json::to_vec(&receipt).unwrap(),
        )
        .unwrap();
        assert_eq!(
            read_restore_receipt(directory.path(), "bucket", 7).unwrap(),
            receipt
        );

        let mut proven = receipt.clone();
        proven.remote_sha256_readback = true;
        fs::write(
            receipt_path(directory.path(), proven.epoch),
            serde_json::to_vec(&proven).unwrap(),
        )
        .unwrap();
        assert_eq!(
            read_restore_receipt(directory.path(), "bucket", 7).unwrap(),
            proven
        );
        assert!(read_restore_receipt(directory.path(), "other", 7).is_err());
    }

    #[test]
    fn restore_publish_never_clobbers_existing_file() {
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("epoch-7.jet");
        fs::write(&destination, b"existing").unwrap();
        let mut temporary = tempfile::NamedTempFile::new_in(directory.path()).unwrap();
        temporary.write_all(b"replacement").unwrap();
        assert!(persist_noclobber(temporary, &destination).is_err());
        assert_eq!(fs::read(destination).unwrap(), b"existing");
    }

    #[test]
    fn restored_archive_is_bound_to_receipt_hash_and_etag() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("epoch-7.jet");
        fs::write(&path, b"archive").unwrap();
        let part_size = 5 * 1024 * 1024;
        let proof = hash_and_etag(&path, regular_identity(&path).unwrap(), part_size).unwrap();
        let receipt = Receipt {
            schema: RECEIPT_SCHEMA.to_owned(),
            bucket: "bucket".to_owned(),
            epoch: 7,
            archive_key: "epoch-7.jet".to_owned(),
            checksum_key: "epoch-7.jet.sha256".to_owned(),
            archive_length: 7,
            archive_sha256: proof.sha256_hex,
            archive_etag: proof.multipart_etag,
            r2_composite_sha256: None,
            remote_sha256_readback: true,
            multipart_part_size: part_size,
            verified_unix_seconds: 1,
        };
        validate_restored_archive(&path, &receipt).unwrap();

        fs::write(&path, b"changed").unwrap();
        assert!(validate_restored_archive(&path, &receipt).is_err());
    }

    #[test]
    fn validates_canonical_sidecar() {
        let directory = tempfile::tempdir().unwrap();
        let archive = directory.path().join("epoch-7.jet");
        fs::write(&archive, b"archive").unwrap();
        let digest = format!("{:x}", Sha256::digest(b"archive"));
        fs::write(
            directory.path().join("epoch-7.jet.sha256"),
            format!("{digest}  epoch-7.jet\n"),
        )
        .unwrap();
        let local = read_local_archive(directory.path(), 7).unwrap();
        assert_eq!(local.sha256_hex, digest);
    }

    #[test]
    fn computes_standard_multipart_etag() {
        let parts = vec![Md5::digest(b"a").to_vec(), Md5::digest(b"b").to_vec()];
        let mut combined = Md5::new();
        combined.update(&parts[0]);
        combined.update(&parts[1]);
        assert_eq!(
            multipart_etag(&parts),
            format!("{:x}-2", combined.finalize())
        );
    }

    #[test]
    fn renders_part_digest_as_etag_hex_without_rehashing() {
        let digest = Md5::digest(b"part");
        assert_eq!(hex_lower(&digest), format!("{digest:x}"));
    }

    #[test]
    fn hash_proof_binds_sha_and_etag() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("archive");
        fs::write(&path, b"abcdefgh").unwrap();
        let proof = hash_and_etag(&path, regular_identity(&path).unwrap(), 5).unwrap();
        assert_eq!(
            proof.sha256_hex,
            format!("{:x}", Sha256::digest(b"abcdefgh"))
        );
        let expected =
            multipart_etag(&[Md5::digest(b"abcde").to_vec(), Md5::digest(b"fgh").to_vec()]);
        assert_eq!(proof.multipart_etag, expected);
        let expected_sha = composite_sha256(&[
            Sha256::digest(b"abcde").to_vec(),
            Sha256::digest(b"fgh").to_vec(),
        ]);
        assert_eq!(proof.composite_sha256, expected_sha);
    }

    #[test]
    fn source_has_no_completed_object_delete_operation() {
        let source = include_str!("main.rs");
        let forbidden = [concat!("delete_", "object"), concat!("delete_", "objects")];
        for operation in forbidden {
            assert!(
                !source.contains(operation),
                "forbidden R2 operation: {operation}"
            );
        }
    }

    #[test]
    fn sidecar_publication_follows_archive_integrity_proof() {
        let source = include_str!("main.rs");
        let start = source.find("async fn sync_epoch(").unwrap();
        let end = source[start..].find("\nasync fn main(").unwrap() + start;
        let sync_epoch = &source[start..end];
        let remote_readback = sync_epoch.find("verify_remote_sha256(").unwrap();
        let local_revalidation = sync_epoch.find("revalidate_local(&local)").unwrap();
        let sidecar_publication = sync_epoch.find("put_sidecar(").unwrap();
        assert!(remote_readback < sidecar_publication);
        assert!(local_revalidation < sidecar_publication);
        assert!(
            sync_epoch.contains("validate_remote_pair_state(")
                && sync_epoch.contains("existing_sidecar.as_deref()")
                && sync_epoch.contains("&local.sidecar"),
            "orphan completion markers must be checked against the local sidecar"
        );
    }
}
