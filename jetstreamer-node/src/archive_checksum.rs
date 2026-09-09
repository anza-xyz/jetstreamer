use std::{
    ffi::OsStr,
    fs::{self, File},
    io::{self, BufReader, Read, Write},
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};
use tempfile::Builder;

pub const ARCHIVE_CHECKSUM_SUFFIX: &str = ".sha256";

pub fn archive_checksum_path(archive_path: impl AsRef<Path>) -> io::Result<PathBuf> {
    let archive_path = archive_path.as_ref();
    let file_name = archive_path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("archive path {} has no filename", archive_path.display()),
        )
    })?;
    let mut checksum_name = file_name.to_os_string();
    checksum_name.push(ARCHIVE_CHECKSUM_SUFFIX);
    Ok(archive_path.with_file_name(checksum_name))
}

fn sha256_file(archive_path: &Path) -> io::Result<[u8; 32]> {
    let file = File::open(archive_path)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("archive is not a regular file: {}", archive_path.display()),
        ));
    }
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 128 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().into())
}

fn checksum_line(digest: &[u8; 32], file_name: &OsStr) -> io::Result<String> {
    let file_name = file_name.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "archive checksum filename is not UTF-8",
        )
    })?;
    Ok(format!(
        "{}  {file_name}\n",
        crate::segment_manifest::sha256_hex_string(digest)
    ))
}

/// Computes the archive digest and creates or repairs its standard adjacent
/// checksum using a synced same-directory temporary file and atomic rename.
/// Callers must complete authoritative archive validation before invoking it;
/// this sidecar is never an archive-validation substitute.
pub fn ensure_archive_checksum(archive_path: impl AsRef<Path>) -> io::Result<PathBuf> {
    let archive_path = archive_path.as_ref();
    let digest = sha256_file(archive_path)?;
    let line = checksum_line(
        &digest,
        archive_path.file_name().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("archive path {} has no filename", archive_path.display()),
            )
        })?,
    )?;
    let checksum_path = archive_checksum_path(archive_path)?;
    match fs::symlink_metadata(&checksum_path) {
        Ok(metadata) if !metadata.file_type().is_file() => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "archive checksum path is not a regular file: {}",
                    checksum_path.display()
                ),
            ));
        }
        Ok(metadata) => {
            if metadata.len() == line.len() as u64 && fs::read(&checksum_path)? == line.as_bytes() {
                return Ok(checksum_path);
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    let parent = checksum_path.parent().unwrap_or_else(|| Path::new("."));
    let mut temporary = Builder::new()
        .prefix(".jetstreamer-archive-checksum-")
        .suffix(".partial")
        .tempfile_in(parent)?;
    temporary.write_all(line.as_bytes())?;
    temporary.flush()?;
    temporary.as_file().sync_all()?;
    let published = temporary
        .persist(&checksum_path)
        .map_err(|error| error.error)?;
    published.sync_all()?;
    File::open(parent)?.sync_all()?;
    Ok(checksum_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksum_line_is_lowercase_coreutils_format() {
        let line = checksum_line(&[0xab; 32], OsStr::new("epoch-7.jet")).unwrap();
        assert_eq!(line, format!("{}  epoch-7.jet\n", "ab".repeat(32)));
    }

    #[test]
    fn checksum_is_atomically_created_and_repaired_adjacent_to_archive() {
        let directory = tempfile::TempDir::new().unwrap();
        let archive = directory.path().join("epoch-7.jet");
        fs::write(&archive, b"verified archive bytes").unwrap();

        let checksum = ensure_archive_checksum(&archive).unwrap();
        assert_eq!(checksum, directory.path().join("epoch-7.jet.sha256"));
        let expected = format!(
            "{}  epoch-7.jet\n",
            crate::segment_manifest::sha256_hex_string(&sha256_file(&archive).unwrap())
        );
        assert_eq!(fs::read_to_string(&checksum).unwrap(), expected);

        fs::write(&checksum, "stale\n").unwrap();
        ensure_archive_checksum(&archive).unwrap();
        assert_eq!(fs::read_to_string(&checksum).unwrap(), expected);
        assert!(fs::read_dir(directory.path()).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains(".partial")
        }));
    }
}
