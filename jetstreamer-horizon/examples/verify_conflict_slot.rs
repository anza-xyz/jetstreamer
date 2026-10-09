//! Verify two canonically successful transactions that write the same account.
//!
//! This is a narrow qualification gate for historical runtime candidates. It
//! proves that a conflict barrier did not turn the second transaction into an
//! `AccountInUse` failure and that both writes retained their transaction
//! attribution and wire order in the resulting archive.
//!
//! Usage:
//! `verify_conflict_slot ARCHIVE SLOT ACCOUNT FIRST_INDEX FIRST_SIGNATURE FIRST_LAMPORTS SECOND_INDEX SECOND_SIGNATURE SECOND_LAMPORTS`

use std::{fs::File, io::BufReader, path::PathBuf, str::FromStr};

use jetstreamer_horizon::archive::{ArchiveReader, ExpectedConflictWrite, verify_conflict_slot};

fn usage() -> ! {
    eprintln!(
        "usage: verify_conflict_slot ARCHIVE SLOT ACCOUNT FIRST_INDEX FIRST_SIGNATURE FIRST_LAMPORTS SECOND_INDEX SECOND_SIGNATURE SECOND_LAMPORTS"
    );
    std::process::exit(2);
}

fn parse<T: FromStr>(value: String, name: &str) -> T
where
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .unwrap_or_else(|error| panic!("invalid {name} {value:?}: {error}"))
}

fn main() {
    let mut args = std::env::args().skip(1);
    let archive = args.next().map(PathBuf::from).unwrap_or_else(|| usage());
    let slot = parse(args.next().unwrap_or_else(|| usage()), "slot");
    let account = parse(args.next().unwrap_or_else(|| usage()), "account");
    let first = ExpectedConflictWrite {
        transaction_index: parse(args.next().unwrap_or_else(|| usage()), "first index"),
        signature: parse(args.next().unwrap_or_else(|| usage()), "first signature"),
        lamports: parse(args.next().unwrap_or_else(|| usage()), "first lamports"),
    };
    let second = ExpectedConflictWrite {
        transaction_index: parse(args.next().unwrap_or_else(|| usage()), "second index"),
        signature: parse(args.next().unwrap_or_else(|| usage()), "second signature"),
        lamports: parse(args.next().unwrap_or_else(|| usage()), "second lamports"),
    };
    if args.next().is_some() {
        usage();
    }

    let file =
        File::open(&archive).unwrap_or_else(|error| panic!("open {}: {error}", archive.display()));
    let mut reader = ArchiveReader::open(BufReader::new(file))
        .unwrap_or_else(|error| panic!("open archive {}: {error}", archive.display()));
    let observed = verify_conflict_slot(&mut reader, slot, account, [first, second])
        .unwrap_or_else(|error| panic!("conflict-slot verification failed: {error}"));
    println!(
        "verified slot {slot}: tx {} {} wrote {} lamports at write_version {}; tx {} {} wrote {} lamports at write_version {}",
        observed[0].transaction_index,
        observed[0].signature,
        observed[0].lamports,
        observed[0].write_version,
        observed[1].transaction_index,
        observed[1].signature,
        observed[1].lamports,
        observed[1].write_version,
    );
}
