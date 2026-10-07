//! Verify two canonically successful transactions that write the same account.
//!
//! This is a narrow qualification gate for historical runtime candidates.  It
//! proves that a conflict barrier did not turn the second transaction into an
//! `AccountInUse` failure and that both writes retained their transaction
//! attribution and wire order in the resulting archive.
//!
//! Usage:
//! `verify_conflict_slot ARCHIVE SLOT ACCOUNT FIRST_INDEX FIRST_SIGNATURE FIRST_LAMPORTS SECOND_INDEX SECOND_SIGNATURE SECOND_LAMPORTS`

use std::{fs::File, io::BufReader, path::PathBuf, str::FromStr};

use jetstreamer_horizon::{
    archive::{ArchiveReader, Consumption, SlotKind, SlotVisitor},
    transactions::Transaction,
};
use solana_address::Address;
use solana_signature::Signature;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExpectedWrite {
    transaction_index: u32,
    signature: Signature,
    lamports: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedWrite {
    transaction_index: u32,
    signature: Signature,
    lamports: u64,
    write_version: u64,
}

struct ConflictVisitor {
    slot: u64,
    account: Address,
    expected: [ExpectedWrite; 2],
    saw_slot: bool,
    observed: Vec<ObservedWrite>,
    error: Option<String>,
}

impl ConflictVisitor {
    fn record_error(&mut self, message: impl Into<String>) {
        if self.error.is_none() {
            self.error = Some(message.into());
        }
    }

    fn finish(self) -> Result<[ObservedWrite; 2], String> {
        if let Some(error) = self.error {
            return Err(error);
        }
        if !self.saw_slot {
            return Err(format!("slot {} is absent from the archive", self.slot));
        }
        let observed: [ObservedWrite; 2] = self.observed.try_into().map_err(|writes: Vec<_>| {
            format!(
                "expected exactly two matching writes in slot {}, observed {}",
                self.slot,
                writes.len()
            )
        })?;
        if observed[0].write_version >= observed[1].write_version {
            return Err(format!(
                "write versions are not increasing in transaction order: {} then {}",
                observed[0].write_version, observed[1].write_version
            ));
        }
        Ok(observed)
    }
}

impl SlotVisitor for ConflictVisitor {
    fn on_slot_start(&mut self, slot: u64, kind: SlotKind) {
        if slot != self.slot {
            self.record_error(format!("reader returned unexpected slot {slot}"));
            return;
        }
        if kind != SlotKind::Block {
            self.record_error(format!("slot {slot} is {kind:?}, expected a block"));
            return;
        }
        self.saw_slot = true;
    }

    fn on_transaction(&mut self, slot: u64, transaction_index: u32, tx: &Transaction) {
        if slot != self.slot {
            self.record_error(format!("transaction belongs to unexpected slot {slot}"));
            return;
        }
        let Some(expected) = self
            .expected
            .iter()
            .find(|expected| expected.transaction_index == transaction_index)
        else {
            return;
        };

        let Some(signature) = tx.signatures.first().copied() else {
            self.record_error(format!("transaction {transaction_index} has no signature"));
            return;
        };
        if signature != expected.signature {
            self.record_error(format!(
                "transaction {transaction_index} signature mismatch: expected {}, got {}",
                expected.signature, signature
            ));
            return;
        }
        if !tx.status.is_ok() {
            self.record_error(format!(
                "transaction {transaction_index} ({signature}) failed: {:?}",
                tx.status
            ));
            return;
        }

        let mut writes = tx
            .iter_account_updates()
            .filter(|(update, _)| update.pubkey == self.account);
        let Some((update, _)) = writes.next() else {
            self.record_error(format!(
                "transaction {transaction_index} ({signature}) has no update for {}",
                self.account
            ));
            return;
        };
        if writes.next().is_some() {
            self.record_error(format!(
                "transaction {transaction_index} ({signature}) has multiple updates for {}",
                self.account
            ));
            return;
        }
        if update.lamports != expected.lamports {
            self.record_error(format!(
                "transaction {transaction_index} ({signature}) lamports mismatch for {}: expected {}, got {}",
                self.account, expected.lamports, update.lamports
            ));
            return;
        }
        self.observed.push(ObservedWrite {
            transaction_index,
            signature,
            lamports: update.lamports,
            write_version: update.write_version,
        });
    }

    fn consumption(&self) -> Consumption {
        Consumption::all()
            .without_account_update_data()
            .without_block_account_update_arenas()
    }
}

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
    let first = ExpectedWrite {
        transaction_index: parse(args.next().unwrap_or_else(|| usage()), "first index"),
        signature: parse(args.next().unwrap_or_else(|| usage()), "first signature"),
        lamports: parse(args.next().unwrap_or_else(|| usage()), "first lamports"),
    };
    let second = ExpectedWrite {
        transaction_index: parse(args.next().unwrap_or_else(|| usage()), "second index"),
        signature: parse(args.next().unwrap_or_else(|| usage()), "second signature"),
        lamports: parse(args.next().unwrap_or_else(|| usage()), "second lamports"),
    };
    if args.next().is_some() {
        usage();
    }
    if first.transaction_index >= second.transaction_index {
        panic!("transaction indices must be strictly increasing");
    }

    let file =
        File::open(&archive).unwrap_or_else(|error| panic!("open {}: {error}", archive.display()));
    let mut reader = ArchiveReader::open(BufReader::new(file))
        .unwrap_or_else(|error| panic!("open archive {}: {error}", archive.display()));
    let mut visitor = ConflictVisitor {
        slot,
        account,
        expected: [first, second],
        saw_slot: false,
        observed: Vec::with_capacity(2),
        error: None,
    };
    let slots_read = reader
        .read_slots(slot, 1, &mut visitor)
        .unwrap_or_else(|error| panic!("read slot {slot} from {}: {error}", archive.display()));
    if slots_read != 1 {
        panic!("expected one slot at {slot}, reader returned {slots_read}");
    }
    let observed = visitor
        .finish()
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

#[cfg(test)]
mod tests {
    use super::*;

    fn visitor(observed: Vec<ObservedWrite>) -> ConflictVisitor {
        let account = Address::new_from_array([3; 32]);
        let first_signature = Signature::from([1; 64]);
        let second_signature = Signature::from([2; 64]);
        ConflictVisitor {
            slot: 42,
            account,
            expected: [
                ExpectedWrite {
                    transaction_index: 7,
                    signature: first_signature,
                    lamports: 11,
                },
                ExpectedWrite {
                    transaction_index: 8,
                    signature: second_signature,
                    lamports: 10,
                },
            ],
            saw_slot: true,
            observed,
            error: None,
        }
    }

    #[test]
    fn accepts_two_ordered_writes() {
        let writes = vec![
            ObservedWrite {
                transaction_index: 7,
                signature: Signature::from([1; 64]),
                lamports: 11,
                write_version: 100,
            },
            ObservedWrite {
                transaction_index: 8,
                signature: Signature::from([2; 64]),
                lamports: 10,
                write_version: 101,
            },
        ];
        assert!(visitor(writes).finish().is_ok());
    }

    #[test]
    fn rejects_non_increasing_write_versions() {
        let writes = vec![
            ObservedWrite {
                transaction_index: 7,
                signature: Signature::from([1; 64]),
                lamports: 11,
                write_version: 101,
            },
            ObservedWrite {
                transaction_index: 8,
                signature: Signature::from([2; 64]),
                lamports: 10,
                write_version: 100,
            },
        ];
        assert!(
            visitor(writes)
                .finish()
                .unwrap_err()
                .contains("not increasing")
        );
    }
}
