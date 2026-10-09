//! Narrow archive qualification for canonically conflicting account writes.

use {
    super::{ArchiveFormatError, ArchiveReader, Consumption, SlotKind, SlotVisitor},
    crate::transactions::Transaction,
    solana_address::Address,
    solana_signature::Signature,
    std::io::{Read, Seek},
    thiserror::Error,
};

/// One canonical transaction and its expected write to the shared account.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpectedConflictWrite {
    pub transaction_index: u32,
    pub signature: Signature,
    pub lamports: u64,
}

/// Evidence decoded from one of the two canonical conflicting transactions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConflictWriteEvidence {
    pub transaction_index: u32,
    pub signature: Signature,
    pub lamports: u64,
    pub write_version: u64,
}

#[derive(Debug, Error)]
pub enum ConflictSlotError {
    #[error("archive read failed: {0}")]
    Archive(#[from] ArchiveFormatError),
    #[error("{0}")]
    Validation(String),
}

struct ConflictVisitor {
    slot: u64,
    account: Address,
    expected: [ExpectedConflictWrite; 2],
    saw_slot: bool,
    observed: Vec<ConflictWriteEvidence>,
    error: Option<String>,
}

impl ConflictVisitor {
    fn record_error(&mut self, message: impl Into<String>) {
        if self.error.is_none() {
            self.error = Some(message.into());
        }
    }

    fn finish(self) -> Result<[ConflictWriteEvidence; 2], ConflictSlotError> {
        if let Some(error) = self.error {
            return Err(ConflictSlotError::Validation(error));
        }
        if !self.saw_slot {
            return Err(ConflictSlotError::Validation(format!(
                "slot {} is absent from the archive",
                self.slot
            )));
        }
        let observed: [ConflictWriteEvidence; 2] =
            self.observed.try_into().map_err(|writes: Vec<_>| {
                ConflictSlotError::Validation(format!(
                    "expected exactly two matching writes in slot {}, observed {}",
                    self.slot,
                    writes.len()
                ))
            })?;
        if observed[0].transaction_index >= observed[1].transaction_index {
            return Err(ConflictSlotError::Validation(format!(
                "observed transaction indices are not increasing: {} then {}",
                observed[0].transaction_index, observed[1].transaction_index
            )));
        }
        if observed[0].write_version >= observed[1].write_version {
            return Err(ConflictSlotError::Validation(format!(
                "write versions are not increasing in transaction order: {} then {}",
                observed[0].write_version, observed[1].write_version
            )));
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
        self.observed.push(ConflictWriteEvidence {
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

/// Seek to one canonical slot and prove that two expected transactions both
/// succeeded, retained their shared-account writes, and preserved write order.
pub fn verify_conflict_slot<R: Read + Seek>(
    reader: &mut ArchiveReader<R>,
    slot: u64,
    account: Address,
    expected: [ExpectedConflictWrite; 2],
) -> Result<[ConflictWriteEvidence; 2], ConflictSlotError> {
    if expected[0].transaction_index >= expected[1].transaction_index {
        return Err(ConflictSlotError::Validation(
            "expected transaction indices must be strictly increasing".to_string(),
        ));
    }
    let mut visitor = ConflictVisitor {
        slot,
        account,
        expected,
        saw_slot: false,
        observed: Vec::with_capacity(2),
        error: None,
    };
    let slots_read = reader.read_slots(slot, 1, &mut visitor)?;
    if slots_read != 1 {
        return Err(ConflictSlotError::Validation(format!(
            "expected one slot at {slot}, reader returned {slots_read}"
        )));
    }
    visitor.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn visitor(observed: Vec<ConflictWriteEvidence>) -> ConflictVisitor {
        ConflictVisitor {
            slot: 42,
            account: Address::new_from_array([3; 32]),
            expected: [
                ExpectedConflictWrite {
                    transaction_index: 7,
                    signature: Signature::from([1; 64]),
                    lamports: 11,
                },
                ExpectedConflictWrite {
                    transaction_index: 8,
                    signature: Signature::from([2; 64]),
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
            ConflictWriteEvidence {
                transaction_index: 7,
                signature: Signature::from([1; 64]),
                lamports: 11,
                write_version: 100,
            },
            ConflictWriteEvidence {
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
            ConflictWriteEvidence {
                transaction_index: 7,
                signature: Signature::from([1; 64]),
                lamports: 11,
                write_version: 101,
            },
            ConflictWriteEvidence {
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
                .to_string()
                .contains("not increasing")
        );
    }
}
