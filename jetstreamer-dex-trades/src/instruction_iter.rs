//! Instruction iterators for traversing outer and inner instructions.
//!
//! Provides a unified [`Instruction`] enum and extension traits that let
//! callers iterate over every instruction in a transaction (top-level and
//! inner/CPI) with resolved account addresses and program IDs.

use jetstreamer_firehose::firehose::TransactionData;
use solana_address::Address;
use solana_message::VersionedMessage;

#[derive(Debug, Clone)]
struct CompiledIx {
    program_id_index: u8,
    accounts: Vec<u8>,
    data: Vec<u8>,
}

/// Unified instruction type (outer or inner).
#[derive(Debug, Clone)]
pub enum Instruction {
    /// A top-level instruction from the transaction message.
    TopLevel {
        /// 1-based index within the transaction.
        index: usize,
        program_id: Address,
        accounts: Vec<Address>,
        data: Vec<u8>,
    },
    /// An inner instruction produced by a CPI.
    Inner {
        /// 1-based index of the parent outer instruction.
        parent_index: usize,
        /// 1-based index within the parent's inner instruction list.
        index: usize,
        program_id: Address,
        accounts: Vec<Address>,
        data: Vec<u8>,
        stack_height: Option<u32>,
    },
}

impl Instruction {
    pub fn program_id(&self) -> &Address {
        match self {
            Self::TopLevel { program_id, .. } | Self::Inner { program_id, .. } => program_id,
        }
    }

    pub fn data(&self) -> &[u8] {
        match self {
            Self::TopLevel { data, .. } | Self::Inner { data, .. } => data,
        }
    }

    pub fn accounts(&self) -> &[Address] {
        match self {
            Self::TopLevel { accounts, .. } | Self::Inner { accounts, .. } => accounts,
        }
    }

    pub fn is_inner(&self) -> bool {
        matches!(self, Self::Inner { .. })
    }

    pub fn instruction_index(&self) -> u32 {
        match self {
            Self::TopLevel { index, .. } => *index as u32,
            Self::Inner { parent_index, .. } => *parent_index as u32,
        }
    }

    pub fn inner_instruction_index(&self) -> u32 {
        match self {
            Self::TopLevel { .. } => 0,
            Self::Inner { index, .. } => *index as u32,
        }
    }

    pub fn has_discriminator(&self, disc: &[u8]) -> bool {
        let data = self.data();
        data.len() >= disc.len() && &data[..disc.len()] == disc
    }
}

/// Extension trait for [`TransactionData`] providing instruction iterators.
pub trait InstructionIteratorExt {
    fn all_instructions(&self) -> AllInstructionIter;
}

impl InstructionIteratorExt for TransactionData {
    fn all_instructions(&self) -> AllInstructionIter {
        AllInstructionIter::new(self)
    }
}

/// Iterator over all instructions (outer + inner) in execution order.
pub struct AllInstructionIter {
    account_keys: Vec<Address>,
    instructions: Vec<CompiledIx>,
    inner_instructions: Vec<solana_transaction_status::InnerInstructions>,
    outer_index: usize,
    inner_ix_index: usize,
    in_inner_mode: bool,
}

impl AllInstructionIter {
    pub fn new(tx: &TransactionData) -> Self {
        let account_keys = get_account_keys(tx);
        let instructions = get_instructions(tx);
        let inner_instructions = tx
            .transaction_status_meta
            .inner_instructions
            .clone()
            .unwrap_or_default();

        Self {
            account_keys,
            instructions,
            inner_instructions,
            outer_index: 1,
            inner_ix_index: 1,
            in_inner_mode: false,
        }
    }

    fn get_inner_set(
        &self,
        outer_array_index: usize,
    ) -> Option<&solana_transaction_status::InnerInstructions> {
        self.inner_instructions
            .iter()
            .find(|inner_set| inner_set.index as usize == outer_array_index)
    }
}

impl Iterator for AllInstructionIter {
    type Item = Instruction;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let outer_array_index = self.outer_index - 1;

            if outer_array_index >= self.instructions.len() {
                return None;
            }

            if !self.in_inner_mode {
                let ix = &self.instructions[outer_array_index];
                let program_id = self.account_keys.get(ix.program_id_index as usize)?.clone();
                let accounts = ix
                    .accounts
                    .iter()
                    .filter_map(|&idx| self.account_keys.get(idx as usize).cloned())
                    .collect();

                let result = Instruction::TopLevel {
                    index: self.outer_index,
                    program_id,
                    accounts,
                    data: ix.data.clone(),
                };

                self.in_inner_mode = true;
                self.inner_ix_index = 1;

                return Some(result);
            }

            if let Some(inner_set) = self.get_inner_set(outer_array_index) {
                let inner_array_index = self.inner_ix_index - 1;

                if inner_array_index < inner_set.instructions.len() {
                    let inner_ix = &inner_set.instructions[inner_array_index];
                    let program_id = self
                        .account_keys
                        .get(inner_ix.instruction.program_id_index as usize)?
                        .clone();
                    let accounts: Vec<Address> = inner_ix
                        .instruction
                        .accounts
                        .iter()
                        .filter_map(|&idx| self.account_keys.get(idx as usize).cloned())
                        .collect();

                    let result = Instruction::Inner {
                        parent_index: self.outer_index,
                        index: self.inner_ix_index,
                        program_id,
                        accounts,
                        data: inner_ix.instruction.data.clone(),
                        stack_height: inner_ix.stack_height,
                    };

                    self.inner_ix_index += 1;
                    return Some(result);
                }
            }

            self.in_inner_mode = false;
            self.outer_index += 1;
        }
    }
}

fn get_account_keys(tx: &TransactionData) -> Vec<Address> {
    match &tx.transaction.message {
        VersionedMessage::Legacy(msg) => msg.account_keys.clone(),
        VersionedMessage::V0(msg) => {
            let mut keys = msg.account_keys.clone();
            let loaded = &tx.transaction_status_meta.loaded_addresses;
            keys.extend_from_slice(&loaded.writable);
            keys.extend_from_slice(&loaded.readonly);
            keys
        }
    }
}

fn get_instructions(tx: &TransactionData) -> Vec<CompiledIx> {
    let message = &tx.transaction.message;
    match message {
        VersionedMessage::Legacy(msg) => msg
            .instructions
            .iter()
            .map(|ix| CompiledIx {
                program_id_index: ix.program_id_index,
                accounts: ix.accounts.clone(),
                data: ix.data.clone(),
            })
            .collect(),
        VersionedMessage::V0(msg) => msg
            .instructions
            .iter()
            .map(|ix| CompiledIx {
                program_id_index: ix.program_id_index,
                accounts: ix.accounts.clone(),
                data: ix.data.clone(),
            })
            .collect(),
    }
}
