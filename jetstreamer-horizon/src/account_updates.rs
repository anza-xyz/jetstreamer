use agave_geyser_plugin_interface::geyser_plugin_interface::{
    ReplicaAccountInfo, ReplicaAccountInfoV2,
};
use lencode::prelude::*;
use solana_pubkey::Pubkey;

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(C)]
pub struct AccountUpdate {
    pub pubkey: Pubkey,
    pub lamports: u64,
    pub owner: Pubkey,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: Vec<u8>,
    pub write_version: u64,
    pub tx_index: Option<u32>,
}

impl From<ReplicaAccountInfo<'_>> for AccountUpdate {
    fn from(info: ReplicaAccountInfo<'_>) -> Self {
        Self {
            pubkey: Pubkey::new_from_array(info.pubkey[..32].try_into().unwrap_or_default()),
            lamports: info.lamports,
            owner: Pubkey::new_from_array(info.owner[..32].try_into().unwrap_or_default()),
            executable: info.executable,
            rent_epoch: info.rent_epoch,
            data: info.data.to_vec(),
            write_version: info.write_version,
            tx_index: None,
        }
    }
}

// impl From<ReplicaAccountInfoV2<'_>> for AccountUpdate {
//     fn from(info: ReplicaAccountInfoV2<'_>) -> Self {
//         Self {
//             pubkey: Pubkey::new_from_array(info.pubkey[..32].try_into().unwrap_or_default()),
//             lamports: info.lamports,
//             owner: Pubkey::new_from_array(info.owner[..32].try_into().unwrap_or_default()),
//             executable: info.executable,
//             rent_epoch: info.rent_epoch,
//             data: info.data.to_vec(),
//             write_version: info.write_version,
//             tx_index: info.tx_index,
//         }
//     }
// }

// #[derive(Debug, Clone, PartialEq, Eq)]
// #[repr(C)]
// /// Information about an account being updated
// /// (extended with reference to transaction doing this update)
// pub struct ReplicaAccountInfoV3<'a> {
//     /// The Pubkey for the account
//     pub pubkey: &'a [u8],

//     /// The lamports for the account
//     pub lamports: u64,

//     /// The Pubkey of the owner program account
//     pub owner: &'a [u8],

//     /// This account's data contains a loaded program (and is now read-only)
//     pub executable: bool,

//     /// The epoch at which this account will next owe rent
//     pub rent_epoch: u64,

//     /// The data held in this account.
//     pub data: &'a [u8],

//     /// A global monotonically increasing atomic number, which can be used
//     /// to tell the order of the account update. For example, when an
//     /// account is updated in the same slot multiple times, the update
//     /// with higher write_version should supersede the one with lower
//     /// write_version.
//     pub write_version: u64,

//     /// Reference to transaction causing this account modification
//     pub txn: Option<&'a SanitizedTransaction>,
// }
