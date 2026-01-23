use agave_geyser_plugin_interface::geyser_plugin_interface::{
    ReplicaAccountInfo, ReplicaAccountInfoV2, ReplicaAccountInfoV3, ReplicaAccountInfoVersions,
};
use lencode::prelude::*;
use solana_pubkey::Pubkey;

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(C)]
pub struct AccountUpdate {
    /// The [`Pubkey`]` for the account
    pub pubkey: Pubkey,
    /// The lamports for the account
    pub lamports: u64,
    /// The [`Pubkey`] of the owner program account
    pub owner: Pubkey,
    /// This account's data contains a loaded program (and is now read-only)
    pub executable: bool,
    /// The epoch at which this account will next owe rent
    pub rent_epoch: u64,
    /// The data held in this account.
    pub data: Vec<u8>,
    /// A global monotonically increasing atomic number, which can be used to tell the order of
    /// the account update. For example, when an account is updated in the same slot multiple
    /// times, the update with higher write_version should supersede the one with lower
    /// write_version.
    pub write_version: u64,
}

impl AccountUpdate {}

impl From<&ReplicaAccountInfo<'_>> for AccountUpdate {
    #[inline]
    fn from(info: &ReplicaAccountInfo<'_>) -> Self {
        Self {
            pubkey: Pubkey::new_from_array(info.pubkey[..32].try_into().unwrap_or_default()),
            lamports: info.lamports,
            owner: Pubkey::new_from_array(info.owner[..32].try_into().unwrap_or_default()),
            executable: info.executable,
            rent_epoch: info.rent_epoch,
            data: info.data.to_vec(),
            write_version: info.write_version,
        }
    }
}

impl From<&ReplicaAccountInfoV2<'_>> for AccountUpdate {
    #[inline]
    fn from(info: &ReplicaAccountInfoV2<'_>) -> Self {
        Self {
            pubkey: Pubkey::new_from_array(info.pubkey[..32].try_into().unwrap_or_default()),
            lamports: info.lamports,
            owner: Pubkey::new_from_array(info.owner[..32].try_into().unwrap_or_default()),
            executable: info.executable,
            rent_epoch: info.rent_epoch,
            data: info.data.to_vec(),
            write_version: info.write_version,
        }
    }
}

impl From<&ReplicaAccountInfoV3<'_>> for AccountUpdate {
    #[inline]
    fn from(info: &ReplicaAccountInfoV3<'_>) -> Self {
        Self {
            pubkey: Pubkey::new_from_array(info.pubkey[..32].try_into().unwrap_or_default()),
            lamports: info.lamports,
            owner: Pubkey::new_from_array(info.owner[..32].try_into().unwrap_or_default()),
            executable: info.executable,
            rent_epoch: info.rent_epoch,
            data: info.data.to_vec(),
            write_version: info.write_version,
        }
    }
}

impl From<&ReplicaAccountInfoVersions<'_>> for AccountUpdate {
    #[inline]
    fn from(info: &ReplicaAccountInfoVersions<'_>) -> Self {
        match info {
            ReplicaAccountInfoVersions::V0_0_1(replica_account_info) => {
                AccountUpdate::from(*replica_account_info)
            }
            ReplicaAccountInfoVersions::V0_0_2(replica_account_info_v2) => {
                AccountUpdate::from(*replica_account_info_v2)
            }
            ReplicaAccountInfoVersions::V0_0_3(replica_account_info_v3) => {
                AccountUpdate::from(*replica_account_info_v3)
            }
        }
    }
}
