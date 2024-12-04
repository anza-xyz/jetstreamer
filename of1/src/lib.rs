#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Epoch {
    pub num: u32,
    pub slot_start: u64,
    pub slot_end: u64,
}
