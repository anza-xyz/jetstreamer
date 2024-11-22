use std::ops::Range;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Epoch {
    pub num: u32,
    pub slots: Range<u64>,
}
