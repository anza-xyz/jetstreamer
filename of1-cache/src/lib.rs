use std::{cell::RefCell, fs::File};

use memmap2::Mmap;

#[derive(Debug)]
pub struct Epoch {
    pub num: u32,
    pub slot_start: u64,
    pub slot_end: u64,
    data: RefCell<CarData>,
}

#[derive(Debug)]
pub struct CarData {
    size: usize,
    file: File,
    mmap: Mmap,
}
