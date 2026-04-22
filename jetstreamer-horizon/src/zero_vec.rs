//! Fixed-capacity heap-free vector backed by an inline array.
//!
//! [`ZeroVec<N, T>`] stores up to `N` elements inline, tracking the current
//! length separately. It mirrors the API of `Vec<T>` — `push`, `pop`, `clear`,
//! `drain`, slicing via [`Deref`]/[`Index`], `Encode`/`Decode`, etc. — but
//! never allocates on the heap. Overflowing capacity panics.
//!
//! Useful for hot paths that want `Vec`-like ergonomics without the allocator
//! overhead: declare once, reuse via `clear()` + `push()` or
//! [`extend_from_slice`][ZeroVec::extend_from_slice], or build in place via
//! [`spare_capacity_mut`][ZeroVec::spare_capacity_mut].
//!
//! # Example
//!
//! ```
//! use jetstreamer_horizon::zero_vec::ZeroVec;
//!
//! let mut buf: ZeroVec<16, u8> = ZeroVec::new();
//! buf.extend_from_slice(b"hello ");
//! buf.extend_from_slice(b"world");
//! assert_eq!(&buf[..5], b"hello");
//! assert_eq!(&buf[6..], b"world");
//! ```

use core::cmp::Ordering;
use core::fmt;
use core::hash::{Hash, Hasher};
use core::mem::MaybeUninit;
use core::ops::{Bound, Deref, DerefMut, Index, IndexMut, RangeBounds};
use core::slice::{self, SliceIndex};
use lencode::prelude::*;

/// A fixed-capacity vector backed by an inline array.
///
/// `ZeroVec<N, T>` stores up to `N` elements of type `T` in a stack-allocated
/// array, tracking the current length separately. It provides most of the
/// functionality of `Vec<T>` without ever hitting the heap allocator.
///
/// In hot loops, declare a `ZeroVec` once and reuse it via [`clear`] +
/// [`push`] / [`extend_from_slice`] to avoid per-iteration allocation overhead.
///
/// # Panics
///
/// Operations that would exceed the capacity `N` will panic.
pub struct ZeroVec<const N: usize, T> {
    buf: [MaybeUninit<T>; N],
    len: usize,
}

impl<const N: usize, T> ZeroVec<N, T> {
    /// Creates a new empty `ZeroVec`.
    #[inline(always)]
    pub const fn new() -> Self {
        Self {
            // SAFETY: An array of MaybeUninit doesn't require initialization.
            buf: unsafe { MaybeUninit::<[MaybeUninit<T>; N]>::uninit().assume_init() },
            len: 0,
        }
    }

    /// Returns the maximum number of elements this `ZeroVec` can hold.
    #[inline(always)]
    pub const fn capacity(&self) -> usize {
        N
    }

    /// Returns the number of elements currently stored.
    #[inline(always)]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if the vector contains no elements.
    #[inline(always)]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the number of additional elements that can be inserted.
    #[inline(always)]
    pub const fn remaining_capacity(&self) -> usize {
        N - self.len
    }

    /// Returns a slice of the initialized elements.
    #[inline(always)]
    pub fn as_slice(&self) -> &[T] {
        // SAFETY: buf[..len] is fully initialized.
        unsafe { slice::from_raw_parts(self.buf.as_ptr() as *const T, self.len) }
    }

    /// Returns a mutable slice of the initialized elements.
    #[inline(always)]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        // SAFETY: buf[..len] is fully initialized.
        unsafe { slice::from_raw_parts_mut(self.buf.as_mut_ptr() as *mut T, self.len) }
    }

    /// Returns a pointer to the first element.
    #[inline(always)]
    pub fn as_ptr(&self) -> *const T {
        self.buf.as_ptr() as *const T
    }

    /// Returns a mutable pointer to the first element.
    #[inline(always)]
    pub fn as_mut_ptr(&mut self) -> *mut T {
        self.buf.as_mut_ptr() as *mut T
    }

    /// Appends an element to the back.
    ///
    /// # Panics
    ///
    /// Panics if the vector is at capacity.
    #[inline(always)]
    #[track_caller]
    pub fn push(&mut self, value: T) {
        assert!(self.len < N, "ZeroVec overflow: capacity is {N}");
        self.buf[self.len] = MaybeUninit::new(value);
        self.len += 1;
    }

    /// Attempts to append an element, returning `Err(value)` if full.
    #[inline(always)]
    pub fn try_push(&mut self, value: T) -> Result<(), T> {
        if self.len >= N {
            return Err(value);
        }
        self.buf[self.len] = MaybeUninit::new(value);
        self.len += 1;
        Ok(())
    }

    /// Removes and returns the last element, or `None` if empty.
    #[inline(always)]
    pub fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        self.len -= 1;
        // SAFETY: buf[len] was initialized and we've decremented len.
        Some(unsafe { self.buf[self.len].assume_init_read() })
    }

    /// Removes all elements, dropping them.
    #[inline(always)]
    pub fn clear(&mut self) {
        // Drop all initialized elements.
        let slice = self.as_mut_slice();
        // SAFETY: we drop the elements in-place and then set len to 0.
        unsafe {
            core::ptr::drop_in_place(slice);
        }
        self.len = 0;
    }

    /// Truncates the vector to `new_len`, dropping excess elements.
    /// Does nothing if `new_len >= self.len`.
    #[inline(always)]
    pub fn truncate(&mut self, new_len: usize) {
        if new_len >= self.len {
            return;
        }
        // SAFETY: drop the tail elements.
        unsafe {
            let tail =
                slice::from_raw_parts_mut(self.as_mut_ptr().add(new_len), self.len - new_len);
            core::ptr::drop_in_place(tail);
        }
        self.len = new_len;
    }

    /// Removes the element at `index` and returns it, replacing its slot with
    /// the last element (O(1)). Does not preserve ordering.
    ///
    /// # Panics
    ///
    /// Panics if `index >= self.len`.
    #[track_caller]
    pub fn swap_remove(&mut self, index: usize) -> T {
        assert!(
            index < self.len,
            "index {index} out of bounds (len={})",
            self.len
        );
        let last = self.len - 1;
        // SAFETY: index and last are both < len; swap them then truncate.
        unsafe {
            let value = self.buf[index].assume_init_read();
            if index != last {
                let last_val = self.buf[last].assume_init_read();
                self.buf[index] = MaybeUninit::new(last_val);
            }
            self.len = last;
            value
        }
    }

    /// Removes the element at `index`, shifting subsequent elements left.
    ///
    /// # Panics
    ///
    /// Panics if `index >= self.len`.
    #[track_caller]
    pub fn remove(&mut self, index: usize) -> T {
        assert!(
            index < self.len,
            "index {index} out of bounds (len={})",
            self.len
        );
        // SAFETY: index < len, so buf[index] is initialized.
        let value = unsafe { self.buf[index].assume_init_read() };
        // Shift elements left.
        if index + 1 < self.len {
            unsafe {
                core::ptr::copy(
                    self.buf.as_ptr().add(index + 1),
                    self.buf.as_mut_ptr().add(index),
                    self.len - index - 1,
                );
            }
        }
        self.len -= 1;
        value
    }

    /// Inserts an element at `index`, shifting subsequent elements right.
    ///
    /// # Panics
    ///
    /// Panics if `index > self.len` or if the vector is at capacity.
    #[track_caller]
    pub fn insert(&mut self, index: usize, value: T) {
        assert!(
            index <= self.len,
            "index {index} out of bounds (len={})",
            self.len
        );
        assert!(self.len < N, "ZeroVec overflow: capacity is {N}");
        if index < self.len {
            unsafe {
                core::ptr::copy(
                    self.buf.as_ptr().add(index),
                    self.buf.as_mut_ptr().add(index + 1),
                    self.len - index,
                );
            }
        }
        self.buf[index] = MaybeUninit::new(value);
        self.len += 1;
    }

    /// Retains only the elements for which the predicate returns `true`.
    pub fn retain<F: FnMut(&T) -> bool>(&mut self, mut f: F) {
        let mut write = 0;
        for read in 0..self.len {
            // SAFETY: buf[read] is initialized.
            let keep = unsafe { f(self.buf[read].assume_init_ref()) };
            if keep {
                if write != read {
                    // SAFETY: move buf[read] to buf[write].
                    unsafe {
                        let val = self.buf[read].assume_init_read();
                        self.buf[write] = MaybeUninit::new(val);
                    }
                }
                write += 1;
            } else {
                // Drop the rejected element.
                unsafe {
                    self.buf[read].assume_init_drop();
                }
            }
        }
        self.len = write;
    }

    /// Returns an iterator over references to the elements.
    #[inline(always)]
    pub fn iter(&self) -> slice::Iter<'_, T> {
        self.as_slice().iter()
    }

    /// Returns an iterator over mutable references to the elements.
    #[inline(always)]
    pub fn iter_mut(&mut self) -> slice::IterMut<'_, T> {
        self.as_mut_slice().iter_mut()
    }

    /// Returns a reference to the first element, or `None` if empty.
    #[inline(always)]
    pub fn first(&self) -> Option<&T> {
        self.as_slice().first()
    }

    /// Returns a reference to the last element, or `None` if empty.
    #[inline(always)]
    pub fn last(&self) -> Option<&T> {
        self.as_slice().last()
    }

    /// Returns a reference to the element at `index`, or `None` if out of bounds.
    #[inline(always)]
    pub fn get(&self, index: usize) -> Option<&T> {
        self.as_slice().get(index)
    }

    /// Returns a mutable reference to the element at `index`, or `None` if out of bounds.
    #[inline(always)]
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        self.as_mut_slice().get_mut(index)
    }

    /// Returns `true` if the vector contains the given value.
    #[inline]
    pub fn contains(&self, value: &T) -> bool
    where
        T: PartialEq,
    {
        self.as_slice().contains(value)
    }

    /// Converts into a `Vec<T>`, moving all elements.
    pub fn into_vec(mut self) -> Vec<T> {
        let mut vec = Vec::with_capacity(self.len);
        for i in 0..self.len {
            // SAFETY: buf[i] is initialized for i < len.
            vec.push(unsafe { self.buf[i].assume_init_read() });
        }
        // Prevent the Drop impl from dropping the moved-out elements.
        self.len = 0;
        vec
    }

    /// Sets the length without dropping or initializing elements.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `new_len` elements are initialized.
    #[inline(always)]
    pub unsafe fn set_len(&mut self, new_len: usize) {
        debug_assert!(new_len <= N);
        self.len = new_len;
    }

    /// Returns a mutable slice of the uninitialized tail, with length equal to
    /// [`remaining_capacity`][Self::remaining_capacity].
    ///
    /// Pair with [`set_len`][Self::set_len] to append initialized elements
    /// in place.
    #[inline(always)]
    pub fn spare_capacity_mut(&mut self) -> &mut [MaybeUninit<T>] {
        &mut self.buf[self.len..]
    }

    /// Resizes the vector to `new_len`, filling new slots by calling `f`.
    ///
    /// # Panics
    ///
    /// Panics if `new_len > N`.
    #[track_caller]
    pub fn resize_with<F: FnMut() -> T>(&mut self, new_len: usize, mut f: F) {
        assert!(
            new_len <= N,
            "ZeroVec::resize_with overflow: {new_len} > capacity {N}"
        );
        if new_len <= self.len {
            self.truncate(new_len);
        } else {
            for i in self.len..new_len {
                self.buf[i] = MaybeUninit::new(f());
            }
            self.len = new_len;
        }
    }

    /// Moves all elements out of `other` and appends them to `self`, leaving
    /// `other` empty.
    ///
    /// # Panics
    ///
    /// Panics if `self.len + other.len > N`.
    #[track_caller]
    pub fn append<const M: usize>(&mut self, other: &mut ZeroVec<M, T>) {
        assert!(
            self.len + other.len <= N,
            "ZeroVec::append overflow: {} + {} > capacity {N}",
            self.len,
            other.len
        );
        // SAFETY: both are contiguous and non-overlapping; bit-move each element.
        unsafe {
            core::ptr::copy_nonoverlapping(
                other.as_ptr(),
                self.as_mut_ptr().add(self.len),
                other.len,
            );
        }
        self.len += other.len;
        other.len = 0;
    }

    /// Removes consecutive duplicate elements.
    #[inline(always)]
    pub fn dedup(&mut self)
    where
        T: PartialEq,
    {
        self.dedup_by(|a, b| a == b);
    }

    /// Removes consecutive elements that compare equal under `same_bucket`.
    pub fn dedup_by<F: FnMut(&mut T, &mut T) -> bool>(&mut self, mut same_bucket: F) {
        if self.len < 2 {
            return;
        }
        let mut write = 1;
        for read in 1..self.len {
            // SAFETY: `write - 1` and `read` are distinct indices within
            // [0, len), so the two mutable references do not alias.
            let are_same = unsafe {
                let prev = &mut *self.buf[write - 1].as_mut_ptr();
                let curr = &mut *self.buf[read].as_mut_ptr();
                same_bucket(curr, prev)
            };
            if are_same {
                // Drop the duplicate at `read`.
                unsafe {
                    self.buf[read].assume_init_drop();
                }
            } else {
                if read != write {
                    // SAFETY: move buf[read] to buf[write].
                    unsafe {
                        let val = self.buf[read].assume_init_read();
                        self.buf[write] = MaybeUninit::new(val);
                    }
                }
                write += 1;
            }
        }
        self.len = write;
    }

    /// Removes consecutive elements whose mapped keys compare equal.
    #[inline(always)]
    pub fn dedup_by_key<K: PartialEq, F: FnMut(&mut T) -> K>(&mut self, mut key: F) {
        self.dedup_by(|a, b| key(a) == key(b));
    }

    /// Drains the specified range, yielding each removed element in order.
    ///
    /// If the returned [`Drain`] iterator is dropped before being fully
    /// consumed, the remaining elements are dropped and the tail is shifted.
    ///
    /// # Panics
    ///
    /// Panics if the range is out of bounds or inverted.
    #[track_caller]
    pub fn drain<R: RangeBounds<usize>>(&mut self, range: R) -> Drain<'_, N, T> {
        let start = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&n) => n + 1,
            Bound::Excluded(&n) => n,
            Bound::Unbounded => self.len,
        };
        assert!(start <= end, "drain: start ({start}) > end ({end})");
        assert!(end <= self.len, "drain: end ({end}) > len ({})", self.len);
        // Save the original length; the Drain's Drop will shift the tail down.
        let original_len = self.len;
        // SAFETY: temporarily lie about len so failures during Drain iteration
        // don't double-drop elements. Drain::drop restores len = start + tail_len.
        self.len = start;
        Drain {
            vec: self,
            start,
            end,
            head: start,
            tail: end,
            original_len,
        }
    }
}

// --- Copy-specific methods (only available when T: Copy) ---

impl<const N: usize, T: Copy> ZeroVec<N, T> {
    /// Copies elements from a slice into this vector, replacing all contents.
    /// This is the primary zero-alloc "overwrite" operation.
    ///
    /// # Panics
    ///
    /// Panics if `src.len() > N`.
    #[inline(always)]
    #[track_caller]
    pub fn set(&mut self, src: &[T]) {
        assert!(
            src.len() <= N,
            "ZeroVec::set overflow: {} > capacity {N}",
            src.len()
        );
        // For Copy types, we don't need to drop old elements.
        // SAFETY: T is Copy, src.len() <= N.
        unsafe {
            core::ptr::copy_nonoverlapping(src.as_ptr(), self.as_mut_ptr(), src.len());
        }
        self.len = src.len();
    }

    /// Appends elements from a slice.
    ///
    /// # Panics
    ///
    /// Panics if remaining capacity is insufficient.
    #[inline(always)]
    #[track_caller]
    pub fn extend_from_slice(&mut self, src: &[T]) {
        assert!(
            self.len + src.len() <= N,
            "ZeroVec::extend_from_slice overflow: {} + {} > capacity {N}",
            self.len,
            src.len()
        );
        unsafe {
            core::ptr::copy_nonoverlapping(
                src.as_ptr(),
                self.as_mut_ptr().add(self.len),
                src.len(),
            );
        }
        self.len += src.len();
    }

    /// Resizes the vector to `new_len`, filling new slots with `value`.
    #[track_caller]
    pub fn resize(&mut self, new_len: usize, value: T) {
        assert!(
            new_len <= N,
            "ZeroVec::resize overflow: {new_len} > capacity {N}"
        );
        if new_len <= self.len {
            self.truncate(new_len);
        } else {
            for i in self.len..new_len {
                self.buf[i] = MaybeUninit::new(value);
            }
            self.len = new_len;
        }
    }

    /// Copies elements from `range` within the vector onto the end.
    ///
    /// # Panics
    ///
    /// Panics if the range is out of bounds or the copied elements wouldn't
    /// fit in remaining capacity.
    #[track_caller]
    pub fn extend_from_within<R: RangeBounds<usize>>(&mut self, range: R) {
        let start = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&n) => n + 1,
            Bound::Excluded(&n) => n,
            Bound::Unbounded => self.len,
        };
        assert!(start <= end, "extend_from_within: start > end");
        assert!(
            end <= self.len,
            "extend_from_within: end ({end}) > len ({})",
            self.len
        );
        let count = end - start;
        assert!(
            self.len + count <= N,
            "ZeroVec::extend_from_within overflow: {} + {count} > capacity {N}",
            self.len
        );
        // SAFETY: T: Copy means we can bitwise-copy, even if source and dest
        // overlap (copy_nonoverlapping is fine here since dest starts at
        // self.len >= end, but use ptr::copy to be safe for edge cases).
        unsafe {
            core::ptr::copy(
                self.as_ptr().add(start),
                self.as_mut_ptr().add(self.len),
                count,
            );
        }
        self.len += count;
    }
}

// --- Drop ---

impl<const N: usize, T> Drop for ZeroVec<N, T> {
    fn drop(&mut self) {
        // Drop all initialized elements.
        unsafe {
            core::ptr::drop_in_place(self.as_mut_slice());
        }
    }
}

// --- Clone ---

impl<const N: usize, T: Clone> Clone for ZeroVec<N, T> {
    fn clone(&self) -> Self {
        let mut new = Self::new();
        for item in self.as_slice() {
            new.buf[new.len] = MaybeUninit::new(item.clone());
            new.len += 1;
        }
        new
    }
}

// --- Default ---

impl<const N: usize, T> Default for ZeroVec<N, T> {
    #[inline(always)]
    fn default() -> Self {
        Self::new()
    }
}

// --- Deref / DerefMut ---

impl<const N: usize, T> Deref for ZeroVec<N, T> {
    type Target = [T];

    #[inline(always)]
    fn deref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<const N: usize, T> DerefMut for ZeroVec<N, T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut [T] {
        self.as_mut_slice()
    }
}

// --- Index / IndexMut (generic, matches `Vec<T>`) ---

impl<const N: usize, T, I: SliceIndex<[T]>> Index<I> for ZeroVec<N, T> {
    type Output = I::Output;

    #[inline(always)]
    fn index(&self, index: I) -> &I::Output {
        Index::index(self.as_slice(), index)
    }
}

impl<const N: usize, T, I: SliceIndex<[T]>> IndexMut<I> for ZeroVec<N, T> {
    #[inline(always)]
    fn index_mut(&mut self, index: I) -> &mut I::Output {
        IndexMut::index_mut(self.as_mut_slice(), index)
    }
}

// --- Comparison traits ---

impl<const N: usize, T: PartialEq> PartialEq for ZeroVec<N, T> {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<const N: usize, T: Eq> Eq for ZeroVec<N, T> {}

impl<const N: usize, T: PartialOrd> PartialOrd for ZeroVec<N, T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_slice().partial_cmp(other.as_slice())
    }
}

impl<const N: usize, T: Ord> Ord for ZeroVec<N, T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_slice().cmp(other.as_slice())
    }
}

impl<const N: usize, T: Hash> Hash for ZeroVec<N, T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state);
    }
}

// --- Debug ---

impl<const N: usize, T: fmt::Debug> fmt::Debug for ZeroVec<N, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.as_slice()).finish()
    }
}

// --- From conversions ---

impl<const N: usize, T: Copy> From<&[T]> for ZeroVec<N, T> {
    fn from(src: &[T]) -> Self {
        let mut v = Self::new();
        v.set(src);
        v
    }
}

impl<const N: usize, T> From<Vec<T>> for ZeroVec<N, T> {
    fn from(vec: Vec<T>) -> Self {
        assert!(
            vec.len() <= N,
            "Vec too large for ZeroVec: {} > capacity {N}",
            vec.len()
        );
        let mut zv = Self::new();
        for item in vec {
            zv.buf[zv.len] = MaybeUninit::new(item);
            zv.len += 1;
        }
        zv
    }
}

impl<const N: usize, T> From<ZeroVec<N, T>> for Vec<T> {
    fn from(zv: ZeroVec<N, T>) -> Self {
        zv.into_vec()
    }
}

// --- PartialEq with Vec and slice for convenience ---

impl<const N: usize, T: PartialEq> PartialEq<Vec<T>> for ZeroVec<N, T> {
    fn eq(&self, other: &Vec<T>) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<const N: usize, T: PartialEq> PartialEq<&[T]> for ZeroVec<N, T> {
    fn eq(&self, other: &&[T]) -> bool {
        self.as_slice() == *other
    }
}

// --- IntoIterator ---

impl<'a, const N: usize, T> IntoIterator for &'a ZeroVec<N, T> {
    type Item = &'a T;
    type IntoIter = slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.as_slice().iter()
    }
}

impl<'a, const N: usize, T> IntoIterator for &'a mut ZeroVec<N, T> {
    type Item = &'a mut T;
    type IntoIter = slice::IterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.as_mut_slice().iter_mut()
    }
}

/// Owned iterator for `ZeroVec`.
pub struct IntoIter<const N: usize, T> {
    vec: ZeroVec<N, T>,
    pos: usize,
}

impl<const N: usize, T> Iterator for IntoIter<N, T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        if self.pos >= self.vec.len {
            return None;
        }
        // SAFETY: pos < len, so buf[pos] is initialized.
        let val = unsafe { self.vec.buf[self.pos].assume_init_read() };
        self.pos += 1;
        val.into()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.vec.len - self.pos;
        (remaining, Some(remaining))
    }
}

impl<const N: usize, T> DoubleEndedIterator for IntoIter<N, T> {
    fn next_back(&mut self) -> Option<T> {
        if self.pos >= self.vec.len {
            return None;
        }
        let last = self.vec.len - 1;
        // SAFETY: last is in bounds (>= pos).
        let val = unsafe { self.vec.buf[last].assume_init_read() };
        self.vec.len = last;
        Some(val)
    }
}

impl<const N: usize, T> ExactSizeIterator for IntoIter<N, T> {}

impl<const N: usize, T> core::iter::FusedIterator for IntoIter<N, T> {}

impl<const N: usize, T> Drop for IntoIter<N, T> {
    fn drop(&mut self) {
        // Drop any elements that weren't consumed by the iterator.
        for i in self.pos..self.vec.len {
            unsafe {
                self.vec.buf[i].assume_init_drop();
            }
        }
        // Prevent ZeroVec's Drop from double-dropping.
        self.vec.len = 0;
    }
}

/// Iterator yielding elements being drained from a `ZeroVec`.
///
/// Returned by [`ZeroVec::drain`]. When dropped, any unconsumed elements in
/// the drained range are dropped and the tail (elements after the drained
/// range) is shifted down to close the gap.
pub struct Drain<'a, const N: usize, T: 'a> {
    vec: &'a mut ZeroVec<N, T>,
    // Fixed bounds of the drained range.
    start: usize,
    end: usize,
    // Iteration cursors: `head` advances from `start` via `next`,
    // `tail` retreats from `end` via `next_back`. Unconsumed elements
    // remain in `[head, tail)`.
    head: usize,
    tail: usize,
    // Length of the vec before drain() was called.
    original_len: usize,
}

impl<'a, const N: usize, T> Iterator for Drain<'a, N, T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        if self.head >= self.tail {
            return None;
        }
        // SAFETY: head < tail <= end <= original_len, so buf[head] was init.
        let val = unsafe { self.vec.buf[self.head].assume_init_read() };
        self.head += 1;
        Some(val)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.tail - self.head;
        (remaining, Some(remaining))
    }
}

impl<'a, const N: usize, T> DoubleEndedIterator for Drain<'a, N, T> {
    fn next_back(&mut self) -> Option<T> {
        if self.head >= self.tail {
            return None;
        }
        self.tail -= 1;
        // SAFETY: tail was > head, so buf[tail] was initialized.
        let val = unsafe { self.vec.buf[self.tail].assume_init_read() };
        Some(val)
    }
}

impl<'a, const N: usize, T> ExactSizeIterator for Drain<'a, N, T> {}

impl<'a, const N: usize, T> core::iter::FusedIterator for Drain<'a, N, T> {}

impl<'a, const N: usize, T> Drop for Drain<'a, N, T> {
    fn drop(&mut self) {
        // Drop any remaining (unconsumed) elements in [head, tail).
        for i in self.head..self.tail {
            unsafe {
                self.vec.buf[i].assume_init_drop();
            }
        }
        // Shift the tail (elements at [end..original_len]) down to close the gap.
        let tail_len = self.original_len - self.end;
        if tail_len > 0 {
            // SAFETY: buf[end..original_len] is initialized. The source range
            // and destination range may overlap; ptr::copy handles that.
            unsafe {
                core::ptr::copy(
                    self.vec.as_ptr().add(self.end),
                    self.vec.as_mut_ptr().add(self.start),
                    tail_len,
                );
            }
        }
        self.vec.len = self.start + tail_len;
    }
}

impl<const N: usize, T> IntoIterator for ZeroVec<N, T> {
    type Item = T;
    type IntoIter = IntoIter<N, T>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter { vec: self, pos: 0 }
    }
}

// --- Extend ---

impl<const N: usize, T> Extend<T> for ZeroVec<N, T> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        for item in iter {
            self.push(item);
        }
    }
}

// --- FromIterator ---

impl<const N: usize, T> FromIterator<T> for ZeroVec<N, T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut zv = Self::new();
        zv.extend(iter);
        zv
    }
}

// --- AsRef / AsMut ---

impl<const N: usize, T> AsRef<[T]> for ZeroVec<N, T> {
    fn as_ref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<const N: usize, T> AsMut<[T]> for ZeroVec<N, T> {
    fn as_mut(&mut self) -> &mut [T] {
        self.as_mut_slice()
    }
}

// --- lencode Encode / Decode ---

impl<const N: usize, T: Encode + 'static> Encode for ZeroVec<N, T> {
    #[inline(always)]
    fn encode_ext(
        &self,
        writer: &mut impl Write,
        mut ctx: Option<&mut lencode::context::EncoderContext>,
    ) -> lencode::Result<usize> {
        // For u8: always write uncompressed (zero-alloc).
        // Wire format: varint(raw_len << 1 | 0) + raw_bytes
        if core::any::TypeId::of::<T>() == core::any::TypeId::of::<u8>() {
            let bytes: &[u8] =
                unsafe { slice::from_raw_parts(self.as_ptr() as *const u8, self.len) };
            let mut total = 0;
            total += Self::encode_len(bytes.len() << 1, writer)?;
            total += writer.write(bytes)?;
            return Ok(total);
        }

        // Non-u8: varint(element_count) + elements
        let mut total = 0;
        total += Self::encode_len(self.len, writer)?;
        for item in self.as_slice() {
            total += item.encode_ext(writer, ctx.as_deref_mut())?;
        }
        Ok(total)
    }
}

impl<const N: usize, T: Decode + 'static> Decode for ZeroVec<N, T> {
    #[inline(always)]
    fn decode_ext(
        reader: &mut impl Read,
        mut ctx: Option<&mut lencode::context::DecoderContext>,
    ) -> lencode::Result<Self> {
        // For u8: read uncompressed directly into inline buffer (zero-alloc).
        if core::any::TypeId::of::<T>() == core::any::TypeId::of::<u8>() {
            let flagged = Self::decode_len(reader)?;
            assert!(flagged & 1 == 0, "ZeroVec does not support compressed data");
            let payload_len = flagged >> 1;
            assert!(
                payload_len <= N,
                "decoded data too large for ZeroVec: {payload_len} > capacity {N}"
            );
            let mut zv = Self::new();
            if payload_len > 0 {
                let buf =
                    unsafe { slice::from_raw_parts_mut(zv.as_mut_ptr() as *mut u8, payload_len) };
                reader.read(buf)?;
            }
            unsafe {
                zv.set_len(payload_len);
            }
            return Ok(zv);
        }

        // Non-u8: varint(element_count) + elements
        let len = Self::decode_len(reader)?;
        assert!(
            len <= N,
            "decoded length too large for ZeroVec: {len} > capacity {N}"
        );
        let mut zv = Self::new();
        for _ in 0..len {
            let item = T::decode_ext(reader, ctx.as_deref_mut())?;
            zv.buf[zv.len] = MaybeUninit::new(item);
            zv.len += 1;
        }
        Ok(zv)
    }

    fn decode_len(reader: &mut impl Read) -> lencode::Result<usize> {
        Vec::<T>::decode_len(reader)
    }
}

// --- Write trait for ZeroVec<N, u8> so it can be used as a write target ---

impl<const N: usize> std::io::Write for ZeroVec<N, u8> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let available = N - self.len;
        let to_write = buf.len().min(available);
        if to_write > 0 {
            unsafe {
                core::ptr::copy_nonoverlapping(
                    buf.as_ptr(),
                    self.as_mut_ptr().add(self.len),
                    to_write,
                );
            }
            self.len += to_write;
        }
        Ok(to_write)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_is_empty() {
        let v: ZeroVec<16, u8> = ZeroVec::new();
        assert!(v.is_empty());
        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 16);
        assert_eq!(v.remaining_capacity(), 16);
    }

    #[test]
    fn test_push_pop() {
        let mut v: ZeroVec<4, i32> = ZeroVec::new();
        v.push(10);
        v.push(20);
        v.push(30);
        assert_eq!(v.len(), 3);
        assert_eq!(v.as_slice(), &[10, 20, 30]);
        assert_eq!(v.pop(), Some(30));
        assert_eq!(v.pop(), Some(20));
        assert_eq!(v.pop(), Some(10));
        assert_eq!(v.pop(), None);
        assert!(v.is_empty());
    }

    #[test]
    #[should_panic(expected = "ZeroVec overflow")]
    fn test_push_overflow_panics() {
        let mut v: ZeroVec<2, u8> = ZeroVec::new();
        v.push(1);
        v.push(2);
        v.push(3); // panics
    }

    #[test]
    fn test_try_push() {
        let mut v: ZeroVec<2, u8> = ZeroVec::new();
        assert!(v.try_push(1).is_ok());
        assert!(v.try_push(2).is_ok());
        assert_eq!(v.try_push(3), Err(3));
        assert_eq!(v.len(), 2);
    }

    #[test]
    fn test_clear() {
        let mut v: ZeroVec<8, u32> = ZeroVec::new();
        v.push(1);
        v.push(2);
        v.push(3);
        v.clear();
        assert!(v.is_empty());
    }

    #[test]
    fn test_set_and_extend_from_slice() {
        let mut v: ZeroVec<16, u8> = ZeroVec::new();
        v.set(b"hello");
        assert_eq!(v.as_slice(), b"hello");
        assert_eq!(v.len(), 5);

        v.extend_from_slice(b" world");
        assert_eq!(v.as_slice(), b"hello world");

        v.set(b"replaced");
        assert_eq!(v.as_slice(), b"replaced");
    }

    #[test]
    fn test_truncate() {
        let mut v: ZeroVec<8, u32> = ZeroVec::new();
        v.push(1);
        v.push(2);
        v.push(3);
        v.push(4);
        v.truncate(2);
        assert_eq!(v.as_slice(), &[1, 2]);
        v.truncate(10); // no-op
        assert_eq!(v.len(), 2);
    }

    #[test]
    fn test_insert_remove() {
        let mut v: ZeroVec<8, i32> = ZeroVec::new();
        v.push(1);
        v.push(3);
        v.push(4);
        v.insert(1, 2);
        assert_eq!(v.as_slice(), &[1, 2, 3, 4]);
        let removed = v.remove(2);
        assert_eq!(removed, 3);
        assert_eq!(v.as_slice(), &[1, 2, 4]);
    }

    #[test]
    fn test_retain() {
        let mut v: ZeroVec<8, i32> = ZeroVec::new();
        for i in 0..6 {
            v.push(i);
        }
        v.retain(|&x| x % 2 == 0);
        assert_eq!(v.as_slice(), &[0, 2, 4]);
    }

    #[test]
    fn test_resize() {
        let mut v: ZeroVec<16, u8> = ZeroVec::new();
        v.resize(5, 0xFF);
        assert_eq!(v.as_slice(), &[0xFF; 5]);
        v.resize(3, 0);
        assert_eq!(v.as_slice(), &[0xFF, 0xFF, 0xFF]);
        v.resize(6, 0xAA);
        assert_eq!(v.as_slice(), &[0xFF, 0xFF, 0xFF, 0xAA, 0xAA, 0xAA]);
    }

    #[test]
    fn test_clone() {
        let mut v: ZeroVec<8, i32> = ZeroVec::new();
        v.push(10);
        v.push(20);
        let v2 = v.clone();
        assert_eq!(v, v2);
    }

    #[test]
    fn test_eq_ord_hash() {
        let mut a: ZeroVec<8, u8> = ZeroVec::new();
        let mut b: ZeroVec<8, u8> = ZeroVec::new();
        a.set(b"abc");
        b.set(b"abc");
        assert_eq!(a, b);

        b.set(b"abd");
        assert!(a < b);

        use std::collections::hash_map::DefaultHasher;
        let hash_of = |v: &ZeroVec<8, u8>| {
            let mut h = DefaultHasher::new();
            v.hash(&mut h);
            h.finish()
        };
        a.set(b"test");
        b.set(b"test");
        assert_eq!(hash_of(&a), hash_of(&b));
    }

    #[test]
    fn test_debug() {
        let mut v: ZeroVec<4, i32> = ZeroVec::new();
        v.push(1);
        v.push(2);
        assert_eq!(format!("{:?}", v), "[1, 2]");
    }

    #[test]
    fn test_from_slice() {
        let v: ZeroVec<8, u8> = ZeroVec::from(&b"hello"[..]);
        assert_eq!(v.as_slice(), b"hello");
    }

    #[test]
    fn test_from_vec() {
        let v: ZeroVec<8, i32> = ZeroVec::from(vec![1, 2, 3]);
        assert_eq!(v.as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn test_into_vec() {
        let mut v: ZeroVec<8, i32> = ZeroVec::new();
        v.push(10);
        v.push(20);
        let vec: Vec<i32> = v.into_vec();
        assert_eq!(vec, vec![10, 20]);
    }

    #[test]
    fn test_partial_eq_with_vec() {
        let mut v: ZeroVec<8, i32> = ZeroVec::new();
        v.push(1);
        v.push(2);
        assert_eq!(v, vec![1, 2]);
    }

    #[test]
    fn test_partial_eq_with_slice() {
        let mut v: ZeroVec<8, u8> = ZeroVec::new();
        v.set(b"abc");
        assert_eq!(v, &b"abc"[..]);
    }

    #[test]
    fn test_deref_slice_methods() {
        let mut v: ZeroVec<16, u8> = ZeroVec::new();
        v.set(b"hello world");
        assert_eq!(v.len(), 11);
        assert!(v.starts_with(b"hello"));
        assert!(v.ends_with(b"world"));
        assert_eq!(&v[0..5], b"hello");
    }

    #[test]
    fn test_index() {
        let mut v: ZeroVec<8, i32> = ZeroVec::new();
        v.push(10);
        v.push(20);
        assert_eq!(v[0], 10);
        assert_eq!(v[1], 20);
    }

    #[test]
    fn test_index_mut() {
        let mut v: ZeroVec<8, i32> = ZeroVec::new();
        v.push(10);
        v[0] = 42;
        assert_eq!(v[0], 42);
    }

    #[test]
    fn test_into_iter() {
        let mut v: ZeroVec<8, i32> = ZeroVec::new();
        v.push(1);
        v.push(2);
        v.push(3);
        let collected: Vec<i32> = v.into_iter().collect();
        assert_eq!(collected, vec![1, 2, 3]);
    }

    #[test]
    fn test_into_iter_partial_consume() {
        let mut v: ZeroVec<8, String> = ZeroVec::new();
        v.push("a".to_string());
        v.push("b".to_string());
        v.push("c".to_string());
        let mut it = v.into_iter();
        assert_eq!(it.next(), Some("a".to_string()));
        // Drop the iterator without consuming all — remaining elements should be dropped.
        drop(it);
    }

    #[test]
    fn test_extend() {
        let mut v: ZeroVec<8, i32> = ZeroVec::new();
        v.extend([1, 2, 3]);
        v.extend([4, 5]);
        assert_eq!(v.as_slice(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_from_iterator() {
        let v: ZeroVec<8, i32> = (0..5).collect();
        assert_eq!(v.as_slice(), &[0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_first_last_get() {
        let mut v: ZeroVec<8, i32> = ZeroVec::new();
        assert_eq!(v.first(), None);
        assert_eq!(v.last(), None);
        v.push(10);
        v.push(20);
        v.push(30);
        assert_eq!(v.first(), Some(&10));
        assert_eq!(v.last(), Some(&30));
        assert_eq!(v.get(1), Some(&20));
        assert_eq!(v.get(5), None);
    }

    #[test]
    fn test_contains() {
        let mut v: ZeroVec<8, i32> = ZeroVec::new();
        v.push(1);
        v.push(2);
        v.push(3);
        assert!(v.contains(&2));
        assert!(!v.contains(&4));
    }

    #[test]
    fn test_io_write_for_u8() {
        let mut v: ZeroVec<16, u8> = ZeroVec::new();
        let n = std::io::Write::write(&mut v, b"hello").unwrap();
        assert_eq!(n, 5);
        let n = std::io::Write::write(&mut v, b" world!").unwrap();
        assert_eq!(n, 7);
        assert_eq!(v.as_slice(), b"hello world!");
    }

    #[test]
    fn test_io_write_truncates_at_capacity() {
        let mut v: ZeroVec<4, u8> = ZeroVec::new();
        let n = std::io::Write::write(&mut v, b"hello").unwrap();
        assert_eq!(n, 4);
        assert_eq!(v.as_slice(), b"hell");
    }

    #[test]
    fn test_drop_nontrivial_types() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

        #[derive(Clone)]
        #[allow(dead_code)]
        struct Tracked(u32);
        impl Drop for Tracked {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }

        DROP_COUNT.store(0, Ordering::SeqCst);
        {
            let mut v: ZeroVec<8, Tracked> = ZeroVec::new();
            v.push(Tracked(1));
            v.push(Tracked(2));
            v.push(Tracked(3));
        }
        assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn test_encode_decode_u8() {
        let mut v: ZeroVec<64, u8> = ZeroVec::new();
        v.set(b"hello world, this is a test of encoding");

        let mut buf = vec![0u8; 4096];
        let mut cursor = lencode::io::Cursor::new(&mut buf[..]);
        let written = v.encode_ext(&mut cursor, None).unwrap();
        assert!(written > 0);

        let mut read_cursor = lencode::io::Cursor::new(&buf[..written]);
        let decoded: ZeroVec<64, u8> = ZeroVec::decode_ext(&mut read_cursor, None).unwrap();
        assert_eq!(v, decoded);
    }

    #[test]
    fn test_encode_decode_u32() {
        let mut v: ZeroVec<16, u32> = ZeroVec::new();
        v.push(100);
        v.push(200);
        v.push(300);

        let mut buf = vec![0u8; 4096];
        let mut cursor = lencode::io::Cursor::new(&mut buf[..]);
        let written = v.encode_ext(&mut cursor, None).unwrap();
        assert!(written > 0);

        let mut read_cursor = lencode::io::Cursor::new(&buf[..written]);
        let decoded: ZeroVec<16, u32> = ZeroVec::decode_ext(&mut read_cursor, None).unwrap();
        assert_eq!(v, decoded);
    }

    #[test]
    fn test_encode_decode_empty() {
        let v: ZeroVec<8, u8> = ZeroVec::new();

        let mut buf = vec![0u8; 4096];
        let mut cursor = lencode::io::Cursor::new(&mut buf[..]);
        let written = v.encode_ext(&mut cursor, None).unwrap();

        let mut read_cursor = lencode::io::Cursor::new(&buf[..written]);
        let decoded: ZeroVec<8, u8> = ZeroVec::decode_ext(&mut read_cursor, None).unwrap();
        assert_eq!(decoded.len(), 0);
    }

    #[test]
    fn test_wire_compatible_with_vec_u32() {
        // Encode as Vec<u32>, decode as ZeroVec<N, u32>.
        let original: Vec<u32> = vec![1, 2, 3, 4, 5];

        let mut buf = vec![0u8; 4096];
        let mut cursor = lencode::io::Cursor::new(&mut buf[..]);
        let written = original.encode_ext(&mut cursor, None).unwrap();

        let mut read_cursor = lencode::io::Cursor::new(&buf[..written]);
        let decoded: ZeroVec<16, u32> = ZeroVec::decode_ext(&mut read_cursor, None).unwrap();
        assert_eq!(decoded.as_slice(), original.as_slice());
    }

    #[test]
    fn test_zerovvec_encodes_same_as_vec_u8() {
        // Encode as ZeroVec, decode as Vec — must round-trip.
        let mut zv: ZeroVec<64, u8> = ZeroVec::new();
        zv.set(b"round trip test");

        let mut buf = vec![0u8; 4096];
        let mut cursor = lencode::io::Cursor::new(&mut buf[..]);
        let written = zv.encode_ext(&mut cursor, None).unwrap();

        let mut read_cursor = lencode::io::Cursor::new(&buf[..written]);
        let decoded: Vec<u8> = Vec::decode_ext(&mut read_cursor, None).unwrap();
        assert_eq!(decoded.as_slice(), zv.as_slice());
    }

    // --- New methods ---

    #[test]
    fn test_swap_remove() {
        let mut v: ZeroVec<8, i32> = ZeroVec::new();
        v.extend_from_slice(&[10, 20, 30, 40]);
        assert_eq!(v.swap_remove(1), 20);
        // Element at index 1 is now the former last (40); order NOT preserved.
        assert_eq!(v.as_slice(), &[10, 40, 30]);
        // swap_remove on the last element is a plain pop.
        assert_eq!(v.swap_remove(2), 30);
        assert_eq!(v.as_slice(), &[10, 40]);
    }

    #[test]
    #[should_panic(expected = "out of bounds")]
    fn test_swap_remove_out_of_bounds() {
        let mut v: ZeroVec<4, i32> = ZeroVec::new();
        v.push(1);
        v.swap_remove(5);
    }

    #[test]
    fn test_spare_capacity_mut() {
        let mut v: ZeroVec<8, u8> = ZeroVec::new();
        v.extend_from_slice(b"ab");
        let spare = v.spare_capacity_mut();
        assert_eq!(spare.len(), 6);
        spare[0] = MaybeUninit::new(b'c');
        spare[1] = MaybeUninit::new(b'd');
        unsafe { v.set_len(4) };
        assert_eq!(v.as_slice(), b"abcd");
    }

    #[test]
    fn test_resize_with() {
        let mut v: ZeroVec<8, String> = ZeroVec::new();
        let mut count = 0;
        v.resize_with(3, || {
            count += 1;
            format!("item{count}")
        });
        assert_eq!(v.len(), 3);
        assert_eq!(v[0], "item1");
        assert_eq!(v[1], "item2");
        assert_eq!(v[2], "item3");
        // Shrinking drops extras.
        v.resize_with(1, || "unused".to_string());
        assert_eq!(v.as_slice().len(), 1);
        assert_eq!(v[0], "item1");
    }

    #[test]
    fn test_append() {
        let mut a: ZeroVec<8, i32> = ZeroVec::new();
        let mut b: ZeroVec<4, i32> = ZeroVec::new();
        a.extend_from_slice(&[1, 2, 3]);
        b.extend_from_slice(&[4, 5]);
        a.append(&mut b);
        assert_eq!(a.as_slice(), &[1, 2, 3, 4, 5]);
        assert!(b.is_empty());
    }

    #[test]
    #[should_panic(expected = "ZeroVec::append overflow")]
    fn test_append_overflow() {
        let mut a: ZeroVec<4, i32> = ZeroVec::new();
        let mut b: ZeroVec<4, i32> = ZeroVec::new();
        a.extend_from_slice(&[1, 2, 3]);
        b.extend_from_slice(&[4, 5, 6]);
        a.append(&mut b); // 3 + 3 > 4
    }

    #[test]
    fn test_dedup() {
        let mut v: ZeroVec<16, i32> = ZeroVec::new();
        v.extend_from_slice(&[1, 1, 2, 3, 3, 3, 4, 1, 1]);
        v.dedup();
        assert_eq!(v.as_slice(), &[1, 2, 3, 4, 1]);
    }

    #[test]
    fn test_dedup_by_key() {
        let mut v: ZeroVec<8, i32> = ZeroVec::new();
        v.extend_from_slice(&[10, 11, 12, 20, 21, 30]);
        v.dedup_by_key(|x| *x / 10);
        assert_eq!(v.as_slice(), &[10, 20, 30]);
    }

    #[test]
    fn test_extend_from_within() {
        let mut v: ZeroVec<16, u8> = ZeroVec::new();
        v.extend_from_slice(b"abc");
        v.extend_from_within(0..2);
        assert_eq!(v.as_slice(), b"abcab");
        v.extend_from_within(..);
        assert_eq!(v.as_slice(), b"abcababcab");
    }

    #[test]
    fn test_drain_full() {
        let mut v: ZeroVec<8, i32> = ZeroVec::new();
        v.extend_from_slice(&[1, 2, 3, 4, 5]);
        let drained: Vec<i32> = v.drain(..).collect();
        assert_eq!(drained, vec![1, 2, 3, 4, 5]);
        assert!(v.is_empty());
    }

    #[test]
    fn test_drain_partial() {
        let mut v: ZeroVec<8, i32> = ZeroVec::new();
        v.extend_from_slice(&[1, 2, 3, 4, 5]);
        let drained: Vec<i32> = v.drain(1..4).collect();
        assert_eq!(drained, vec![2, 3, 4]);
        assert_eq!(v.as_slice(), &[1, 5]);
    }

    #[test]
    fn test_drain_drop_mid_iter() {
        // Ensure unconsumed drained elements are dropped and the tail is shifted.
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
        static DROPS: AtomicUsize = AtomicUsize::new(0);

        struct D(u32);
        impl Drop for D {
            fn drop(&mut self) {
                DROPS.fetch_add(1, AtomicOrdering::SeqCst);
            }
        }

        DROPS.store(0, AtomicOrdering::SeqCst);
        let mut v: ZeroVec<8, D> = ZeroVec::new();
        v.push(D(1));
        v.push(D(2));
        v.push(D(3));
        v.push(D(4));
        v.push(D(5));
        {
            let mut it = v.drain(1..4);
            let _first = it.next().unwrap(); // consume D(2), will drop when _first is dropped
            // Drop it; D(3) and D(4) should be dropped by Drain::drop.
        }
        // After drain: v = [D(1), D(5)]. 3 drops so far (D(2), D(3), D(4)).
        assert_eq!(DROPS.load(AtomicOrdering::SeqCst), 3);
        assert_eq!(v.len(), 2);
    }

    #[test]
    fn test_drain_rev() {
        let mut v: ZeroVec<8, i32> = ZeroVec::new();
        v.extend_from_slice(&[1, 2, 3, 4, 5]);
        let drained: Vec<i32> = v.drain(1..4).rev().collect();
        assert_eq!(drained, vec![4, 3, 2]);
        assert_eq!(v.as_slice(), &[1, 5]);
    }

    #[test]
    fn test_into_iter_rev() {
        let mut v: ZeroVec<4, i32> = ZeroVec::new();
        v.extend_from_slice(&[1, 2, 3]);
        let collected: Vec<i32> = v.into_iter().rev().collect();
        assert_eq!(collected, vec![3, 2, 1]);
    }

    #[test]
    fn test_index_range() {
        // Range, RangeFrom, RangeTo, RangeFull, RangeInclusive all via SliceIndex.
        let mut v: ZeroVec<16, u8> = ZeroVec::new();
        v.extend_from_slice(b"abcdef");
        assert_eq!(&v[1..4], b"bcd");
        assert_eq!(&v[..3], b"abc");
        assert_eq!(&v[3..], b"def");
        assert_eq!(&v[..], b"abcdef");
        assert_eq!(&v[1..=3], b"bcd");
    }

    #[test]
    fn test_index_range_mut() {
        let mut v: ZeroVec<8, u8> = ZeroVec::new();
        v.extend_from_slice(b"hello");
        v[1..4].copy_from_slice(b"ELL");
        assert_eq!(v.as_slice(), b"hELLo");
    }
}
