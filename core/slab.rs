//! Fixed-capacity object slab.
//!
//! A slab owns a homogeneous array of objects and an intrusive free list over
//! the unused entries. Allocation removes one entry from the free list.
//! Release returns the entry to the free list. The backing array is allocated
//! once during construction and is never resized.

use std::alloc::Allocator;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

/// Fixed-capacity object slab backed by an intrusive free list.
pub struct Slab<A: Allocator, T> {
    pub(crate) entries: Box<[T], A>,
    free_head: Option<usize>,
}

/// Mutable handle to a slab entry.
///
/// If the guard is dropped while the entry is logically free, the slot is
/// returned to the free list automatically.
pub struct EntryMut<'a, T: SlabEntry> {
    entries: *mut T,
    free_head: *mut Option<usize>,
    id: Option<usize>,
    _marker: PhantomData<&'a mut T>,
}

/// Mutable iterator over allocated slab entries.
pub struct EntriesMut<'a, T: SlabEntry> {
    entries: *mut T,
    free_head: *mut Option<usize>,
    len: usize,
    next: usize,
    _marker: PhantomData<&'a mut T>,
}

impl<A: Allocator, T: SlabEntry> Slab<A, T> {
    /// Constructs a slab with `capacity` entries.
    pub fn new(allocator: A, capacity: usize) -> Self {
        let mut entries = Vec::with_capacity_in(capacity, allocator);
        for id in 0..capacity {
            entries.push(T::new_free((id + 1 < capacity).then_some(id + 1)));
        }

        Self {
            entries: entries.into_boxed_slice(),
            free_head: (capacity > 0).then_some(0),
        }
    }

    /// Returns the number of entries in the slab.
    pub fn capacity(&self) -> usize {
        self.entries.len()
    }

    /// Returns an entry by index.
    pub fn entry_mut(&mut self, id: usize) -> Option<&mut T> {
        self.entries.get_mut(id)
    }

    /// Returns a mutable iterator over allocated entries in the slab.
    pub fn entries_mut(&mut self) -> EntriesMut<'_, T> {
        EntriesMut {
            entries: self.entries.as_mut_ptr(),
            free_head: &mut self.free_head,
            len: self.entries.len(),
            next: 0,
            _marker: PhantomData,
        }
    }

    /// Allocates one entry from the free list.
    pub fn acquire(&mut self) -> Option<usize> {
        let id = self.free_head?;
        let entry = &self.entries[id];
        assert!(entry.is_free(), "free list head must point to a free slot");
        self.free_head = entry.next_free();
        Some(id)
    }

    /// Allocates one entry from the free list and returns a mutable guard to it.
    pub fn acquire_mut(&mut self) -> Option<EntryMut<'_, T>> {
        let id = self.acquire()?;
        Some(EntryMut {
            entries: self.entries.as_mut_ptr(),
            free_head: &mut self.free_head,
            id: Some(id),
            _marker: PhantomData,
        })
    }

    /// Releases an occupied entry back to the free list.
    pub fn release(&mut self, id: usize) {
        let entry = self
            .entries
            .get_mut(id)
            .expect("slab release requires a valid slot");
        assert!(!entry.is_free(), "slab slot must be occupied");
        entry.release(self.free_head);
        self.free_head = Some(id);
    }
}

/// Entry stored in a [`Slab`].
///
/// Free entries carry the next-free link for the slab. Occupied entries own
/// their normal live state.
pub trait SlabEntry {
    /// Constructs a free entry linked to `next`.
    fn new_free(next: Option<usize>) -> Self;

    /// Returns whether this entry is currently free.
    fn is_free(&self) -> bool;

    /// Returns the next-free link for a free entry.
    fn next_free(&self) -> Option<usize>;

    /// Releases an occupied entry and links it to `next`.
    fn release(&mut self, next: Option<usize>);
}

impl<T: SlabEntry> EntryMut<'_, T> {
    /// Returns the slot index.
    pub fn id(&self) -> usize {
        self.id.expect("entry guard must own a slot")
    }

    /// Releases the slot back to the slab immediately.
    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        let id = self.id.take().expect("entry guard must own a slot");
        unsafe {
            let entry = &mut *self.entries.add(id);
            assert!(!entry.is_free(), "slab slot must be occupied");
            entry.release(*self.free_head);
            *self.free_head = Some(id);
        }
    }
}

impl<'a, T: SlabEntry> Iterator for EntriesMut<'a, T> {
    type Item = EntryMut<'a, T>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.next < self.len {
            let id = self.next;
            self.next += 1;
            unsafe {
                let entry = &mut *self.entries.add(id);
                if !entry.is_free() {
                    return Some(EntryMut {
                        entries: self.entries,
                        free_head: self.free_head,
                        id: Some(id),
                        _marker: PhantomData,
                    });
                }
            }
        }
        None
    }
}

impl<T: SlabEntry> Deref for EntryMut<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        let id = self.id.expect("entry guard must own a slot");
        unsafe { &*self.entries.add(id) }
    }
}

impl<T: SlabEntry> DerefMut for EntryMut<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let id = self.id.expect("entry guard must own a slot");
        unsafe { &mut *self.entries.add(id) }
    }
}

impl<T: SlabEntry> Drop for EntryMut<'_, T> {
    fn drop(&mut self) {
        let Some(id) = self.id.take() else {
            return;
        };
        unsafe {
            if (&*self.entries.add(id)).is_free() {
                *self.free_head = Some(id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{alloc::Global, collections::BTreeSet};

    use proptest::prelude::*;

    use super::{Slab, SlabEntry};

    #[derive(Debug)]
    enum TestEntry {
        Free { next: Option<usize> },
        Occupied,
    }

    impl TestEntry {
        fn occupy(&mut self) {
            assert!(self.is_free(), "test entry must be free before occupy");
            *self = Self::Occupied;
        }
    }

    impl SlabEntry for TestEntry {
        fn new_free(next: Option<usize>) -> Self {
            Self::Free { next }
        }

        fn is_free(&self) -> bool {
            matches!(self, Self::Free { .. })
        }

        fn next_free(&self) -> Option<usize> {
            match self {
                Self::Free { next } => *next,
                Self::Occupied => None,
            }
        }

        fn release(&mut self, next: Option<usize>) {
            assert!(
                !self.is_free(),
                "test entry must be occupied before release"
            );
            *self = Self::Free { next };
        }
    }

    proptest! {
        #[test]
        fn acquire_exhausts_capacity_without_duplicates(capacity in 0usize..128) {
            let mut slab = Slab::<Global, TestEntry>::new(Global, capacity);
            let mut acquired = BTreeSet::new();

            for _ in 0..capacity {
                let id = slab.acquire().expect("slab must have free entries");
                prop_assert!(id < capacity);
                prop_assert!(acquired.insert(id), "slab returned a duplicate entry");
                slab.entry_mut(id).expect("acquired entry must exist").occupy();
            }

            prop_assert_eq!(acquired.len(), capacity);
            prop_assert!(slab.acquire().is_none());
        }

        #[test]
        fn released_entries_are_reused_from_the_free_list(capacity in 1usize..128) {
            let mut slab = Slab::<Global, TestEntry>::new(Global, capacity);
            let mut acquired = Vec::new();

            for _ in 0..capacity {
                let id = slab.acquire().expect("slab must have free entries");
                slab.entry_mut(id).expect("acquired entry must exist").occupy();
                acquired.push(id);
            }

            for id in acquired.iter().copied() {
                slab.release(id);
            }

            for expected in acquired.into_iter().rev() {
                let id = slab.acquire().expect("released entry must be reusable");
                prop_assert_eq!(id, expected);
                slab.entry_mut(id).expect("acquired entry must exist").occupy();
            }

            prop_assert!(slab.acquire().is_none());
        }

        #[test]
        fn acquire_mut_drop_restores_a_free_slot(capacity in 1usize..128) {
            let mut slab = Slab::<Global, TestEntry>::new(Global, capacity);
            let id = slab.acquire_mut().expect("slab must have free entries").id();
            let reacquired = slab
                .acquire()
                .expect("dropped free acquired entry must be reusable");
            prop_assert_eq!(reacquired, id);
        }

        #[test]
        fn entries_mut_yields_only_allocated_entries(capacity in 0usize..64) {
            let mut slab = Slab::<Global, TestEntry>::new(Global, capacity);
            let mut expected = BTreeSet::new();

            for _ in 0..(capacity / 2) {
                let mut entry = slab.acquire_mut().expect("slab must have free entries");
                let id = entry.id();
                entry.occupy();
                expected.insert(id);
            }

            let actual: BTreeSet<_> = slab.entries_mut().map(|entry| entry.id()).collect();
            prop_assert_eq!(actual, expected);
        }

        #[test]
        fn random_acquire_release_sequences_preserve_free_list(
            capacity in 0usize..64,
            operations in prop::collection::vec(any::<bool>(), 0..512),
        ) {
            let mut slab = Slab::<Global, TestEntry>::new(Global, capacity);
            let mut occupied = BTreeSet::new();

            assert_invariants(&slab, &occupied);

            for (step, acquire) in operations.into_iter().enumerate() {
                if acquire {
                    match slab.acquire() {
                        Some(id) => {
                            prop_assert!(occupied.len() < capacity);
                            prop_assert!(occupied.insert(id), "slab acquired an occupied entry");
                            slab.entry_mut(id).expect("acquired entry must exist").occupy();
                        }
                        None => {
                            prop_assert_eq!(occupied.len(), capacity);
                        }
                    }
                } else if !occupied.is_empty() {
                    let offset = step % occupied.len();
                    let id = *occupied.iter().nth(offset).expect("occupied entry must exist");
                    slab.release(id);
                    occupied.remove(&id);
                }

                assert_invariants(&slab, &occupied);
            }
        }
    }

    fn assert_invariants(slab: &Slab<Global, TestEntry>, occupied: &BTreeSet<usize>) {
        let capacity = slab.capacity();
        let mut free = BTreeSet::new();
        let mut cursor = slab.free_head;

        while let Some(id) = cursor {
            assert!(id < capacity, "free-list entry out of bounds");
            assert!(free.insert(id), "free-list cycle or duplicate entry");
            assert!(
                !occupied.contains(&id),
                "entry cannot be both occupied and free"
            );
            let entry = &slab.entries[id];
            assert!(entry.is_free(), "free list must point to free entries");
            cursor = entry.next_free();
        }

        for id in 0..capacity {
            let entry = &slab.entries[id];
            if occupied.contains(&id) {
                assert!(!entry.is_free(), "occupied entry marked free");
            } else {
                assert!(entry.is_free(), "unoccupied entry missing from free list");
                assert!(free.contains(&id), "free entry missing from free list");
            }
        }

        assert_eq!(
            occupied.len() + free.len(),
            capacity,
            "free and occupied entries must partition the slab"
        );
    }
}
