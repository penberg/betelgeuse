//! Fixed-capacity object slab.
//!
//! A slab owns a homogeneous array of objects and an intrusive free list over
//! the unused entries. Allocation removes one entry from the free list.
//! Release returns the entry to the free list. The backing array is allocated
//! once during construction and is never resized.

use std::alloc::Allocator;

/// Fixed-capacity object slab backed by an intrusive free list.
pub struct Slab<T, A: Allocator> {
    pub(crate) entries: Box<[T], A>,
    free_head: Option<usize>,
}

impl<T: SlabEntry, A: Allocator> Slab<T, A> {
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

    /// Returns an occupied entry by index.
    pub fn occupied_mut(&mut self, id: usize) -> Option<&mut T> {
        self.entries.get_mut(id).filter(|entry| !entry.is_free())
    }

    /// Returns a mutable iterator over all entries in the slab, both free and
    /// occupied. Callers are expected to use [`SlabEntry::is_free`] to filter
    /// when they only want occupied entries.
    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, T> {
        self.entries.iter_mut()
    }

    /// Allocates one entry from the free list.
    pub fn acquire(&mut self) -> Option<usize> {
        let id = self.free_head?;
        let entry = &self.entries[id];
        assert!(entry.is_free(), "free list head must point to a free slot");
        self.free_head = entry.next_free();
        Some(id)
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
            let mut slab = Slab::<TestEntry, Global>::new(Global, capacity);
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
            let mut slab = Slab::<TestEntry, Global>::new(Global, capacity);
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
        fn random_acquire_release_sequences_preserve_free_list(
            capacity in 0usize..64,
            operations in prop::collection::vec(any::<bool>(), 0..512),
        ) {
            let mut slab = Slab::<TestEntry, Global>::new(Global, capacity);
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

    fn assert_invariants(slab: &Slab<TestEntry, Global>, occupied: &BTreeSet<usize>) {
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
