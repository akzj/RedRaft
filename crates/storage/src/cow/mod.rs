use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    iter::Iterator,
    ops,
    sync::{
        atomic::{AtomicI32, AtomicUsize, Ordering},
        Arc,
    },
};

use parking_lot::RwLock;

pub trait CowValue {
    fn make_cow(&self) -> Self;
    fn is_cow(&self) -> bool;
    fn merge(&mut self);
}

pub struct CowOverlay<K, V, M> {
    updated: HashMap<K, V>,
    removed: HashSet<K>,
    metadata: M,
}

pub struct CowStoreInner<K, V, M, const OVERLAY_COUNT: usize = 10> {
    overlays: [Arc<RwLock<CowOverlay<K, V, M>>>; OVERLAY_COUNT],
    level: AtomicUsize,
    ref_count: Arc<AtomicI32>,
    merge_lock: Arc<RwLock<()>>,
}

impl<K, V, M> CowOverlay<K, V, M>
where
    K: Eq + Hash,
    V: CowValue,
    M: Clone + Default,
{
    pub fn new() -> Self {
        Self {
            updated: HashMap::new(),
            removed: HashSet::new(),
            metadata: M::default(),
        }
    }
}

impl<K, V, M, const OVERLAY_COUNT: usize> CowStoreInner<K, V, M, OVERLAY_COUNT>
where
    K: Eq + Hash + Clone,
    V: CowValue + Clone,
    M: Clone + Default,
{
    pub fn new() -> Self {
        Self {
            overlays: core::array::from_fn(|_| Arc::new(RwLock::new(CowOverlay::new()))),
            level: AtomicUsize::new(0),
            ref_count: Arc::new(AtomicI32::new(1)),
            merge_lock: Arc::new(RwLock::new(())),
        }
    }

    pub fn get<R, F>(&self, key: &K, f: F) -> Option<R>
    where
        F: FnOnce(&V) -> R,
    {
        let _merge_guard = self.merge_lock.read();
        for i in (0..self.level.load(Ordering::Relaxed) + 1).rev() {
            let overlay_guard = self.overlays[i].read();
            if let Some(value) = overlay_guard.updated.get(key) {
                return Some(f(value));
            } else if overlay_guard.removed.contains(key) {
                return None;
            }
            drop(overlay_guard);
        }
        None
    }

    pub fn in_cow_mode(&self) -> (bool, usize) {
        if self.level.load(Ordering::Relaxed) > 0 {
            (true, self.level.load(Ordering::Relaxed))
        } else {
            (false, 0)
        }
    }

    pub fn get_mut<R, F>(&mut self, key: &K, f: F) -> Option<R>
    where
        F: FnOnce(&mut V, &mut M) -> R,
    {
        let _merge_guard = self.merge_lock.read();
        let (in_cow_mode, cow_level) = self.in_cow_mode();
        if in_cow_mode {
            // Check if key is removed in current cow_level
            {
                let overlay_guard = self.overlays[cow_level].read();
                if overlay_guard.removed.contains(key) {
                    return None;
                }
            }

            // Check if key exists in current cow_level
            {
                let overlay = &mut *self.overlays[cow_level].write();
                let (updated, metadata) = (&mut overlay.updated, &mut overlay.metadata);
                if let Some(value) = updated.get_mut(key) {
                    return Some(f(value, metadata));
                }
            }

            // COW from lower level: find the key in lower overlays
            // Search from top to bottom (higher index to lower index)
            for i in (0..cow_level).rev() {
                // Check if key is removed in this overlay
                {
                    let overlay_guard = self.overlays[i].read();
                    if overlay_guard.removed.contains(key) {
                        return None;
                    }
                }

                // If key exists in this overlay, perform COW
                {
                    let overlay_guard = self.overlays[i].read();
                    if let Some(value) = overlay_guard.updated.get(key) {
                        // Create COW copy directly from reference (no clone needed)
                        // IMPORTANT: Do NOT remove from lower layer - keep it for snapshot views
                        // make_cow() only clones Arc (O(1)), not the entire data structure
                        let cow_value = value.make_cow();

                        // Release read lock before acquiring write lock
                        drop(overlay_guard);

                        // Insert COW copy into current cow_level
                        let mut cow_level_guard = self.overlays[cow_level].write();
                        cow_level_guard.updated.insert(key.clone(), cow_value);

                        // Get mutable reference to the COW copy and call closure
                        let overlay = &mut *cow_level_guard;
                        let (updated, metadata) = (&mut overlay.updated, &mut overlay.metadata);
                        if let Some(value) = updated.get_mut(key) {
                            return Some(f(value, metadata));
                        }
                    }
                }
            }
            None
        } else {
            // Not in COW mode, directly access overlay[0]
            let overlay = &mut *self.overlays[0].write();
            let (updated, metadata) = (&mut overlay.updated, &mut overlay.metadata);
            if let Some(value) = updated.get_mut(key) {
                Some(f(value, metadata))
            } else {
                None
            }
        }
    }

    pub fn insert<F>(&mut self, key: K, f: F)
    where
        F: FnOnce(&mut M) -> V,
    {
        let _merge_guard = self.merge_lock.read();
        let (in_cow_mode, cow_level) = self.in_cow_mode();
        if in_cow_mode {
            let overlay = &mut *self.overlays[cow_level].write();
            let value = f(&mut overlay.metadata);
            overlay.removed.remove(&key);
            overlay.updated.insert(key, value);
        } else {
            let overlay = &mut *self.overlays[0].write();
            let value = f(&mut overlay.metadata);
            overlay.updated.insert(key, value);
        }
    }

    pub fn merge_cow(&self) {
        let _merge_guard = self.merge_lock.write();
        if self.ref_count.fetch_sub(1, Ordering::AcqRel) > 1 {
            return;
        }

        let current_level = self.level.load(Ordering::Relaxed);
        if current_level == 0 {
            return;
        }

        let mut merged_updated = HashMap::new();
        let mut merged_removed = HashSet::new();

        for i in (1..current_level + 1).rev() {
            let mut overlay_guard = self.overlays[i].write();
            for (key, mut value) in overlay_guard.updated.drain() {
                // upper layer removed this key, skip it
                if merged_removed.contains(&key) {
                    continue;
                }

                // upper layer updated this key, skip it
                if merged_updated.contains_key(&key) {
                    continue;
                }

                value.merge();
                merged_updated.insert(key, value);
            }

            for key in overlay_guard.removed.drain() {
                // upper layer updated this key, remove it from merged_updated
                if merged_updated.contains_key(&key) {
                    continue;
                } else {
                    merged_removed.insert(key);
                }
            }
        }

        let mut base_guard = self.overlays[0].write();
        for key in merged_removed.drain() {
            base_guard.removed.insert(key);
        }
        for (key, value) in merged_updated.drain() {
            base_guard.updated.insert(key, value);
        }

        // Reset level to 0 after merging
        self.level.store(0, Ordering::Release);
    }

    /// Iterate over key-value pairs using a closure
    /// Iterates from top to bottom overlays, skipping removed keys
    /// The closure receives (&K, &V) and returns true to continue iteration, false to stop
    /// Note: Values are returned as references (no clone)
    pub fn range<F>(&self, mut f: F)
    where
        F: FnMut(&K, &V) -> bool,
    {
        let _merge_guard = self.merge_lock.read();
        let mut seen_keys = HashSet::new();

        // Iterate from top to bottom (level to 0)
        for overlay_idx in (0..self.level.load(Ordering::Relaxed) + 1).rev() {
            let guard = self.overlays[overlay_idx].read();

            // Iterate over all keys in this overlay
            for (key, value) in guard.updated.iter() {
                // Skip if we've already seen this key (in a higher overlay)
                if seen_keys.contains(key) {
                    continue;
                }

                // Skip if this key is removed in current overlay
                if guard.removed.contains(key) {
                    continue;
                }

                // Mark as seen
                seen_keys.insert(key.clone());

                // Call closure - if it returns false, stop iteration
                if !f(key, value) {
                    return;
                }
            }
        }
    }
}

impl<K, V, M, const OVERLAY_COUNT: usize> Clone for CowStoreInner<K, V, M, OVERLAY_COUNT>
where
    K: Eq + Hash + Clone,
    V: CowValue + Clone,
    M: Clone + Default,
{
    fn clone(&self) -> Self {
        Self {
            overlays: self.overlays.clone(),
            ref_count: self.ref_count.clone(),
            merge_lock: Arc::clone(&self.merge_lock),
            level: AtomicUsize::new(self.level.load(Ordering::Acquire)),
        }
    }
}
pub struct CowStore<K, V, M, const OVERLAY_COUNT: usize = 10>
where
    K: Eq + Hash + Clone,
    V: CowValue + Clone,
    M: Clone + Default,
{
    inner: Arc<CowStoreInner<K, V, M, OVERLAY_COUNT>>,
}

impl<K, V, M, const OVERLAY_COUNT: usize> ops::Deref for CowStore<K, V, M, OVERLAY_COUNT>
where
    K: Eq + Hash + Clone,
    V: CowValue + Clone,
    M: Clone + Default,
{
    type Target = CowStoreInner<K, V, M, OVERLAY_COUNT>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<K, V, M, const OVERLAY_COUNT: usize> CowStore<K, V, M, OVERLAY_COUNT>
where
    K: Eq + Hash + Clone,
    V: CowValue + Clone,
    M: Clone + Default,
{
    pub fn new() -> Self {
        Self {
            inner: Arc::new(CowStoreInner::new()),
        }
    }

    /// Creates a new snapshot.
    ///
    /// The snapshot captures the state of all overlays up to the current level.
    /// Since writes always go to a new higher-level overlay (append-only),
    /// concurrent snapshot creation is safe: each snapshot sees a consistent,
    /// monotonically increasing view of history.

    pub fn make_snapshot(&self) -> CowStoreSnapshot<K, V, M, OVERLAY_COUNT> {
        if self.level.load(Ordering::Relaxed) >= OVERLAY_COUNT {
            panic!("CowStore has reached the maximum number of overlays");
        }

        self.ref_count.fetch_add(1, Ordering::AcqRel);
        let clone_store = (*self.inner).clone();
        // Increment write level
        self.level.fetch_add(1, Ordering::AcqRel);

        CowStoreSnapshot::new(Arc::new(clone_store), Arc::clone(&self.inner))
    }
}

pub struct CowStoreSnapshot<K, V, M, const OVERLAY_COUNT: usize = 10>
where
    K: Eq + Hash + Clone,
    V: CowValue + Clone,
    M: Clone + Default,
{
    read: Arc<CowStoreInner<K, V, M, OVERLAY_COUNT>>,
    write: Arc<CowStoreInner<K, V, M, OVERLAY_COUNT>>,
}

impl<K, V, M, const OVERLAY_COUNT: usize> CowStoreSnapshot<K, V, M, OVERLAY_COUNT>
where
    K: Eq + Hash + Clone,
    V: CowValue + Clone,
    M: Clone + Default,
{
    pub fn new(
        read: Arc<CowStoreInner<K, V, M, OVERLAY_COUNT>>,
        write: Arc<CowStoreInner<K, V, M, OVERLAY_COUNT>>,
    ) -> Self {
        Self { read, write }
    }
}

impl<K, V, M, const OVERLAY_COUNT: usize> Drop for CowStoreSnapshot<K, V, M, OVERLAY_COUNT>
where
    K: Eq + Hash + Clone,
    V: CowValue + Clone,
    M: Clone + Default,
{
    fn drop(&mut self) {
        self.write.merge_cow();
    }
}

impl<K, V, M, const OVERLAY_COUNT: usize> ops::Deref for CowStoreSnapshot<K, V, M, OVERLAY_COUNT>
where
    K: Eq + Hash + Clone,
    V: CowValue + Clone,
    M: Clone + Default,
{
    type Target = CowStoreInner<K, V, M, OVERLAY_COUNT>;

    fn deref(&self) -> &Self::Target {
        &self.read
    }
}
