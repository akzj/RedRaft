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

pub mod zset_cow;

use parking_lot::RwLock;

pub trait CowValue {
    fn make_cow(&self, layer_level: usize) -> Self;
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
    layer: AtomicUsize,
    ref_count: Arc<AtomicI32>,
    merge_lock: Arc<RwLock<()>>,
    // Incremental merge state: (overlay_index, keys_processed_in_current_overlay)
    merge_progress: Arc<RwLock<Option<(usize, usize)>>>,
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
            layer: AtomicUsize::new(0),
            ref_count: Arc::new(AtomicI32::new(1)),
            merge_lock: Arc::new(RwLock::new(())),
            merge_progress: Arc::new(RwLock::new(None)),
        }
    }

    pub fn get_layer(&self) -> usize {
        self.layer.load(Ordering::Relaxed)
    }

    pub fn get<R, F>(&self, key: &K, f: F) -> Option<R>
    where
        F: FnOnce(&V) -> R,
    {
        let _merge_guard = self.merge_lock.read();
        for i in (0..self.layer.load(Ordering::Relaxed) + 1).rev() {
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
        if self.layer.load(Ordering::Relaxed) > 0 {
            (true, self.layer.load(Ordering::Relaxed))
        } else {
            (false, 0)
        }
    }

    pub fn get_mut<R, F>(&mut self, key: &K, f: F) -> Option<R>
    where
        F: FnOnce(&mut V, &mut M) -> R,
    {
        let _merge_guard = self.merge_lock.read();
        let (in_cow_mode, layer_level) = self.in_cow_mode();
        if in_cow_mode {
            // Check if key is removed in current cow_level
            {
                let overlay_guard = self.overlays[layer_level].read();
                if overlay_guard.removed.contains(key) {
                    return None;
                }
            }

            // Check if key exists in current cow_level
            {
                let overlay = &mut *self.overlays[layer_level].write();
                let (updated, metadata) = (&mut overlay.updated, &mut overlay.metadata);
                if let Some(value) = updated.get_mut(key) {
                    return Some(f(value, metadata));
                }
            }

            // COW from lower level: find the key in lower overlays
            // Search from top to bottom (higher index to lower index)
            for i in (0..layer_level).rev() {
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
                        let cow_value = value.make_cow(layer_level);

                        // Release read lock before acquiring write lock
                        drop(overlay_guard);

                        // Insert COW copy into current cow_level
                        let mut cow_level_guard = self.overlays[layer_level].write();
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

    pub fn remove(&mut self, key: &K) {
        let _merge_guard = self.merge_lock.read();
        let (in_cow_mode, cow_level) = self.in_cow_mode();
        if in_cow_mode {
            let overlay = &mut *self.overlays[cow_level].write();
            overlay.updated.remove(key);
            overlay.removed.insert(key.clone());
        } else {
            let overlay = &mut *self.overlays[0].write();
            overlay.updated.remove(key);
        }
    }

    /// Merge COW changes incrementally to avoid blocking other operations
    /// 
    /// This method processes a batch of keys at a time, releasing locks between batches.
    /// Returns `true` if merge is complete, `false` if more work remains.
    /// 
    /// # Arguments
    /// * `batch_size` - Maximum number of keys to process in this batch (default: 100)
    /// 
    /// # Returns
    /// * `true` - Merge completed, all overlays merged to base
    /// * `false` - More work remains, should be called again
    pub fn merge_cow_incremental(&self, batch_size: usize) -> bool {
        // Try to acquire merge lock (non-blocking)
        let merge_guard = match self.merge_lock.try_write() {
            Some(guard) => guard,
            None => {
                // Another merge is in progress, skip this call
                return false;
            }
        };

        // Decrement ref count
        self.ref_count.fetch_sub(1, Ordering::AcqRel);
        let ref_count = self.ref_count.load(Ordering::Acquire);
        
        // If ref_count > 1, other snapshots still exist, don't merge yet
        if ref_count > 1 {
            return false;
        }

        let current_level = self.layer.load(Ordering::Relaxed);
        if current_level == 0 {
            // Already merged, clear progress
            *self.merge_progress.write() = None;
            return true;
        }

        // Get or initialize merge progress
        let progress_guard = self.merge_progress.read();
        let start_overlay = progress_guard.unwrap_or((current_level, 0)).0;
        drop(progress_guard);

        // Collect keys to merge (without holding write locks for too long)
        let mut keys_to_merge: Vec<(usize, K, V)> = Vec::new();
        let mut keys_to_remove: Vec<(usize, K)> = Vec::new();
        let mut processed_count = 0;

        // Collect keys from overlays (top to bottom)
        for overlay_idx in (start_overlay..=current_level).rev() {
            if overlay_idx == 0 {
                break; // Skip base layer (index 0)
            }

            let overlay_guard = self.overlays[overlay_idx].read();
            let updated_keys: Vec<_> = overlay_guard.updated.keys().cloned().collect();
            let removed_keys: Vec<_> = overlay_guard.removed.iter().cloned().collect();
            drop(overlay_guard);

            // Process updated keys
            for key in updated_keys {
                if processed_count >= batch_size {
                    // Update progress and return false to continue later
                    *self.merge_progress.write() = Some((overlay_idx, processed_count));
                    return false;
                }

                let mut overlay_guard = self.overlays[overlay_idx].write();
                if let Some(mut value) = overlay_guard.updated.remove(&key) {
                    value.merge();
                    keys_to_merge.push((overlay_idx, key, value));
                    processed_count += 1;
                }
                drop(overlay_guard);
            }

            // Process removed keys
            for key in removed_keys {
                if processed_count >= batch_size {
                    *self.merge_progress.write() = Some((overlay_idx, processed_count));
                    return false;
                }

                let mut overlay_guard = self.overlays[overlay_idx].write();
                if overlay_guard.removed.remove(&key) {
                    keys_to_remove.push((overlay_idx, key));
                    processed_count += 1;
        }
                drop(overlay_guard);
            }

            // Move to next overlay after processing current one
        }

        // Apply collected changes to base layer
        if !keys_to_merge.is_empty() || !keys_to_remove.is_empty() {
            let mut base_guard = self.overlays[0].write();
            
            // Track which keys are updated vs removed for conflict resolution
            let updated_keys_set: HashSet<K> = keys_to_merge.iter().map(|(_, k, _)| k.clone()).collect();
            
            // Apply removals first (but skip if key is being updated)
            for (_, key) in keys_to_remove {
                if !updated_keys_set.contains(&key) {
                    base_guard.removed.insert(key);
                }
            }
            
            // Apply updates (updates take precedence over removals)
            for (_, key, value) in keys_to_merge {
                base_guard.removed.remove(&key);
                base_guard.updated.insert(key, value);
            }
            drop(base_guard);
        }

        // Check if all overlays are processed
        let mut all_empty = true;
        for i in 1..=current_level {
            let overlay_guard = self.overlays[i].read();
            if !overlay_guard.updated.is_empty() || !overlay_guard.removed.is_empty() {
                all_empty = false;
                break;
            }
        }

        if all_empty {
            // All overlays are empty, reset layer and clear progress
            self.layer.store(0, Ordering::Release);
            *self.merge_progress.write() = None;
            drop(merge_guard);
            return true;
        } else {
            // More work remains, update progress
            *self.merge_progress.write() = Some((current_level, processed_count));
            drop(merge_guard);
            return false;
        }
    }

    /// Merge COW changes synchronously (for backward compatibility)
    /// 
    /// This method processes all changes at once. For large datasets,
    /// consider using `merge_cow_incremental` instead to avoid blocking.
    pub fn merge_cow(&self) {
        // Keep calling incremental merge until complete
        const DEFAULT_BATCH_SIZE: usize = 1000;
        while !self.merge_cow_incremental(DEFAULT_BATCH_SIZE) {
            // Continue merging in batches
        }
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
        for overlay_idx in (0..self.layer.load(Ordering::Relaxed) + 1).rev() {
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
            layer: AtomicUsize::new(self.layer.load(Ordering::Acquire)),
            merge_progress: Arc::clone(&self.merge_progress),
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
        if self.layer.load(Ordering::Relaxed) >= OVERLAY_COUNT {
            panic!("CowStore has reached the maximum number of overlays");
        }

        self.ref_count.fetch_add(1, Ordering::AcqRel);
        let clone_store = (*self.inner).clone();
        // Increment write level
        self.layer.fetch_add(1, Ordering::AcqRel);

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
