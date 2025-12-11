//! Shard data structure with metadata
//!
//! Contains both data and metadata (like Raft apply index) for each shard
//!
//! ShardData contains all data structures (List, Set, ZSet) in a unified HashMap

use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::{
    memory::bitmap::BitmapData,
    memory::{ListData, SetDataCow, ZSetDataCow},
};

/// Shard metadata
#[derive(Debug, Clone)]
pub struct ShardMetadata {
    /// Raft log apply index (last applied log entry index)
    /// This is set when creating a snapshot
    pub apply_index: Option<u64>,

    /// Last snapshot index (for incremental snapshots)
    pub last_snapshot_index: Option<u64>,

    /// Shard creation time
    pub created_at: u64,

    /// Last update time
    pub last_updated: u64,
}

impl ShardMetadata {
    pub fn new() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            apply_index: None,
            last_snapshot_index: None,
            created_at: now,
            last_updated: now,
        }
    }

    /// Update apply index (called during snapshot creation)
    pub fn set_apply_index(&mut self, index: u64) {
        self.apply_index = Some(index);
        self.last_updated = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Update snapshot index
    pub fn set_snapshot_index(&mut self, index: u64) {
        self.last_snapshot_index = Some(index);
        self.last_updated = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }
}

impl Default for ShardMetadata {
    fn default() -> Self {
        Self::new()
    }
}

/// Unified data type enum with COW support
///
/// Wraps all Redis data types (List, Set, ZSet, Bitmap) in a single enum.
/// Supports type detection and COW mechanism.
#[derive(Debug, Clone)]
pub enum DataCow {
    /// List data (direct storage, cloned when modified)
    List(ListData),
    /// Set data (with COW support)
    Set(SetDataCow),
    /// ZSet data (with COW support)
    ZSet(ZSetDataCow),
    /// Bitmap data (direct storage, cloned when modified)
    Bitmap(BitmapData),
}

impl DataCow {
    /// Get the type name of the data
    pub fn type_name(&self) -> &'static str {
        match self {
            DataCow::List(_) => "list",
            DataCow::Set(_) => "set",
            DataCow::ZSet(_) => "zset",
            DataCow::Bitmap(_) => "bitmap",
        }
    }

    /// Check if the data is empty
    pub fn is_empty(&self) -> bool {
        match self {
            DataCow::List(l) => l.is_empty(),
            DataCow::Set(s) => s.is_empty(),
            DataCow::ZSet(z) => z.is_empty(),
            DataCow::Bitmap(b) => b.is_empty(),
        }
    }

    /// Get length/size of the data
    pub fn len(&self) -> usize {
        match self {
            DataCow::List(l) => l.len(),
            DataCow::Set(s) => s.len(),
            DataCow::ZSet(z) => z.len(),
            DataCow::Bitmap(b) => b.len(),
        }
    }

    /// Create a COW instance (for types that support COW)
    pub fn make_cow(&mut self) -> DataCow {
        match self {
            DataCow::Set(s) => DataCow::Set(s.make_cow()),
            DataCow::ZSet(z) => DataCow::ZSet(z.make_cow()),
            // List and Bitmap don't have COW, just clone
            DataCow::List(l) => DataCow::List(l.clone()),
            DataCow::Bitmap(b) => DataCow::Bitmap(b.clone()),
        }
    }

    /// Merge COW changes (for types that support COW)
    pub fn merge_cow(&mut self) {
        match self {
            DataCow::Set(s) => {
                if s.is_in_cow_mode() {
                    s.merge_cow();
                }
            }
            DataCow::ZSet(z) => {
                if z.is_in_cow_mode() {
                    z.merge_cow();
                }
            }
            // List and Bitmap don't have COW, no-op
            DataCow::List(_) | DataCow::Bitmap(_) => {}
        }
    }

    /// Serialize base data for snapshot
    /// 
    /// Extracts base data from COW structure and serializes it
    pub fn serialize_base(&self) -> Result<Vec<u8>, String> {
        use bincode::serde::encode_to_vec;
        use bincode::config::standard;
        
        match self {
            DataCow::List(list) => {
                encode_to_vec(list, standard())
                    .map_err(|e| format!("Failed to serialize ListData: {}", e))
            }
            DataCow::Set(set_cow) => {
                let base = set_cow.get_base_for_serialization();
                encode_to_vec(&*base, standard())
                    .map_err(|e| format!("Failed to serialize SetData: {}", e))
            }
            DataCow::ZSet(zset_cow) => {
                let base = zset_cow.get_base_for_serialization();
                encode_to_vec(&*base, standard())
                    .map_err(|e| format!("Failed to serialize ZSetData: {}", e))
            }
            DataCow::Bitmap(bitmap) => {
                encode_to_vec(bitmap, standard())
                    .map_err(|e| format!("Failed to serialize BitmapData: {}", e))
            }
        }
    }
}

/// Unified Store with Incremental Copy-on-Write (COW) support
///
/// Combines all data types (List, Set, ZSet, Bitmap) in a single store with:
/// - Type detection: O(1) lookup via unified HashMap
/// - COW mechanism: Only changed items are recorded, NOT full data copy
/// - `make_snapshot()`: Only clones Arc (increases ref count), NO data copy
/// - Write operations: Records changes in COW cache (only changed items), NO full copy
/// - Read operations: Merges COW cache + base data (O(1) lookup)
/// - `merge_cow()`: Applies only changed items to base (O(M) where M = changes, not total data)
///
/// This avoids full HashMap copy even for 1000 billion keys when only 3 keys change.
#[derive(Debug, Clone)]
pub struct MemStoreCow {
    /// Base data (shared via Arc<RwLock<>>, can be directly modified without clone)
    /// Unified storage for all data types: key -> DataCow
    pub(crate) base: Arc<RwLock<HashMap<Vec<u8>, DataCow>>>,

    /// COW cache: Updated/added items (only changed items)
    /// DataCow is created via make_cow() here (required for consistency)
    pub(crate) updated: Option<HashMap<Vec<u8>, DataCow>>,

    /// COW cache: Removed keys
    pub(crate) removed: Option<HashSet<Vec<u8>>>,
}

impl MemStoreCow {
    /// Create a new empty unified store with COW support
    pub fn new() -> Self {
        Self {
            base: Arc::new(RwLock::new(HashMap::new())),
            updated: None,
            removed: None,
        }
    }

    /// Check if in COW mode (has snapshot)
    pub fn is_cow_mode(&self) -> bool {
        self.updated.is_some()
    }

    /// Create a snapshot (only increases reference count, NO data copy)
    ///
    /// Returns a cloned Arc that shares the same base data.
    /// Write operations will use `make_cow()` to create COW instances instead of copying data.
    pub fn make_snapshot(&mut self) -> Arc<RwLock<HashMap<Vec<u8>, DataCow>>> {
        if self.is_cow_mode() {
            // Already in COW mode, just return existing base
            return Arc::clone(&self.base);
        }

        // Enter COW mode: initialize caches
        self.updated = Some(HashMap::new());
        self.removed = Some(HashSet::new());

        // ✅ Only clone Arc (O(1)), NO data copy
        Arc::clone(&self.base)
    }

    /// Merge COW changes back to base (applies only changed items)
    ///
    /// This is called when snapshot is no longer needed.
    /// Only changed items are applied via RwLock, not full data copy.
    pub fn merge_cow(&mut self) {
        if !self.is_cow_mode() {
            return;
        }

        let updated = self.updated.take();
        let removed = self.removed.take();

        // Collect updated keys before processing (for removal check)
        let updated_keys: HashSet<Vec<u8>> = updated
            .as_ref()
            .map(|u| u.keys().cloned().collect())
            .unwrap_or_default();

        // ✅ Get write lock and directly modify base (NO clone!)
        let mut base = self.base.write();

        // Apply updates/additions first (updated takes precedence over removed)
        if let Some(mut updated) = updated {
            for (key, mut data_cow) in updated.drain() {
                // Merge the DataCow's changes to its base first (for types that support COW)
                data_cow.merge_cow();

                // Replace or insert the merged DataCow
                base.insert(key, data_cow);
            }
        }

        // Apply removals (only if not in updated - updated takes precedence)
        if let Some(ref removed) = removed {
            for key in removed {
                // Only remove if not being updated (updated already applied above)
                if !updated_keys.contains(key) {
                    base.remove(key);
                }
            }
        }
    }

    /// Get key type (O(1) lookup)
    pub fn key_type(&self, key: &[u8]) -> Option<&'static str> {
        if self.is_cow_mode() {
            // Check COW cache first
            if let Some(ref updated) = self.updated {
                if let Some(data) = updated.get(key) {
                    return Some(data.type_name());
                }
            }
            // Check if removed
            if let Some(ref removed) = self.removed {
                if removed.contains(key) {
                    return None;
                }
            }
        }
        // Check base
        let base = self.base.read();
        base.get(key).map(|data| data.type_name())
    }

    /// Check if key exists (O(1) lookup)
    pub fn contains_key(&self, key: &[u8]) -> bool {
        if self.is_cow_mode() {
            // Check COW cache first (updated takes precedence over removed)
            if let Some(ref updated) = self.updated {
                if updated.contains_key(key) {
                    return true;
                }
            }

            // Check if removed (only if not in updated)
            if let Some(ref removed) = self.removed {
                if removed.contains(key) {
                    return false;
                }
            }
        }
        // Check base
        let base = self.base.read();
        base.contains_key(key)
    }

    /// Delete key from any data type
    pub fn del(&mut self, key: &[u8]) -> bool {
        if self.is_cow_mode() {
            let updated = self.updated.as_mut().unwrap();
            let removed = self.removed.as_mut().unwrap();

            // Remove from updated cache if present
            if updated.remove(key).is_some() {
                removed.insert(key.to_vec());
                return true;
            }

            // Check if already in removed cache
            if removed.contains(key) {
                return false;
            }

            // Check base
            let base = self.base.read();
            if base.contains_key(key) {
                removed.insert(key.to_vec());
                return true;
            }
            false
        } else {
            // No COW mode: directly remove from base
            let mut base = self.base.write();
            base.remove(key).is_some()
        }
    }

    /// Get DataCow for key (returns reference - for read operations)
    ///
    /// Note: This method can only return references from COW cache, not from base
    /// (due to lock lifetime constraints). For base lookups, use other methods.
    pub fn get(&self, key: &[u8]) -> Option<&DataCow> {
        if self.is_cow_mode() {
            // Check COW cache first
            if let Some(ref updated) = self.updated {
                if let Some(data) = updated.get(key) {
                    return Some(data);
                }
            }

            // Check if removed
            if let Some(ref removed) = self.removed {
                if removed.contains(key) {
                    return None;
                }
            }
            // If not in updated and not removed, we can't return a reference from base
            // (due to lock lifetime), so return None
            return None;
        }

        // No COW mode: can't return reference from base (lock lifetime)
        None
    }

    /// Get mutable DataCow for key (creates COW instance if needed)
    ///
    /// This method:
    /// 1. Checks if data is already in COW cache
    /// 2. If yes, returns mutable reference (no additional copy!)
    /// 3. If no, uses `make_cow()` to create COW instance (no full copy!)
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut DataCow> {
        if self.is_cow_mode() {
            let updated = self.updated.as_mut().unwrap();
            let removed = self.removed.as_mut().unwrap();

            // Check if already in COW cache
            if updated.contains_key(key) {
                // Already has COW instance: return mutable reference (no additional copy!)
                return updated.get_mut(key);
            }

            // Not in COW cache: create COW instance via make_cow() (no full copy!)
            let data_cow = {
                // Check if removed
                if removed.contains(key) {
                    // For removed key, we can't determine type, so return None
                    // Caller should specify type when creating new
                    return None;
                } else {
                    // Get from base and create COW instance
                    let base = self.base.read();
                    if let Some(base_data) = base.get(key) {
                        // Clone the DataCow struct (only clones Arc for COW types, NO data copy!)
                        // Then call make_cow() to create COW instance (NO full copy!)
                        let mut base_data_clone = base_data.clone();
                        base_data_clone.make_cow()
                    } else {
                        // Key doesn't exist, can't determine type
                        return None;
                    }
                }
            };
            updated.insert(key.to_vec(), data_cow);

            // Remove from removed cache if present
            removed.remove(key);

            // Return mutable reference to the newly inserted DataCow
            updated.get_mut(key)
        } else {
            // No snapshot: cannot return mutable reference from lock
            // Callers should create a snapshot first to enable COW mode
            None
        }
    }

    /// Insert or update DataCow for key
    ///
    /// If key exists with different type, it will be replaced.
    pub fn insert(&mut self, key: Vec<u8>, data: DataCow) {
        if self.is_cow_mode() {
            let updated = self.updated.as_mut().unwrap();
            let removed = self.removed.as_mut().unwrap();

            // Remove from removed cache if present
            removed.remove(&key);

            // Insert into updated cache
            updated.insert(key, data);
        } else {
            // No COW mode: directly insert into base
            let mut base = self.base.write();
            base.insert(key, data);
        }
    }

    /// Get total key count across all data types
    pub fn key_count(&self) -> usize {
        if self.is_cow_mode() {
            let base = self.base.read();
            let base_count = base.len();
            let updated_count = self.updated.as_ref().map(|u| u.len()).unwrap_or(0);
            let removed_count = self.removed.as_ref().map(|r| r.len()).unwrap_or(0);
            // Count = base + new in updated - removed
            base_count + updated_count - removed_count
        } else {
            let base = self.base.read();
            base.len()
        }
    }
}

/// Shard data container
///
/// Contains all data structures (List, Set, ZSet, Bitmap) in a unified store
/// with type detection and COW support.
pub struct ShardStore {
    /// Unified store for all data types with COW support
    pub store: MemStoreCow,

    /// Shard metadata
    pub metadata: ShardMetadata,
}

impl ShardStore {
    pub fn new() -> Self {
        Self {
            store: MemStoreCow::new(),
            metadata: ShardMetadata::new(),
        }
    }

    /// Get key type (O(1) lookup)
    pub fn key_type(&self, key: &[u8]) -> Option<&'static str> {
        self.store.key_type(key)
    }

    /// Check if key exists (O(1) lookup)
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.store.contains_key(key)
    }

    /// Delete key from any data type
    pub fn del(&mut self, key: &[u8]) -> bool {
        self.store.del(key)
    }

    /// Get total key count across all data types
    pub fn key_count(&self) -> usize {
        self.store.key_count()
    }
}
