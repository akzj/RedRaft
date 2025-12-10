//! Bitmap Store with Incremental Copy-on-Write (COW)
//!
//! True incremental COW semantics - only changed bitmaps are recorded, NOT full data copy:
//! - Snapshot: Only clones Arc (increases ref count), NO data copy
//! - Write: Records changes in small COW cache (only changed bitmaps), NO full copy
//! - Read: Merges COW cache + base data (O(1) lookup)
//! - Merge: Applies only changed bitmaps to base (O(M) where M = changes, not total data)
//!
//! Note: BitmapData is directly copied when modified (required for consistency).
//! This is acceptable because individual bitmaps are typically small.
//!
//! Example: 1000 billion bitmaps, modify 3 bitmaps
//! - Old approach (Arc::make_mut): Copies all 1000 billion bitmaps ❌
//! - This approach: Only records 3 changed bitmaps in small HashMap ✅
//!
//! This module provides the core data structure for bitmaps,
//! without implementing Redis API traits.

use parking_lot::RwLock;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

/// Bitmap data structure (Vec<u8> where each byte contains 8 bits)
pub type BitmapData = Vec<u8>;

/// Bitmap Store with Incremental Copy-on-Write (COW) support
///
/// True incremental COW semantics:
/// - `make_snapshot()`: Only clones Arc (increases ref count), NO data copy
/// - `setbit()`/`bitop()`: Records changes in small COW cache (only changed bitmaps), NO full copy
/// - Read operations: Merges COW cache + base data (O(1) lookup)
/// - `merge_cow()`: Applies only changed bitmaps to base via RwLock (O(M) where M = changes, not total data)
///
/// Note: BitmapData is copied when modified (required for consistency), but only for changed bitmaps.
/// This avoids full HashMap copy even for 1000 billion bitmaps when only 3 bitmaps change.
#[derive(Debug, Clone)]
pub struct BitmapStoreCow {
    /// Base data (shared via Arc<RwLock<>>, can be directly modified without clone)
    base: Arc<RwLock<HashMap<Vec<u8>, BitmapData>>>,

    /// COW cache: Updated/added bitmaps (only changed bitmaps)
    /// BitmapData is copied here (required for consistency)
    bitmaps_updated: Option<HashMap<Vec<u8>, BitmapData>>,

    /// COW cache: Removed bitmap keys
    bitmaps_removed: Option<HashSet<Vec<u8>>>,
}

impl BitmapStoreCow {
    /// Create a new empty BitmapStore with COW support
    pub fn new() -> Self {
        Self {
            base: Arc::new(RwLock::new(HashMap::new())),
            bitmaps_updated: None,
            bitmaps_removed: None,
        }
    }

    /// Create from existing HashMap
    pub fn from_data(data: HashMap<Vec<u8>, BitmapData>) -> Self {
        Self {
            base: Arc::new(RwLock::new(data)),
            bitmaps_updated: None,
            bitmaps_removed: None,
        }
    }

    /// Check if in COW mode (has snapshot)
    fn is_cow_mode(&self) -> bool {
        self.bitmaps_updated.is_some()
    }

    /// Create a snapshot (only increases reference count, NO data copy)
    ///
    /// Returns a cloned Arc that shares the same base data.
    /// Write operations will record changes in COW cache instead of copying data.
    pub fn make_snapshot(&mut self) -> Arc<RwLock<HashMap<Vec<u8>, BitmapData>>> {
        if self.is_cow_mode() {
            // Already in COW mode, just return existing base
            return Arc::clone(&self.base);
        }

        // Enter COW mode: initialize caches
        self.bitmaps_updated = Some(HashMap::new());
        self.bitmaps_removed = Some(HashSet::new());

        // ✅ Only clone Arc (O(1)), NO data copy
        Arc::clone(&self.base)
    }

    /// Merge COW changes back to base (applies only changed bitmaps)
    ///
    /// This is called when snapshot is no longer needed.
    /// Only changed bitmaps are applied via RwLock, not full data copy.
    pub fn merge_cow(&mut self) {
        if !self.is_cow_mode() {
            return;
        }

        let updated = self.bitmaps_updated.take();
        let removed = self.bitmaps_removed.take();

        // ✅ Get write lock and directly modify base (NO clone!)
        let mut base = self.base.write();

        // Apply updates/additions first (updated takes precedence over removed)
        // This handles both new and updated bitmaps
        if let Some(ref updated) = updated {
            for (key, bitmap_data) in updated {
                // BitmapData is copied here (required for consistency)
                base.insert(key.clone(), bitmap_data.clone());
            }
        }

        // Apply removals (only if not in updated - updated takes precedence)
        if let Some(ref removed) = removed {
            for key in removed {
                // Only remove if not being updated (updated already applied above)
                if updated.as_ref().map_or(true, |u| !u.contains_key(key)) {
                    base.remove(key);
                }
            }
        }

        // Write lock is released here, no need to replace base
    }

    /// Get bitmap for key (returns a copy - only use when modification is needed)
    ///
    /// ⚠️ WARNING: This method copies the entire bitmap!
    /// For read-only operations, use optimized methods instead:
    /// - `getbit(key, offset)` - get bit without copy
    /// - `bitcount(key)` - count bits without copy
    ///
    /// This method should only be used when you need to modify the bitmap
    /// (e.g., in setbit/bitop operations).
    pub fn get_bitmap(&self, key: &[u8]) -> Option<BitmapData> {
        if self.is_cow_mode() {
            // Check COW cache first (updated takes precedence over removed)
            if let Some(ref updated) = self.bitmaps_updated {
                if let Some(bitmap) = updated.get(key) {
                    return Some(bitmap.clone());
                }
            }

            // Check if removed (only if not in updated)
            if let Some(ref removed) = self.bitmaps_removed {
                if removed.contains(key) {
                    return None;
                }
            }
            // If not in updated and not removed, fall through to base
        }

        // Fall back to base (read lock)
        let base = self.base.read();
        base.get(key).cloned()
    }

    /// Check if key exists (read operation, merges COW cache + base)
    pub fn contains_key(&self, key: &[u8]) -> bool {
        if self.is_cow_mode() {
            // Check COW cache first (updated takes precedence over removed)
            if let Some(ref updated) = self.bitmaps_updated {
                if updated.contains_key(key) {
                    return true;
                }
            }

            // Check if removed (only if not in updated)
            if let Some(ref removed) = self.bitmaps_removed {
                if removed.contains(key) {
                    return false;
                }
            }
            // If not in updated and not removed, fall through to base
        }

        // Fall back to base (read lock)
        let base = self.base.read();
        base.contains_key(key)
    }

    /// Get bitmap length in bytes (read operation, no copy)
    pub fn len(&self, key: &[u8]) -> Option<usize> {
        if self.is_cow_mode() {
            // Check COW cache first (updated takes precedence over removed)
            if let Some(ref updated) = self.bitmaps_updated {
                if let Some(bitmap) = updated.get(key) {
                    return Some(bitmap.len());
                }
            }

            // Check if removed (only if not in updated)
            if let Some(ref removed) = self.bitmaps_removed {
                if removed.contains(key) {
                    return None;
                }
            }
            // If not in updated and not removed, fall through to base
        }

        // Fall back to base (read lock)
        let base = self.base.read();
        base.get(key).map(|bitmap| bitmap.len())
    }

    /// Set bit at offset (incremental COW: only records change)
    ///
    /// Returns the previous value of the bit (0 or 1).
    pub fn setbit(&mut self, key: Vec<u8>, offset: usize, value: bool) -> Option<bool> {
        if self.is_cow_mode() {
            let updated = self.bitmaps_updated.as_mut().unwrap();
            let removed = self.bitmaps_removed.as_mut().unwrap();

            // Check if already in COW cache (already copied)
            if let Some(bitmap) = updated.get_mut(&key) {
                // Already copied: directly modify (no additional copy!)
                Some(set_bit_in_bitmap(bitmap, offset, value))
            } else {
                // Not in COW cache: copy from base once, then modify
                let mut bitmap = {
                    // Check if removed
                    if removed.contains(&key) {
                        Vec::new()
                    } else {
                        // Copy from base (first modification)
                        let base = self.base.read();
                        base.get(&key).cloned().unwrap_or_default()
                    }
                };
                let old_value = set_bit_in_bitmap(&mut bitmap, offset, value);
                updated.insert(key.clone(), bitmap);
                // Remove from removed cache if present
                removed.remove(&key);
                Some(old_value)
            }
        } else {
            // No snapshot: directly modify base via write lock
            let mut base = self.base.write();
            let bitmap = base.entry(key).or_default();
            Some(set_bit_in_bitmap(bitmap, offset, value))
        }
    }

    /// Get bit at offset (read operation, no copy)
    pub fn getbit(&self, key: &[u8], offset: usize) -> Option<bool> {
        if self.is_cow_mode() {
            // Check COW cache first (updated takes precedence over removed)
            if let Some(ref updated) = self.bitmaps_updated {
                if let Some(bitmap) = updated.get(key) {
                    return Some(get_bit_from_bitmap(bitmap, offset));
                }
            }

            // Check if removed (only if not in updated)
            if let Some(ref removed) = self.bitmaps_removed {
                if removed.contains(key) {
                    return None;
                }
            }
            // If not in updated and not removed, fall through to base
        }

        // Fall back to base (read lock)
        let base = self.base.read();
        base.get(key).map(|bitmap| get_bit_from_bitmap(bitmap, offset))
    }

    /// Count set bits in bitmap (read operation, no copy)
    pub fn bitcount(&self, key: &[u8]) -> Option<usize> {
        if self.is_cow_mode() {
            // Check COW cache first (updated takes precedence over removed)
            if let Some(ref updated) = self.bitmaps_updated {
                if let Some(bitmap) = updated.get(key) {
                    return Some(count_bits_in_bitmap(bitmap));
                }
            }

            // Check if removed (only if not in updated)
            if let Some(ref removed) = self.bitmaps_removed {
                if removed.contains(key) {
                    return None;
                }
            }
            // If not in updated and not removed, fall through to base
        }

        // Fall back to base (read lock)
        let base = self.base.read();
        base.get(key).map(|bitmap| count_bits_in_bitmap(bitmap))
    }

    /// Count set bits in range [start, end] (read operation, no copy)
    pub fn bitcount_range(&self, key: &[u8], start: usize, end: usize) -> Option<usize> {
        if self.is_cow_mode() {
            // Check COW cache first (updated takes precedence over removed)
            if let Some(ref updated) = self.bitmaps_updated {
                if let Some(bitmap) = updated.get(key) {
                    return Some(count_bits_in_range(bitmap, start, end));
                }
            }

            // Check if removed (only if not in updated)
            if let Some(ref removed) = self.bitmaps_removed {
                if removed.contains(key) {
                    return None;
                }
            }
            // If not in updated and not removed, fall through to base
        }

        // Fall back to base (read lock)
        let base = self.base.read();
        base.get(key).map(|bitmap| count_bits_in_range(bitmap, start, end))
    }

    /// Find first set bit (read operation, no copy)
    pub fn bitpos(&self, key: &[u8], bit: bool, start: Option<usize>, end: Option<usize>) -> Option<usize> {
        if self.is_cow_mode() {
            // Check COW cache first (updated takes precedence over removed)
            if let Some(ref updated) = self.bitmaps_updated {
                if let Some(bitmap) = updated.get(key) {
                    return find_bit_in_bitmap(bitmap, bit, start, end);
                }
            }

            // Check if removed (only if not in updated)
            if let Some(ref removed) = self.bitmaps_removed {
                if removed.contains(key) {
                    return None;
                }
            }
            // If not in updated and not removed, fall through to base
        }

        // Fall back to base (read lock)
        let base = self.base.read();
        base.get(key).and_then(|bitmap| find_bit_in_bitmap(bitmap, bit, start, end))
    }

    /// Perform bitwise operation (AND, OR, XOR, NOT)
    ///
    /// Operations:
    /// - "AND": result = dest & src1 & src2 & ...
    /// - "OR": result = dest | src1 | src2 | ...
    /// - "XOR": result = dest ^ src1 ^ src2 ^ ...
    /// - "NOT": result = !dest (only uses dest, ignores sources)
    pub fn bitop(
        &mut self,
        op: &str,
        dest_key: Vec<u8>,
        src_keys: Vec<Vec<u8>>,
    ) -> Option<usize> {
        if self.is_cow_mode() {
            let updated = self.bitmaps_updated.as_mut().unwrap();
            let removed = self.bitmaps_removed.as_mut().unwrap();

            // Get source bitmaps (from COW cache or base)
            let mut src_bitmaps = Vec::new();
            for src_key in &src_keys {
                if let Some(bitmap) = updated.get(src_key) {
                    src_bitmaps.push(bitmap.clone());
                } else if !removed.contains(src_key) {
                    let base = self.base.read();
                    if let Some(bitmap) = base.get(src_key) {
                        src_bitmaps.push(bitmap.clone());
                    }
                    drop(base);
                }
            }

            // Perform operation
            let result = match op {
                "AND" => bitop_and(&src_bitmaps),
                "OR" => bitop_or(&src_bitmaps),
                "XOR" => bitop_xor(&src_bitmaps),
                "NOT" => {
                    // NOT only uses first source (dest_key)
                    if src_keys.is_empty() {
                        return None;
                    }
                    let dest_bitmap = if let Some(bitmap) = updated.get(&src_keys[0]) {
                        bitmap.clone()
                    } else if !removed.contains(&src_keys[0]) {
                        let base = self.base.read();
                        base.get(&src_keys[0]).cloned().unwrap_or_default()
                    } else {
                        Vec::new()
                    };
                    bitop_not(&dest_bitmap)
                }
                _ => return None,
            };

            // Store result in COW cache
            let result_len = result.len();
            updated.insert(dest_key.clone(), result);
            removed.remove(&dest_key);
            Some(result_len)
        } else {
            // No snapshot: directly modify base via write lock
            let mut base = self.base.write();

            // Get source bitmaps
            let mut src_bitmaps = Vec::new();
            for src_key in &src_keys {
                if let Some(bitmap) = base.get(src_key) {
                    src_bitmaps.push(bitmap.clone());
                }
            }

            // Perform operation
            let result = match op {
                "AND" => bitop_and(&src_bitmaps),
                "OR" => bitop_or(&src_bitmaps),
                "XOR" => bitop_xor(&src_bitmaps),
                "NOT" => {
                    if src_keys.is_empty() {
                        return None;
                    }
                    let dest_bitmap = base.get(&src_keys[0]).cloned().unwrap_or_default();
                    bitop_not(&dest_bitmap)
                }
                _ => return None,
            };

            let result_len = result.len();
            base.insert(dest_key, result);
            Some(result_len)
        }
    }

    /// Clear bitmap for key (incremental COW: only records change)
    pub fn clear(&mut self, key: &[u8]) -> bool {
        if self.is_cow_mode() {
            let updated = self.bitmaps_updated.as_mut().unwrap();
            let removed = self.bitmaps_removed.as_mut().unwrap();

            // Check if already removed
            if removed.contains(key) {
                return true; // Already removed
            }

            // Check if exists in updated or base
            let exists = updated.contains_key(key) || {
                let base = self.base.read();
                base.contains_key(key)
            };

            if exists {
                // Mark as removed
                removed.insert(key.to_vec());
                updated.remove(key); // Remove from updated if present
                true
            } else {
                false
            }
        } else {
            // No snapshot: directly modify base via write lock
            let mut base = self.base.write();
            base.remove(key).is_some()
        }
    }

    /// Remove bitmap for key (incremental COW: only records change)
    pub fn remove(&mut self, key: &[u8]) -> bool {
        self.clear(key)
    }

    /// Get all keys (read operation)
    pub fn keys(&self) -> Vec<Vec<u8>> {
        let mut keys = HashSet::new();

        if self.is_cow_mode() {
            // Add keys from base (excluding removed)
            let base = self.base.read();
            if let Some(ref removed) = self.bitmaps_removed {
                for key in base.keys() {
                    if !removed.contains(key) {
                        keys.insert(key.clone());
                    }
                }
            } else {
                for key in base.keys() {
                    keys.insert(key.clone());
                }
            }
            drop(base);

            // Add keys from COW cache (updated takes precedence)
            if let Some(ref updated) = self.bitmaps_updated {
                for key in updated.keys() {
                    keys.insert(key.clone());
                }
            }
        } else {
            // Fall back to base (read lock)
            let base = self.base.read();
            keys = base.keys().cloned().collect();
        }

        keys.into_iter().collect()
    }

    /// Get key count (read operation)
    pub fn key_count(&self) -> usize {
        self.keys().len()
    }

    /// Get reference count (for testing)
    pub fn ref_count(&self) -> usize {
        Arc::strong_count(&self.base)
    }

    /// Check if in COW mode (for testing)
    pub fn is_in_cow_mode(&self) -> bool {
        self.is_cow_mode()
    }
}

impl Default for BitmapStoreCow {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Set bit at offset in bitmap (expands bitmap if needed)
fn set_bit_in_bitmap(bitmap: &mut BitmapData, offset: usize, value: bool) -> bool {
    let byte_index = offset / 8;
    let bit_index = offset % 8;

    // Expand bitmap if needed
    if byte_index >= bitmap.len() {
        bitmap.resize(byte_index + 1, 0);
    }

    let byte = &mut bitmap[byte_index];
    let mask = 1 << (7 - bit_index); // MSB first (Redis convention)
    let old_value = (*byte & mask) != 0;

    if value {
        *byte |= mask;
    } else {
        *byte &= !mask;
    }

    old_value
}

/// Get bit at offset from bitmap
fn get_bit_from_bitmap(bitmap: &BitmapData, offset: usize) -> bool {
    let byte_index = offset / 8;
    let bit_index = offset % 8;

    if byte_index >= bitmap.len() {
        return false;
    }

    let byte = bitmap[byte_index];
    let mask = 1 << (7 - bit_index); // MSB first (Redis convention)
    (byte & mask) != 0
}

/// Count set bits in bitmap
fn count_bits_in_bitmap(bitmap: &BitmapData) -> usize {
    bitmap.iter().map(|&byte| byte.count_ones() as usize).sum()
}

/// Count set bits in range [start, end] (bit offsets)
fn count_bits_in_range(bitmap: &BitmapData, start: usize, end: usize) -> usize {
    let start_byte = start / 8;
    let start_bit = start % 8;
    let end_byte = end / 8;
    let end_bit = end % 8;

    if start_byte >= bitmap.len() {
        return 0;
    }

    let end_byte = end_byte.min(bitmap.len() - 1);

    let mut count = 0;

    // First byte (partial)
    if start_byte == end_byte {
        // Same byte: count bits from start_bit to end_bit (inclusive)
        let byte = bitmap[start_byte];
        for bit_idx in start_bit..=end_bit.min(7) {
            let mask = 1 << (7 - bit_idx);
            if (byte & mask) != 0 {
                count += 1;
            }
        }
    } else {
        // First byte: count bits from start_bit to end of byte
        let byte = bitmap[start_byte];
        for bit_idx in start_bit..8 {
            let mask = 1 << (7 - bit_idx);
            if (byte & mask) != 0 {
                count += 1;
            }
        }

        // Middle bytes
        for byte in &bitmap[start_byte + 1..end_byte] {
            count += byte.count_ones() as usize;
        }

        // Last byte: count bits from start of byte to end_bit
        if end_byte < bitmap.len() {
            let byte = bitmap[end_byte];
            for bit_idx in 0..=end_bit.min(7) {
                let mask = 1 << (7 - bit_idx);
                if (byte & mask) != 0 {
                    count += 1;
                }
            }
        }
    }

    count
}

/// Find first bit (set or clear) in bitmap
fn find_bit_in_bitmap(
    bitmap: &BitmapData,
    bit: bool,
    start: Option<usize>,
    end: Option<usize>,
) -> Option<usize> {
    let start = start.unwrap_or(0);
    let end = end.unwrap_or_else(|| bitmap.len() * 8 - 1);

    let start_byte = start / 8;
    let start_bit = start % 8;
    let end_byte = (end / 8).min(bitmap.len().saturating_sub(1));
    let end_bit = end % 8;

    if start_byte >= bitmap.len() {
        return if bit { None } else { Some(start) };
    }

    // Search from start
    for byte_idx in start_byte..=end_byte {
        let byte = bitmap[byte_idx];
        let bit_start = if byte_idx == start_byte { start_bit } else { 0 };
        let bit_end = if byte_idx == end_byte { end_bit } else { 7 };

        for bit_idx in bit_start..=bit_end {
            let offset = byte_idx * 8 + bit_idx;
            let mask = 1 << (7 - bit_idx);
            let bit_value = (byte & mask) != 0;

            if bit_value == bit {
                return Some(offset);
            }
        }
    }

    None
}

/// Bitwise AND operation
fn bitop_and(bitmaps: &[BitmapData]) -> BitmapData {
    if bitmaps.is_empty() {
        return Vec::new();
    }

    let max_len = bitmaps.iter().map(|b| b.len()).max().unwrap_or(0);
    if max_len == 0 {
        return Vec::new();
    }

    // Initialize result with first bitmap (or all 1s if empty)
    let mut result = if let Some(first) = bitmaps.first() {
        first.clone()
    } else {
        vec![0xFF; max_len]
    };

    // Extend result to max_len if needed
    if result.len() < max_len {
        result.resize(max_len, 0);
    }

    // AND with remaining bitmaps
    for bitmap in bitmaps.iter().skip(1) {
        for (i, &byte) in bitmap.iter().enumerate() {
            if i < result.len() {
                result[i] &= byte;
            }
        }
        // For bytes beyond bitmap length, AND with 0 (clears those bits)
        for i in bitmap.len()..result.len() {
            result[i] = 0;
        }
    }

    result
}

/// Bitwise OR operation
fn bitop_or(bitmaps: &[BitmapData]) -> BitmapData {
    if bitmaps.is_empty() {
        return Vec::new();
    }

    let max_len = bitmaps.iter().map(|b| b.len()).max().unwrap_or(0);
    let mut result = vec![0u8; max_len];

    for bitmap in bitmaps {
        for (i, &byte) in bitmap.iter().enumerate() {
            if i < result.len() {
                result[i] |= byte;
            }
        }
    }

    result
}

/// Bitwise XOR operation
fn bitop_xor(bitmaps: &[BitmapData]) -> BitmapData {
    if bitmaps.is_empty() {
        return Vec::new();
    }

    let max_len = bitmaps.iter().map(|b| b.len()).max().unwrap_or(0);
    let mut result = vec![0u8; max_len];

    for bitmap in bitmaps {
        for (i, &byte) in bitmap.iter().enumerate() {
            if i < result.len() {
                result[i] ^= byte;
            }
        }
    }

    result
}

/// Bitwise NOT operation
fn bitop_not(bitmap: &BitmapData) -> BitmapData {
    bitmap.iter().map(|&byte| !byte).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap_store_basic_operations() {
        let mut store = BitmapStoreCow::new();

        // Set bits
        assert_eq!(store.setbit(b"bitmap1".to_vec(), 0, true), Some(false));
        assert_eq!(store.setbit(b"bitmap1".to_vec(), 1, true), Some(false));
        assert_eq!(store.setbit(b"bitmap1".to_vec(), 8, true), Some(false));

        // Get bits
        assert_eq!(store.getbit(b"bitmap1", 0), Some(true));
        assert_eq!(store.getbit(b"bitmap1", 1), Some(true));
        assert_eq!(store.getbit(b"bitmap1", 2), Some(false));
        assert_eq!(store.getbit(b"bitmap1", 8), Some(true));

        // Update bit
        assert_eq!(store.setbit(b"bitmap1".to_vec(), 0, false), Some(true));
        assert_eq!(store.getbit(b"bitmap1", 0), Some(false));

        // Bit count
        assert_eq!(store.bitcount(b"bitmap1"), Some(2));
    }

    #[test]
    fn test_bitmap_store_snapshot_no_copy() {
        let mut store = BitmapStoreCow::new();
        store.setbit(b"bitmap1".to_vec(), 0, true);
        store.setbit(b"bitmap1".to_vec(), 1, true);
        store.setbit(b"bitmap1".to_vec(), 2, true);

        // Before snapshot: ref count should be 1
        assert_eq!(store.ref_count(), 1);

        // Create snapshot (only increases ref count, no copy)
        let snapshot = store.make_snapshot();

        // After snapshot: ref count should be 2
        assert_eq!(store.ref_count(), 2);
        assert_eq!(Arc::strong_count(&snapshot), 2);

        // Snapshot should have same data
        let snapshot_data = snapshot.read();
        assert_eq!(snapshot_data.len(), 1);
        assert_eq!(
            snapshot_data.get(b"bitmap1".as_slice()).unwrap().len(),
            1
        );
        drop(snapshot_data);

        // Original should still work
        assert_eq!(store.bitcount(b"bitmap1"), Some(3));
        assert_eq!(store.getbit(b"bitmap1", 0), Some(true));
    }

    #[test]
    fn test_bitmap_store_write_after_snapshot_no_copy() {
        let mut store = BitmapStoreCow::new();
        store.setbit(b"bitmap1".to_vec(), 0, true);
        store.setbit(b"bitmap1".to_vec(), 1, true);

        // Create snapshot
        let snapshot = store.make_snapshot();
        assert_eq!(store.ref_count(), 2);
        assert!(store.is_in_cow_mode());

        // Write operation records change in COW cache (NO data copy)
        store.setbit(b"bitmap1".to_vec(), 2, true);
        assert_eq!(store.ref_count(), 2); // Ref count unchanged (no copy!)
        assert_eq!(Arc::strong_count(&snapshot), 2);
        assert!(store.is_in_cow_mode());

        // Original should have new data (via COW cache)
        assert_eq!(store.bitcount(b"bitmap1"), Some(3));
        assert_eq!(store.getbit(b"bitmap1", 2), Some(true));

        // Snapshot should have old data (unchanged, from base)
        let snapshot_data = snapshot.read();
        let base_bitmap = snapshot_data.get(b"bitmap1".as_slice()).unwrap();
        assert_eq!(count_bits_in_bitmap(base_bitmap), 2);
        assert!(!get_bit_from_bitmap(base_bitmap, 2));
        drop(snapshot_data);
    }

    #[test]
    fn test_bitmap_store_write_without_snapshot_no_copy() {
        let mut store = BitmapStoreCow::new();
        store.setbit(b"bitmap1".to_vec(), 0, true);

        // No snapshot, ref count is 1
        assert_eq!(store.ref_count(), 1);

        // Write operation should NOT copy (ref count stays 1)
        store.setbit(b"bitmap1".to_vec(), 1, true);
        assert_eq!(store.ref_count(), 1); // No copy happened

        // Another write
        store.setbit(b"bitmap1".to_vec(), 2, true);
        assert_eq!(store.ref_count(), 1); // Still no copy
    }

    #[test]
    fn test_bitmap_store_merge_applies_only_changes() {
        let mut store = BitmapStoreCow::new();
        store.setbit(b"bitmap1".to_vec(), 0, true);
        store.setbit(b"bitmap1".to_vec(), 1, true);
        store.setbit(b"bitmap1".to_vec(), 2, true);

        // Create snapshot
        let snapshot = store.make_snapshot();
        assert_eq!(store.ref_count(), 2);

        // Make changes (only 2 operations, not full copy)
        store.setbit(b"bitmap1".to_vec(), 3, true); // Add new bit
        store.setbit(b"bitmap1".to_vec(), 0, false); // Clear existing bit

        // Before merge: changes are in COW cache
        assert!(store.is_in_cow_mode());
        assert_eq!(store.bitcount(b"bitmap1"), Some(3)); // bits 1, 2, 3
        assert_eq!(store.getbit(b"bitmap1", 0), Some(false));
        assert_eq!(store.getbit(b"bitmap1", 3), Some(true));

        // Snapshot still has old data
        let snapshot_data = snapshot.read();
        let base_bitmap = snapshot_data.get(b"bitmap1".as_slice()).unwrap();
        assert_eq!(count_bits_in_bitmap(base_bitmap), 3);
        assert!(get_bit_from_bitmap(base_bitmap, 0));
        assert!(!get_bit_from_bitmap(base_bitmap, 3));
        drop(snapshot_data);

        // Merge: applies only changes to base (O(M) where M=1, not O(N))
        store.merge_cow();
        assert!(!store.is_in_cow_mode());
        // Ref count is still 2 because snapshot still exists (this is correct)
        assert_eq!(store.ref_count(), 2);

        // After merge: changes are in base
        assert_eq!(store.bitcount(b"bitmap1"), Some(3));
        assert_eq!(store.getbit(b"bitmap1", 0), Some(false));
        assert_eq!(store.getbit(b"bitmap1", 1), Some(true));
        assert_eq!(store.getbit(b"bitmap1", 2), Some(true));
        assert_eq!(store.getbit(b"bitmap1", 3), Some(true));
    }

    #[test]
    fn test_bitmap_store_multiple_keys() {
        let mut store = BitmapStoreCow::new();
        store.setbit(b"bitmap1".to_vec(), 0, true);
        store.setbit(b"bitmap2".to_vec(), 0, true);
        store.setbit(b"bitmap2".to_vec(), 1, true);

        assert_eq!(store.key_count(), 2);
        assert_eq!(store.bitcount(b"bitmap1"), Some(1));
        assert_eq!(store.bitcount(b"bitmap2"), Some(2));

        // Create snapshot
        let _snapshot = store.make_snapshot();

        // Modify only bitmap1
        store.setbit(b"bitmap1".to_vec(), 1, true);

        // bitmap1 should be updated
        assert_eq!(store.bitcount(b"bitmap1"), Some(2));
        // bitmap2 should be unchanged (from base)
        assert_eq!(store.bitcount(b"bitmap2"), Some(2));
    }

    #[test]
    fn test_bitmap_store_updated_overrides_removed() {
        let mut store = BitmapStoreCow::new();
        store.setbit(b"bitmap1".to_vec(), 0, true);
        store.setbit(b"bitmap1".to_vec(), 1, true);

        // Create snapshot
        let _snapshot = store.make_snapshot();

        // Remove bitmap1 (adds to removed cache)
        assert!(store.clear(b"bitmap1"));
        assert!(!store.contains_key(b"bitmap1"));
        assert_eq!(store.len(b"bitmap1"), None);

        // Re-add bitmap1 (adds to updated cache, should override removed)
        store.setbit(b"bitmap1".to_vec(), 2, true);
        assert!(store.contains_key(b"bitmap1")); // Should exist (updated overrides removed)
        assert_eq!(store.len(b"bitmap1"), Some(1));
        assert_eq!(store.getbit(b"bitmap1", 2), Some(true));

        // Merge: updated should take precedence
        store.merge_cow();
        assert!(store.contains_key(b"bitmap1")); // Should still exist after merge
        assert_eq!(store.len(b"bitmap1"), Some(1));
        assert_eq!(store.getbit(b"bitmap1", 2), Some(true));
    }

    #[test]
    fn test_bitmap_store_bitop() {
        let mut store = BitmapStoreCow::new();

        // Create two bitmaps
        store.setbit(b"bitmap1".to_vec(), 0, true);
        store.setbit(b"bitmap1".to_vec(), 1, true);
        store.setbit(b"bitmap2".to_vec(), 1, true);
        store.setbit(b"bitmap2".to_vec(), 2, true);

        // AND operation
        let result_len = store.bitop("AND", b"result".to_vec(), vec![b"bitmap1".to_vec(), b"bitmap2".to_vec()]);
        assert_eq!(result_len, Some(1));
        assert_eq!(store.getbit(b"result", 0), Some(false));
        assert_eq!(store.getbit(b"result", 1), Some(true)); // Both have bit 1
        assert_eq!(store.getbit(b"result", 2), Some(false));

        // OR operation
        let result_len = store.bitop("OR", b"result2".to_vec(), vec![b"bitmap1".to_vec(), b"bitmap2".to_vec()]);
        assert_eq!(result_len, Some(1));
        assert_eq!(store.getbit(b"result2", 0), Some(true));
        assert_eq!(store.getbit(b"result2", 1), Some(true));
        assert_eq!(store.getbit(b"result2", 2), Some(true));

        // XOR operation
        let result_len = store.bitop("XOR", b"result3".to_vec(), vec![b"bitmap1".to_vec(), b"bitmap2".to_vec()]);
        assert_eq!(result_len, Some(1));
        assert_eq!(store.getbit(b"result3", 0), Some(true)); // Only bitmap1
        assert_eq!(store.getbit(b"result3", 1), Some(false)); // Both have it
        assert_eq!(store.getbit(b"result3", 2), Some(true)); // Only bitmap2

        // NOT operation
        let result_len = store.bitop("NOT", b"result4".to_vec(), vec![b"bitmap1".to_vec()]);
        assert_eq!(result_len, Some(1));
        // NOT of bitmap1 (bits 0,1 set) should have all other bits set
        assert_eq!(store.getbit(b"result4", 0), Some(false));
        assert_eq!(store.getbit(b"result4", 1), Some(false));
        assert_eq!(store.getbit(b"result4", 2), Some(true));
    }

    #[test]
    fn test_bitmap_store_bitcount_range() {
        let mut store = BitmapStoreCow::new();
        store.setbit(b"bitmap1".to_vec(), 0, true);
        store.setbit(b"bitmap1".to_vec(), 1, true);
        store.setbit(b"bitmap1".to_vec(), 8, true);
        store.setbit(b"bitmap1".to_vec(), 9, true);
        store.setbit(b"bitmap1".to_vec(), 10, true);

        // Count bits in range [0, 7] (first byte)
        assert_eq!(store.bitcount_range(b"bitmap1", 0, 7), Some(2));

        // Count bits in range [8, 15] (second byte)
        assert_eq!(store.bitcount_range(b"bitmap1", 8, 15), Some(3));

        // Count bits in range [0, 15] (both bytes)
        assert_eq!(store.bitcount_range(b"bitmap1", 0, 15), Some(5));
    }

    #[test]
    fn test_bitmap_store_bitpos() {
        let mut store = BitmapStoreCow::new();
        store.setbit(b"bitmap1".to_vec(), 2, true);
        store.setbit(b"bitmap1".to_vec(), 5, true);
        store.setbit(b"bitmap1".to_vec(), 10, true);

        // Find first set bit
        assert_eq!(store.bitpos(b"bitmap1", true, None, None), Some(2));

        // Find first set bit from offset 3
        assert_eq!(store.bitpos(b"bitmap1", true, Some(3), None), Some(5));

        // Find first clear bit
        assert_eq!(store.bitpos(b"bitmap1", false, None, None), Some(0));

        // Find first clear bit from offset 3
        assert_eq!(store.bitpos(b"bitmap1", false, Some(3), None), Some(3));
    }

    #[test]
    fn test_bitmap_store_keys() {
        let mut store = BitmapStoreCow::new();
        store.setbit(b"bitmap1".to_vec(), 0, true);
        store.setbit(b"bitmap2".to_vec(), 0, true);
        store.setbit(b"bitmap3".to_vec(), 0, true);

        let keys = store.keys();
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&b"bitmap1".to_vec()));
        assert!(keys.contains(&b"bitmap2".to_vec()));
        assert!(keys.contains(&b"bitmap3".to_vec()));

        // Create snapshot and remove bitmap2
        let _snapshot = store.make_snapshot();
        assert!(store.clear(b"bitmap2"));

        // Keys should exclude removed bitmap2
        let keys = store.keys();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&b"bitmap1".to_vec()));
        assert!(!keys.contains(&b"bitmap2".to_vec()));
        assert!(keys.contains(&b"bitmap3".to_vec()));
    }

    #[test]
    fn test_bitmap_store_clear() {
        let mut store = BitmapStoreCow::new();
        store.setbit(b"bitmap1".to_vec(), 0, true);
        store.setbit(b"bitmap1".to_vec(), 1, true);

        // Clear bitmap1
        assert!(store.clear(b"bitmap1"));
        assert!(!store.contains_key(b"bitmap1"));
        assert_eq!(store.len(b"bitmap1"), None);
        assert_eq!(store.getbit(b"bitmap1", 0), None);

        // Clear non-existent bitmap
        assert!(!store.clear(b"nonexistent"));
    }

    #[test]
    fn test_bitmap_store_from_data() {
        let mut data = HashMap::new();
        let mut bitmap1 = Vec::new();
        set_bit_in_bitmap(&mut bitmap1, 0, true);
        set_bit_in_bitmap(&mut bitmap1, 1, true);
        data.insert(b"bitmap1".to_vec(), bitmap1);

        let store = BitmapStoreCow::from_data(data);
        assert_eq!(store.key_count(), 1);
        assert_eq!(store.len(b"bitmap1"), Some(1));
        assert_eq!(store.getbit(b"bitmap1", 0), Some(true));
        assert_eq!(store.getbit(b"bitmap1", 1), Some(true));
    }

    #[test]
    fn test_bitmap_store_multiple_writes_same_bitmap() {
        let mut store = BitmapStoreCow::new();
        store.setbit(b"bitmap1".to_vec(), 0, true);

        // Create snapshot
        let _snapshot = store.make_snapshot();

        // Multiple writes to same bitmap (should reuse COW instance)
        store.setbit(b"bitmap1".to_vec(), 1, true);
        store.setbit(b"bitmap1".to_vec(), 2, true);
        store.setbit(b"bitmap1".to_vec(), 0, false); // Clear bit 0

        assert_eq!(store.bitcount(b"bitmap1"), Some(2));
        assert_eq!(store.getbit(b"bitmap1", 0), Some(false));
        assert_eq!(store.getbit(b"bitmap1", 1), Some(true));
        assert_eq!(store.getbit(b"bitmap1", 2), Some(true));

        // Ref count should still be 2 (no full copy!)
        assert_eq!(store.ref_count(), 2);
    }

    #[test]
    fn test_bitmap_expansion() {
        let mut store = BitmapStoreCow::new();

        // Set bit at large offset (should expand bitmap)
        store.setbit(b"bitmap1".to_vec(), 100, true);
        assert_eq!(store.len(b"bitmap1"), Some(13)); // 100 / 8 + 1 = 13 bytes
        assert_eq!(store.getbit(b"bitmap1", 100), Some(true));
        assert_eq!(store.getbit(b"bitmap1", 0), Some(false));
    }

    // ========== Additional COW Tests (similar to set.rs patterns) ==========

    #[test]
    fn test_bitmap_data_cow_clear_in_cow_mode() {
        let mut store = BitmapStoreCow::new();
        
        // Add initial data
        store.setbit(b"bitmap1".to_vec(), 0, true);
        store.setbit(b"bitmap1".to_vec(), 1, true);
        store.setbit(b"bitmap2".to_vec(), 5, true);
        
        // Create snapshot
        store.make_snapshot();
        assert!(store.is_cow_mode());
        
        // Clear bitmaps in COW mode
        assert!(store.clear(b"bitmap1"));
        assert!(store.clear(b"bitmap2"));
        
        // Verify cleared state
        assert!(!store.contains_key(b"bitmap1"));
        assert!(!store.contains_key(b"bitmap2"));
        assert_eq!(store.len(b"bitmap1"), None);
        assert_eq!(store.len(b"bitmap2"), None);
        
        // Verify keys() returns empty
        let keys = store.keys();
        assert!(keys.is_empty());
    }

    #[test]
    fn test_bitmap_data_cow_multiple_snapshots() {
        use std::sync::Arc;
        use std::collections::{HashMap, HashSet};
        
        let mut store1 = BitmapStoreCow::new();
        
        // Add data to store1
        store1.setbit(b"bitmap1".to_vec(), 0, true);
        store1.setbit(b"bitmap1".to_vec(), 1, true);
        store1.setbit(b"bitmap2".to_vec(), 5, true);
        
        // Create snapshot and get reference
        let _snapshot = store1.make_snapshot();
        assert_eq!(store1.ref_count(), 2);
        
        // Create store2 from same base (simulating multiple snapshots)
        let mut store2 = BitmapStoreCow {
            base: Arc::clone(&store1.base),
            bitmaps_updated: Some(HashMap::new()),
            bitmaps_removed: Some(HashSet::new()),
        };
        
        // Both should share the same base
        assert_eq!(store1.ref_count(), 3); // store1.base + snapshot + store2
        assert!(store2.is_cow_mode());
        
        // Make independent changes
        store1.setbit(b"bitmap1".to_vec(), 2, true); // Add to bitmap1
        store2.setbit(b"bitmap2".to_vec(), 6, true); // Add to bitmap2
        store2.clear(b"bitmap1"); // Remove bitmap1 in store2
        
        // Verify independent states
        assert_eq!(store1.bitcount(b"bitmap1"), Some(3)); // bits 0, 1, 2
        assert_eq!(store1.bitcount(b"bitmap2"), Some(1)); // bit 5
        assert!(store1.contains_key(b"bitmap1"));
        
        assert_eq!(store2.bitcount(b"bitmap1"), None); // cleared
        assert_eq!(store2.bitcount(b"bitmap2"), Some(2)); // bits 5, 6
        assert!(!store2.contains_key(b"bitmap1"));
    }

    #[test]
    fn test_bitmap_data_cow_edge_cases() {
        let mut store = BitmapStoreCow::new();
        
        // Test empty key
        store.setbit(vec![], 0, true);
        assert!(store.contains_key(&[]));
        assert_eq!(store.len(&[]), Some(1));
        assert_eq!(store.getbit(&[], 0), Some(true));
        
        // Test large bitmap
        let large_offset = 1024 * 1024; // 1MB worth of bits
        store.setbit(b"large".to_vec(), large_offset, true);
        assert!(store.contains_key(b"large"));
        assert_eq!(store.len(b"large"), Some(131073)); // 1MB/8 + 1 bytes
        assert_eq!(store.getbit(b"large", large_offset), Some(true));
        
        // Test binary data in key
        let binary_key = vec![0, 1, 2, 255, 254, 253];
        store.setbit(binary_key.clone(), 10, true);
        assert!(store.contains_key(&binary_key));
        assert_eq!(store.getbit(&binary_key, 10), Some(true));
        
        // Test with snapshot
        store.make_snapshot();
        store.setbit(binary_key.clone(), 11, true);
        assert_eq!(store.bitcount(&binary_key), Some(2));
    }

    #[test]
    fn test_bitmap_data_cow_concurrent_read_write() {
        use std::sync::Arc;
        use std::thread;
        use parking_lot::RwLock;
        
        let mut store = BitmapStoreCow::new();
        
        // Add initial data
        for i in 0..100 {
            store.setbit(b"shared".to_vec(), i * 8, true);
        }
        
        // Create snapshot
        store.make_snapshot();
        let store_arc = Arc::new(RwLock::new(store));
        
        // Spawn reader threads
        let mut handles = vec![];
        
        for _ in 0..3 {
            let store_clone = Arc::clone(&store_arc);
            let handle = thread::spawn(move || {
                for j in 0..20 {
                    let store = store_clone.read();
                    let _len = store.len(b"shared");
                    let _bit = store.getbit(b"shared", j * 8);
                    let _contains = store.contains_key(b"shared");
                    let _count = store.bitcount(b"shared");
                }
            });
            handles.push(handle);
        }
        
        // Spawn writer thread
        let store_clone = Arc::clone(&store_arc);
        let writer_handle = thread::spawn(move || {
            let mut store = store_clone.write();
            for i in 100..110 {
                store.setbit(b"shared".to_vec(), i * 8, true);
            }
        });
        
        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }
        writer_handle.join().unwrap();
        
        // Verify final state
        let store = store_arc.read();
        assert_eq!(store.bitcount(b"shared"), Some(110));
    }

    #[test]
    fn test_bitmap_data_cow_multiple_merges() {
        let mut store = BitmapStoreCow::new();
        
        // First cycle: add data and merge
        store.setbit(b"bitmap1".to_vec(), 0, true);
        store.make_snapshot();
        store.setbit(b"bitmap1".to_vec(), 1, true);
        store.merge_cow();
        
        // Second cycle: modify and merge again
        store.make_snapshot();
        store.setbit(b"bitmap1".to_vec(), 2, true);
        store.clear(b"bitmap1");
        store.merge_cow();
        
        // Verify final state
        assert_eq!(store.bitcount(b"bitmap1"), None); // cleared
        assert!(!store.contains_key(b"bitmap1"));
    }

    #[test]
    fn test_bitmap_store_cow_from_data() {
        use std::collections::HashMap;
        
        let mut data = HashMap::new();
        let mut bitmap1 = Vec::new();
        // Manually set bits in bitmap
        let byte_index = 0 / 8;
        let bit_index = 0 % 8;
        if byte_index >= bitmap1.len() {
            bitmap1.resize(byte_index + 1, 0);
        }
        bitmap1[byte_index] |= 1 << (7 - bit_index); // Set bit 0
        
        let byte_index = 7 / 8;
        let bit_index = 7 % 8;
        if byte_index >= bitmap1.len() {
            bitmap1.resize(byte_index + 1, 0);
        }
        bitmap1[byte_index] |= 1 << (7 - bit_index); // Set bit 7
        
        data.insert(b"bitmap1".to_vec(), bitmap1);
        
        let mut store = BitmapStoreCow::from_data(data);
        
        // Verify initial data
        assert_eq!(store.bitcount(b"bitmap1"), Some(2));
        assert_eq!(store.getbit(b"bitmap1", 0), Some(true));
        assert_eq!(store.getbit(b"bitmap1", 7), Some(true));
        
        // Create snapshot and modify
        store.make_snapshot();
        store.setbit(b"bitmap1".to_vec(), 3, true);
        
        // Verify changes
        assert_eq!(store.bitcount(b"bitmap1"), Some(3));
        assert_eq!(store.getbit(b"bitmap1", 3), Some(true));
    }

    #[test]
    fn test_bitmap_store_cow_clear_operations() {
        let mut store = BitmapStoreCow::new();
        
        // Add some data
        store.setbit(b"bitmap1".to_vec(), 0, true);
        store.setbit(b"bitmap1".to_vec(), 1, true);
        store.setbit(b"bitmap2".to_vec(), 5, true);
        
        // Create snapshot
        store.make_snapshot();
        
        // Test clear operations
        assert!(store.clear(b"bitmap1"));
        assert!(!store.clear(b"nonexistent"));
        
        // Verify state
        assert!(!store.contains_key(b"bitmap1"));
        assert!(store.contains_key(b"bitmap2"));
        assert_eq!(store.len(b"bitmap1"), None);
        assert_eq!(store.len(b"bitmap2"), Some(1));
        
        // Test merge
        store.merge_cow();
        assert!(!store.contains_key(b"bitmap1"));
        assert!(store.contains_key(b"bitmap2"));
    }

    #[test]
    fn test_bitmap_store_cow_bitop_with_cow() {
        let mut store = BitmapStoreCow::new();
        
        // Create base bitmaps
        store.setbit(b"bitmap1".to_vec(), 0, true);
        store.setbit(b"bitmap1".to_vec(), 1, true);
        store.setbit(b"bitmap2".to_vec(), 1, true);
        store.setbit(b"bitmap2".to_vec(), 2, true);
        
        // Create snapshot
        store.make_snapshot();
        
        // Modify one bitmap in COW mode
        store.setbit(b"bitmap1".to_vec(), 3, true);
        
        // Perform bitop with COW-modified bitmap
        let result_len = store.bitop("AND", b"result".to_vec(), vec![b"bitmap1".to_vec(), b"bitmap2".to_vec()]);
        assert_eq!(result_len, Some(1));
        
        // Verify result uses COW-modified bitmap1
        assert_eq!(store.getbit(b"result", 0), Some(false));
        assert_eq!(store.getbit(b"result", 1), Some(true)); // Both have bit 1
        assert_eq!(store.getbit(b"result", 2), Some(false));
        assert_eq!(store.getbit(b"result", 3), Some(false)); // bitmap2 doesn't have bit 3
    }

    #[test]
    fn test_bitmap_store_cow_bitcount_with_cow() {
        let mut store = BitmapStoreCow::new();
        
        // Create base bitmap
        store.setbit(b"bitmap1".to_vec(), 0, true);
        store.setbit(b"bitmap1".to_vec(), 1, true);
        store.setbit(b"bitmap1".to_vec(), 8, true);
        
        // Create snapshot
        store.make_snapshot();
        
        // Modify bitmap in COW mode
        store.setbit(b"bitmap1".to_vec(), 2, true);
        store.setbit(b"bitmap1".to_vec(), 9, false); // Clear this bit (bit 9 = 1st bit of 2nd byte)
        
        // Verify bitcount uses COW-modified data
        assert_eq!(store.bitcount(b"bitmap1"), Some(4)); // bits 0, 1, 2, 8 (bit 9 cleared, bit 8 still set)
        assert_eq!(store.bitcount_range(b"bitmap1", 0, 7), Some(3)); // bits 0, 1, 2
        assert_eq!(store.bitcount_range(b"bitmap1", 8, 15), Some(1)); // bit 8 still set
    }
}

