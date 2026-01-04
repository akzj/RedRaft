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


/// Bitmap data structure (Vec<u8> where each byte contains 8 bits)
pub type BitmapData = Vec<u8>;

/// Bitmap Store with Incremental Copy-on-Write (COW) support
///
/// True incremental COW semantics:
/// - `make_snapshot()`: Only clones Arc (increases ref count), NO data copy
/// - `setbit()`/`bitop()`: Records changes in small COW cache (only changed bitmaps), NO full copy
/// - Read operations: Merges COW cache + base data (O(1) lookup)
/// - `merge_cow()`: Applies only changed bitmaps to base via RwLock (O(M) where M = changes, not total data)

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

