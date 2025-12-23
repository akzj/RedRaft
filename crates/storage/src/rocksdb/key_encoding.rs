//! Key encoding and decoding utilities for RocksDB storage
//!
//! Provides functions to encode/decode keys for different data types:
//! - String: `{slot(4字节)}:s:{key}`
//! - Hash field: `{slot(4字节)}:h:{key}:{field}`
//! - Hash metadata: `{slot(4字节)}:H:{key}`
//! - Apply index: `@:apply_index` (no slot prefix)
//!
//! Slot is encoded as 4-byte big-endian u32 at the beginning of each key.
//! This enables efficient slot-based range queries and data migration.

/// Key type prefixes
pub mod key_prefix {
    pub const STRING: u8 = b's';
    pub const HASH: u8 = b'h';
    pub const HASH_META: u8 = b'H';
    pub const APPLY_INDEX: u8 = b'@'; // Special prefix for apply_index
}

/// Slot prefix length (4 bytes for u32)
const SLOT_PREFIX_LEN: usize = 4;

/// Encode slot as 4-byte big-endian
fn encode_slot(slot: u32) -> [u8; 4] {
    slot.to_be_bytes()
}

/// Decode slot from 4-byte big-endian
fn decode_slot(bytes: &[u8]) -> Option<u32> {
    if bytes.len() < 4 {
        return None;
    }
    Some(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

/// Build apply_index key for shard (no slot prefix)
pub fn apply_index_key() -> Vec<u8> {
    vec![
        key_prefix::APPLY_INDEX,
        b':',
        b'a',
        b'p',
        b'p',
        b'l',
        b'y',
        b'_',
        b'i',
        b'n',
        b'd',
        b'e',
        b'x',
    ]
}

/// Build string key: `{slot(4字节)}:s:{key}`
pub fn string_key(slot: u32, key: &[u8]) -> Vec<u8> {
    let slot_bytes = encode_slot(slot);
    let mut result = Vec::with_capacity(SLOT_PREFIX_LEN + 2 + key.len() + 1);
    result.extend_from_slice(&slot_bytes);
    result.push(b':');
    result.push(key_prefix::STRING);
    result.push(b':');
    result.extend_from_slice(key);
    result
}

/// Build hash field key: `{slot(4字节)}:h:{key}:{field}`
pub fn hash_field_key(slot: u32, key: &[u8], field: &[u8]) -> Vec<u8> {
    let slot_bytes = encode_slot(slot);
    let mut result = Vec::with_capacity(SLOT_PREFIX_LEN + 3 + key.len() + field.len() + 1);
    result.extend_from_slice(&slot_bytes);
    result.push(b':');
    result.push(key_prefix::HASH);
    result.push(b':');
    result.extend_from_slice(key);
    result.push(b':');
    result.extend_from_slice(field);
    result
}

/// Build hash metadata key: `{slot(4字节)}:H:{key}`
pub fn hash_meta_key(slot: u32, key: &[u8]) -> Vec<u8> {
    let slot_bytes = encode_slot(slot);
    let mut result = Vec::with_capacity(SLOT_PREFIX_LEN + 2 + key.len() + 1);
    result.extend_from_slice(&slot_bytes);
    result.push(b':');
    result.push(key_prefix::HASH_META);
    result.push(b':');
    result.extend_from_slice(key);
    result
}

/// Build hash field prefix for iteration: `{slot(4字节)}:h:{key}:`
pub fn hash_field_prefix(slot: u32, key: &[u8]) -> Vec<u8> {
    let slot_bytes = encode_slot(slot);
    let mut result = Vec::with_capacity(SLOT_PREFIX_LEN + 3 + key.len() + 1);
    result.extend_from_slice(&slot_bytes);
    result.push(b':');
    result.push(key_prefix::HASH);
    result.push(b':');
    result.extend_from_slice(key);
    result.push(b':');
    result
}

/// Extract slot from encoded key
/// Returns (slot, remaining_key) if successful
pub fn extract_slot(encoded: &[u8]) -> Option<(u32, &[u8])> {
    if encoded.len() < SLOT_PREFIX_LEN + 1 {
        return None;
    }
    let slot = decode_slot(&encoded[0..SLOT_PREFIX_LEN])?;
    // Skip slot prefix and colon
    if encoded[SLOT_PREFIX_LEN] != b':' {
        return None;
    }
    Some((slot, &encoded[SLOT_PREFIX_LEN + 1..]))
}

/// Extract field from hash key (with slot prefix)
/// Format: `{slot}:h:{key}:{field}`
pub fn extract_hash_field<'a>(encoded: &'a [u8], slot: u32, key: &[u8]) -> Option<&'a [u8]> {
    // Expected format: {slot(4)}:h:{key}:{field}
    let slot_bytes = encode_slot(slot);
    let expected_prefix_len = SLOT_PREFIX_LEN + 1 + 1 + 1 + key.len() + 1; // slot + : + h + : + key + :
    
    if encoded.len() <= expected_prefix_len {
        return None;
    }
    
    // Verify slot matches
    if &encoded[0..SLOT_PREFIX_LEN] != slot_bytes.as_slice() {
        return None;
    }
    
    // Verify format: slot: + h: + key + :
    if encoded[SLOT_PREFIX_LEN] != b':'
        || encoded[SLOT_PREFIX_LEN + 1] != key_prefix::HASH
        || encoded[SLOT_PREFIX_LEN + 2] != b':'
        || &encoded[SLOT_PREFIX_LEN + 3..SLOT_PREFIX_LEN + 3 + key.len()] != key
        || encoded[SLOT_PREFIX_LEN + 3 + key.len()] != b':'
    {
        return None;
    }
    
    Some(&encoded[expected_prefix_len..])
}

/// Parse string key from encoded format
/// Returns (slot, original_key) if successful
pub fn parse_string_key(encoded: &[u8]) -> Option<(u32, &[u8])> {
    let (slot, rest) = extract_slot(encoded)?;
    if rest.len() < 2 || rest[0] != key_prefix::STRING || rest[1] != b':' {
        return None;
    }
    Some((slot, &rest[2..]))
}

/// Parse hash field key from encoded format
/// Returns (slot, hash_key, field) if successful
pub fn parse_hash_field_key(encoded: &[u8]) -> Option<(u32, &[u8], &[u8])> {
    let (slot, rest) = extract_slot(encoded)?;
    if rest.len() < 2 || rest[0] != key_prefix::HASH || rest[1] != b':' {
        return None;
    }
    let key_part = &rest[2..];
    if let Some(colon_pos) = key_part.iter().position(|&b| b == b':') {
        let hash_key = &key_part[..colon_pos];
        let field = &key_part[colon_pos + 1..];
        return Some((slot, hash_key, field));
    }
    None
}

/// Parse hash metadata key from encoded format
/// Returns (slot, hash_key) if successful
pub fn parse_hash_meta_key(encoded: &[u8]) -> Option<(u32, &[u8])> {
    let (slot, rest) = extract_slot(encoded)?;
    if rest.len() < 2 || rest[0] != key_prefix::HASH_META || rest[1] != b':' {
        return None;
    }
    Some((slot, &rest[2..]))
}

/// Build slot range prefix for iteration
/// Returns the prefix bytes for keys with slot in [slot_start, slot_end)
pub fn slot_range_prefix(slot_start: u32, slot_end: u32) -> (Vec<u8>, Vec<u8>) {
    let start_bytes = encode_slot(slot_start);
    let end_bytes = encode_slot(slot_end);
    (start_bytes.to_vec(), end_bytes.to_vec())
}

