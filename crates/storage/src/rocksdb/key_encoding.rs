//! Key encoding and decoding utilities for RocksDB storage
//!
//! Provides functions to encode/decode keys for different data types:
//! - String: `s:{key}`
//! - Hash field: `h:{key}:{field}`
//! - Hash metadata: `H:{key}`
//! - Apply index: `@:apply_index`

/// Key type prefixes
pub mod key_prefix {
    pub const STRING: u8 = b's';
    pub const HASH: u8 = b'h';
    pub const HASH_META: u8 = b'H';
    pub const APPLY_INDEX: u8 = b'@'; // Special prefix for apply_index
}

/// Build apply_index key for shard
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

/// Build string key: `s:{key}`
pub fn string_key(key: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(2 + key.len());
    result.push(key_prefix::STRING);
    result.push(b':');
    result.extend_from_slice(key);
    result
}

/// Build hash field key: `h:{key}:{field}`
pub fn hash_field_key(key: &[u8], field: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(3 + key.len() + field.len());
    result.push(key_prefix::HASH);
    result.push(b':');
    result.extend_from_slice(key);
    result.push(b':');
    result.extend_from_slice(field);
    result
}

/// Build hash metadata key: `H:{key}`
pub fn hash_meta_key(key: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(2 + key.len());
    result.push(key_prefix::HASH_META);
    result.push(b':');
    result.extend_from_slice(key);
    result
}

/// Build hash field prefix for iteration: `h:{key}:`
pub fn hash_field_prefix(key: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(3 + key.len());
    result.push(key_prefix::HASH);
    result.push(b':');
    result.extend_from_slice(key);
    result.push(b':');
    result
}

/// Extract field from hash key
pub fn extract_hash_field<'a>(encoded: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
    let prefix_len = 2 + key.len() + 1;
    if encoded.len() <= prefix_len {
        return None;
    }
    Some(&encoded[prefix_len..])
}

