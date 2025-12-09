//! RocksDB-based persistent storage implementation
//!
//! Provides persistent storage for Redis data structures:
//! - String: Simple key-value storage with O(1) operations
//! - Hash: Field-based storage using key prefixes
//!
//! ## Key Encoding
//!
//! Keys are encoded with type prefix to support multiple data types:
//! - String: `s:{key}`
//! - Hash: `h:{key}:{field}`
//! - Hash metadata: `H:{key}` (stores field count)
//!
//! ## Features
//! - Persistent storage with WAL
//! - Checkpoint-based snapshots
//! - Slot-aware operations for sharding

mod store;

pub use store::RocksDBStore;

/// Key type prefixes for different Redis data types
pub mod key_prefix {
    /// String type prefix: s:{key}
    pub const STRING: u8 = b's';
    /// Hash type prefix: h:{key}:{field}
    pub const HASH: u8 = b'h';
    /// Hash metadata prefix: H:{key}
    pub const HASH_META: u8 = b'H';
}

/// Build a string key: s:{key}
pub fn string_key(key: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(2 + key.len());
    result.push(key_prefix::STRING);
    result.push(b':');
    result.extend_from_slice(key);
    result
}

/// Build a hash field key: h:{key}:{field}
pub fn hash_field_key(key: &[u8], field: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(3 + key.len() + field.len());
    result.push(key_prefix::HASH);
    result.push(b':');
    result.extend_from_slice(key);
    result.push(b':');
    result.extend_from_slice(field);
    result
}

/// Build a hash metadata key: H:{key}
pub fn hash_meta_key(key: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(2 + key.len());
    result.push(key_prefix::HASH_META);
    result.push(b':');
    result.extend_from_slice(key);
    result
}

/// Build hash field prefix for iteration: h:{key}:
pub fn hash_field_prefix(key: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(3 + key.len());
    result.push(key_prefix::HASH);
    result.push(b':');
    result.extend_from_slice(key);
    result.push(b':');
    result
}

/// Extract original key from encoded key
pub fn extract_key(encoded: &[u8]) -> Option<&[u8]> {
    if encoded.len() < 2 || encoded[1] != b':' {
        return None;
    }
    Some(&encoded[2..])
}

/// Extract field from hash field key
pub fn extract_hash_field<'a>(encoded: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
    // Expected format: h:{key}:{field}
    let prefix_len = 2 + key.len() + 1; // "h:" + key + ":"
    if encoded.len() <= prefix_len {
        return None;
    }
    Some(&encoded[prefix_len..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_key() {
        let key = string_key(b"mykey");
        assert_eq!(key, b"s:mykey");
    }

    #[test]
    fn test_hash_field_key() {
        let key = hash_field_key(b"myhash", b"field1");
        assert_eq!(key, b"h:myhash:field1");
    }

    #[test]
    fn test_hash_meta_key() {
        let key = hash_meta_key(b"myhash");
        assert_eq!(key, b"H:myhash");
    }

    #[test]
    fn test_extract_key() {
        let encoded = b"s:mykey";
        assert_eq!(extract_key(encoded), Some(b"mykey".as_slice()));
    }

    #[test]
    fn test_extract_hash_field() {
        let encoded = b"h:myhash:field1";
        assert_eq!(
            extract_hash_field(encoded, b"myhash"),
            Some(b"field1".as_slice())
        );
    }
}

