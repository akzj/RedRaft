//! List Store with Incremental Copy-on-Write (COW)
//!
//! True incremental COW semantics - only changed lists are recorded, NOT full data copy:
//! - Snapshot: Only clones Arc (increases ref count), NO data copy
//! - Write: Records changes in small COW cache (only changed lists), NO full copy
//! - Read: Merges COW cache + base data (O(1) lookup)
//! - Merge: Applies only changed lists to base (O(M) where M = changes, not total data)
//!
//! Note: ListData is directly copied when modified (required for consistency).
//! This is acceptable because individual lists are typically small.
//!
//! Example: 1000 billion lists, modify 3 lists
//! - Old approach (Arc::make_mut): Copies all 1000 billion lists ❌
//! - This approach: Only records 3 changed lists in small HashMap ✅
//!
//! This module provides the core data structure for lists,
//! without implementing Redis API traits.

use std::collections::VecDeque;

use bytes::Bytes;


/// List data structure (VecDeque for O(1) head/tail operations)
/// 
/// Note: VecDeque<Bytes> is serializable because Bytes implements Serialize/Deserialize
pub type ListData = VecDeque<Bytes>;

