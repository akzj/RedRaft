//! Unit tests for the log storage module.

#[cfg(test)]
mod entry_tests {
    use crate::RaftId;
    use crate::message::LogEntry;
    use crate::storage::log::entry::*;

    fn create_test_entry(index: u64, term: u64) -> LogEntry {
        LogEntry {
            index,
            term,
            command: format!("command_{}", index).into_bytes(),
            is_config: false,
            client_request_id: None,
        }
    }

    fn test_raft_id(name: &str) -> RaftId {
        RaftId::new(format!("group_{}", name), format!("node_{}", name))
    }

    #[test]
    fn test_entry_meta_default() {
        let meta = EntryMeta::default();
        assert_eq!(meta.log_index, 0);
        assert_eq!(meta.term, 0);
        assert_eq!(meta.offset, 0);
        assert_eq!(meta.size, 0);
    }

    #[test]
    fn test_entry_meta_equality() {
        let meta1 = EntryMeta {
            log_index: 1,
            term: 2,
            offset: 100,
            size: 50,
        };
        let meta2 = meta1.clone();
        assert_eq!(meta1, meta2);
    }

    #[test]
    fn test_truncate_record_serialization() {
        let record = TruncateRecord {
            raft_id: test_raft_id("1"),
            truncate_index: 100,
        };

        let bytes = record.serialize().unwrap();
        let (deserialized, _) = TruncateRecord::deserialize(&bytes).unwrap();

        assert_eq!(deserialized.raft_id, record.raft_id);
        assert_eq!(deserialized.truncate_index, record.truncate_index);
    }

    #[test]
    fn test_log_entry_record_serialization() {
        let entry = create_test_entry(1, 1);
        let record = LogEntryRecord {
            raft_id: test_raft_id("1"),
            entry: entry.clone(),
        };

        let bytes = record.serialize().unwrap();
        let (deserialized, _) = LogEntryRecord::deserialize(&bytes).unwrap();

        assert_eq!(deserialized.raft_id, record.raft_id);
        assert_eq!(deserialized.entry.index, entry.index);
        assert_eq!(deserialized.entry.term, entry.term);
    }

    #[test]
    fn test_raft_entry_index_default() {
        let index = RaftEntryIndex::default();
        assert_eq!(index.first_log_index, 0);
        assert_eq!(index.last_log_index, 0);
        assert!(index.entries.is_empty());
    }

    #[test]
    fn test_raft_entry_index_is_valid_index() {
        let mut index = RaftEntryIndex::default();
        index.first_log_index = 5;
        index.last_log_index = 10;

        // Valid indices
        assert!(index.is_valid_index(5));
        assert!(index.is_valid_index(7));
        assert!(index.is_valid_index(10));

        // Invalid indices
        assert!(!index.is_valid_index(4));
        assert!(!index.is_valid_index(11));
        assert!(!index.is_valid_index(0));
    }

    #[test]
    fn test_raft_entry_index_get_entry() {
        let mut index = RaftEntryIndex::default();
        index.first_log_index = 5;
        index.last_log_index = 7;
        index.entries = vec![
            EntryMeta {
                log_index: 5,
                term: 1,
                offset: 0,
                size: 10,
            },
            EntryMeta {
                log_index: 6,
                term: 1,
                offset: 10,
                size: 10,
            },
            EntryMeta {
                log_index: 7,
                term: 2,
                offset: 20,
                size: 10,
            },
        ];

        // Valid get
        let entry = index.get_entry(6).unwrap();
        assert_eq!(entry.log_index, 6);
        assert_eq!(entry.offset, 10);

        // Invalid get
        assert!(index.get_entry(4).is_none());
        assert!(index.get_entry(8).is_none());
    }

    #[test]
    fn test_raft_entry_index_truncate_prefix() {
        let mut index = RaftEntryIndex::default();
        index.first_log_index = 1;
        index.last_log_index = 5;
        index.entries = vec![
            EntryMeta {
                log_index: 1,
                term: 1,
                offset: 0,
                size: 10,
            },
            EntryMeta {
                log_index: 2,
                term: 1,
                offset: 10,
                size: 10,
            },
            EntryMeta {
                log_index: 3,
                term: 1,
                offset: 20,
                size: 10,
            },
            EntryMeta {
                log_index: 4,
                term: 2,
                offset: 30,
                size: 10,
            },
            EntryMeta {
                log_index: 5,
                term: 2,
                offset: 40,
                size: 10,
            },
        ];

        // Truncate prefix at index 3 (remove entries 1, 2)
        index.truncate_prefix(3);

        assert_eq!(index.first_log_index, 3);
        assert_eq!(index.last_log_index, 5);
        assert_eq!(index.entries.len(), 3);
        assert_eq!(index.entries[0].log_index, 3);
    }

    #[test]
    fn test_raft_entry_index_truncate_prefix_no_op() {
        let mut index = RaftEntryIndex::default();
        index.first_log_index = 5;
        index.last_log_index = 10;
        index.entries = vec![EntryMeta {
            log_index: 5,
            term: 1,
            offset: 0,
            size: 10,
        }];

        // Truncate at index <= first_log_index should be a no-op
        index.truncate_prefix(3);
        assert_eq!(index.first_log_index, 5);

        index.truncate_prefix(5);
        assert_eq!(index.first_log_index, 5);
    }

    #[test]
    fn test_raft_entry_index_truncate_prefix_all() {
        let mut index = RaftEntryIndex::default();
        index.first_log_index = 1;
        index.last_log_index = 5;
        index.entries = vec![
            EntryMeta {
                log_index: 1,
                term: 1,
                offset: 0,
                size: 10,
            },
            EntryMeta {
                log_index: 2,
                term: 1,
                offset: 10,
                size: 10,
            },
        ];

        // Truncate all entries
        index.truncate_prefix(10);

        assert_eq!(index.first_log_index, 10);
        assert_eq!(index.last_log_index, 9); // saturating_sub(1)
        assert!(index.entries.is_empty());
    }

    #[test]
    fn test_raft_entry_index_truncate_suffix() {
        let mut index = RaftEntryIndex::default();
        index.first_log_index = 1;
        index.last_log_index = 5;
        index.entries = vec![
            EntryMeta {
                log_index: 1,
                term: 1,
                offset: 0,
                size: 10,
            },
            EntryMeta {
                log_index: 2,
                term: 1,
                offset: 10,
                size: 10,
            },
            EntryMeta {
                log_index: 3,
                term: 1,
                offset: 20,
                size: 10,
            },
            EntryMeta {
                log_index: 4,
                term: 2,
                offset: 30,
                size: 10,
            },
            EntryMeta {
                log_index: 5,
                term: 2,
                offset: 40,
                size: 10,
            },
        ];

        // Truncate suffix at index 3 (remove entries 4, 5)
        index.truncate_suffix(3);

        assert_eq!(index.first_log_index, 1);
        assert_eq!(index.last_log_index, 3);
        assert_eq!(index.entries.len(), 3);
    }

    #[test]
    fn test_raft_entry_index_truncate_suffix_no_op() {
        let mut index = RaftEntryIndex::default();
        index.first_log_index = 1;
        index.last_log_index = 5;
        index.entries = vec![EntryMeta {
            log_index: 1,
            term: 1,
            offset: 0,
            size: 10,
        }];

        // Truncate at index >= last_log_index should be a no-op
        index.truncate_suffix(5);
        assert_eq!(index.last_log_index, 5);

        index.truncate_suffix(10);
        assert_eq!(index.last_log_index, 5);
    }

    #[test]
    fn test_raft_entry_index_truncate_suffix_all() {
        let mut index = RaftEntryIndex::default();
        index.first_log_index = 5;
        index.last_log_index = 10;
        index.entries = vec![
            EntryMeta {
                log_index: 5,
                term: 1,
                offset: 0,
                size: 10,
            },
            EntryMeta {
                log_index: 6,
                term: 1,
                offset: 10,
                size: 10,
            },
        ];

        // Truncate all entries (index < first_log_index)
        index.truncate_suffix(2);

        assert_eq!(index.last_log_index, 4); // first_log_index - 1
        assert!(index.entries.is_empty());
    }

    #[test]
    fn test_entry_header_serialization() {
        let header = EntryHeader::new(100, EntryType::LogEntry, 12345);

        let bytes = header.serialize().unwrap();
        assert_eq!(bytes.len(), 16);

        // Deserialize the 16-byte header
        let deserialized = EntryHeader::deserialize(&bytes).unwrap();
        assert_eq!(deserialized.size, 100);
        assert_eq!(deserialized.magic_num, ENTRY_MAGIC_NUM);
        assert_eq!(deserialized.crc, 12345);
    }

    #[test]
    fn test_entry_header_invalid_length() {
        let short_data = [0u8; 10];
        let result = EntryHeader::deserialize(&short_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_entry_header_invalid_magic() {
        let mut data = [0u8; 16];
        // Set a valid entry type at offset 4..8
        data[4..8].copy_from_slice(&1u32.to_le_bytes());
        // Set invalid magic number at offset 8..12
        data[8..12].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());

        let result = EntryHeader::deserialize(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_entry_type_serialization() {
        let types = vec![
            (EntryType::LogEntry, 1u32),
            (EntryType::Snapshot, 2u32),
            (EntryType::ClusterConfig, 3u32),
            (EntryType::TruncatePrefix, 4u32),
            (EntryType::TruncateSuffix, 5u32),
        ];

        for (entry_type, expected_code) in types {
            let header = EntryHeader::new(50, entry_type, 0);
            let bytes = header.serialize().unwrap();

            // entry_type is at offset 4-8 in serialized format
            let mut type_bytes = [0u8; 4];
            type_bytes.copy_from_slice(&bytes[4..8]);
            let actual_code = u32::from_le_bytes(type_bytes);

            assert_eq!(actual_code, expected_code);
        }
    }
}

#[cfg(test)]
mod segment_tests {
    use std::fs::OpenOptions;
    use std::sync::Arc;
    use parking_lot::RwLock;
    use tempfile::TempDir;
    use tokio::sync::Semaphore;

    use crate::RaftId;
    use crate::message::{HardStateMap, LogEntry};
    use crate::storage::log::entry::*;
    use crate::storage::log::segment::*;

    fn create_test_entry(index: u64, term: u64) -> LogEntry {
        LogEntry {
            index,
            term,
            command: format!("command_{}", index).into_bytes(),
            is_config: false,
            client_request_id: None,
        }
    }

    fn test_raft_id(name: &str) -> RaftId {
        RaftId::new(format!("group_{}", name), format!("node_{}", name))
    }

    fn create_test_segment(dir: &TempDir) -> LogSegment {
        let file_path = dir.path().join("test_segment.log");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&file_path)
            .unwrap();

        LogSegment {
            file_name: file_path,
            entry_index: Index::default(),
            file: Arc::new(file),
            io_semaphore: Arc::new(Semaphore::new(4)),
            hard_states: RwLock::new(HardStateMap::default()),
        }
    }

    #[test]
    fn test_segment_write_log_entries() {
        let dir = TempDir::new().unwrap();
        let mut segment = create_test_segment(&dir);
        let raft_id = test_raft_id("1");

        let entries = vec![
            create_test_entry(1, 1),
            create_test_entry(2, 1),
            create_test_entry(3, 2),
        ];

        segment
            .write_log_entries(&raft_id, entries.clone())
            .unwrap();

        // Verify index was updated
        let raft_index = segment.entry_index.entries.get(&raft_id).unwrap();
        assert_eq!(raft_index.first_log_index, 1);
        assert_eq!(raft_index.last_log_index, 3);
        assert_eq!(raft_index.entries.len(), 3);
    }

    #[test]
    fn test_segment_write_empty_entries() {
        let dir = TempDir::new().unwrap();
        let mut segment = create_test_segment(&dir);
        let raft_id = test_raft_id("1");

        // Writing empty entries should be a no-op
        segment.write_log_entries(&raft_id, vec![]).unwrap();

        assert!(segment.entry_index.entries.get(&raft_id).is_none());
    }

    #[test]
    fn test_segment_write_truncate_prefix() {
        let dir = TempDir::new().unwrap();
        let mut segment = create_test_segment(&dir);
        let raft_id = test_raft_id("1");

        // First write some entries
        let entries = vec![
            create_test_entry(1, 1),
            create_test_entry(2, 1),
            create_test_entry(3, 2),
        ];
        segment.write_log_entries(&raft_id, entries).unwrap();

        // Now truncate prefix
        segment.write_truncate_prefix(&raft_id, 2).unwrap();

        let raft_index = segment.entry_index.entries.get(&raft_id).unwrap();
        assert_eq!(raft_index.first_log_index, 2);
        assert_eq!(raft_index.last_log_index, 3);
        assert_eq!(raft_index.entries.len(), 2);
    }

    #[test]
    fn test_segment_write_truncate_suffix() {
        let dir = TempDir::new().unwrap();
        let mut segment = create_test_segment(&dir);
        let raft_id = test_raft_id("1");

        // First write some entries
        let entries = vec![
            create_test_entry(1, 1),
            create_test_entry(2, 1),
            create_test_entry(3, 2),
        ];
        segment.write_log_entries(&raft_id, entries).unwrap();

        // Now truncate suffix
        segment.write_truncate_suffix(&raft_id, 2).unwrap();

        let raft_index = segment.entry_index.entries.get(&raft_id).unwrap();
        assert_eq!(raft_index.first_log_index, 1);
        assert_eq!(raft_index.last_log_index, 2);
        assert_eq!(raft_index.entries.len(), 2);
    }

    #[test]
    fn test_segment_first_last_entry() {
        let dir = TempDir::new().unwrap();
        let mut segment = create_test_segment(&dir);
        let raft_id = test_raft_id("1");

        // No entries yet
        assert!(segment.first_entry(&raft_id).is_none());
        assert!(segment.last_entry(&raft_id).is_none());

        // Write some entries
        let entries = vec![
            create_test_entry(1, 1),
            create_test_entry(2, 1),
            create_test_entry(3, 2),
        ];
        segment.write_log_entries(&raft_id, entries).unwrap();

        let first = segment.first_entry(&raft_id).unwrap();
        assert_eq!(first.log_index, 1);
        assert_eq!(first.term, 1);

        let last = segment.last_entry(&raft_id).unwrap();
        assert_eq!(last.log_index, 3);
        assert_eq!(last.term, 2);
    }

    #[test]
    fn test_segment_multi_raft() {
        let dir = TempDir::new().unwrap();
        let mut segment = create_test_segment(&dir);

        let raft_id1 = test_raft_id("1");
        let raft_id2 = test_raft_id("2");

        // Write entries for two different raft nodes
        segment
            .write_log_entries(
                &raft_id1,
                vec![create_test_entry(1, 1), create_test_entry(2, 1)],
            )
            .unwrap();

        segment
            .write_log_entries(
                &raft_id2,
                vec![
                    create_test_entry(1, 1),
                    create_test_entry(2, 2),
                    create_test_entry(3, 2),
                ],
            )
            .unwrap();

        // Verify both indices are independent
        let index1 = segment.entry_index.entries.get(&raft_id1).unwrap();
        assert_eq!(index1.entries.len(), 2);

        let index2 = segment.entry_index.entries.get(&raft_id2).unwrap();
        assert_eq!(index2.entries.len(), 3);
    }

    #[tokio::test]
    async fn test_segment_read_entry() {
        let dir = TempDir::new().unwrap();
        let mut segment = create_test_segment(&dir);
        let raft_id = test_raft_id("1");

        let entries = vec![
            create_test_entry(1, 1),
            create_test_entry(2, 1),
            create_test_entry(3, 2),
        ];
        segment
            .write_log_entries(&raft_id, entries.clone())
            .unwrap();
        segment.sync_data().unwrap();

        // Read back entries
        let entry1 = segment.read_entry(&raft_id, 1).await.unwrap();
        assert_eq!(entry1.index, 1);
        assert_eq!(entry1.term, 1);

        let entry3 = segment.read_entry(&raft_id, 3).await.unwrap();
        assert_eq!(entry3.index, 3);
        assert_eq!(entry3.term, 2);
    }

    #[tokio::test]
    async fn test_segment_read_entry_not_found() {
        let dir = TempDir::new().unwrap();
        let mut segment = create_test_segment(&dir);
        let raft_id = test_raft_id("1");

        let entries = vec![create_test_entry(5, 1)];
        segment.write_log_entries(&raft_id, entries).unwrap();

        // Try to read non-existent entry
        let result = segment.read_entry(&raft_id, 1).await;
        assert!(result.is_err());

        let result = segment.read_entry(&raft_id, 10).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_segment_replay() {
        let dir = TempDir::new().unwrap();
        let mut segment = create_test_segment(&dir);
        let raft_id = test_raft_id("1");

        // Write entries - sync after each operation to ensure data is on disk
        segment
            .write_log_entries(
                &raft_id,
                vec![
                    create_test_entry(1, 1),
                    create_test_entry(2, 1),
                    create_test_entry(3, 2),
                    create_test_entry(4, 2),
                    create_test_entry(5, 3),
                ],
            )
            .unwrap();
        segment.sync_data().unwrap();

        // Verify entries were written before truncate
        assert_eq!(
            segment
                .entry_index
                .entries
                .get(&raft_id)
                .unwrap()
                .entries
                .len(),
            5
        );

        segment.write_truncate_prefix(&raft_id, 2).unwrap();
        segment.sync_data().unwrap();

        segment.write_truncate_suffix(&raft_id, 4).unwrap();
        segment.sync_data().unwrap();

        // Verify index state before replay
        let before_index = segment.entry_index.entries.get(&raft_id).unwrap();
        assert_eq!(before_index.first_log_index, 2);
        assert_eq!(before_index.last_log_index, 4);
        assert_eq!(before_index.entries.len(), 3);

        // Clear the in-memory index
        segment.entry_index = Index::default();
        assert!(segment.entry_index.entries.is_empty());

        // Replay from segment
        segment.replay_segment().unwrap();

        // Verify the replayed state matches
        let raft_index = segment.entry_index.entries.get(&raft_id);
        assert!(raft_index.is_some(), "raft_index should exist after replay");
        let raft_index = raft_index.unwrap();
        assert_eq!(raft_index.first_log_index, 2, "first_log_index mismatch");
        assert_eq!(raft_index.last_log_index, 4, "last_log_index mismatch");
        assert_eq!(raft_index.entries.len(), 3, "entries count mismatch");
    }

    #[test]
    fn test_segment_replay_empty_file() {
        let dir = TempDir::new().unwrap();
        let mut segment = create_test_segment(&dir);

        // Replay empty file
        segment.replay_segment().unwrap();

        assert!(segment.entry_index.entries.is_empty());
    }
}

#[cfg(test)]
mod store_tests {
    use std::collections::HashMap;
    use std::fs::OpenOptions;
    use std::sync::Arc;
    use parking_lot::RwLock;
    use tempfile::TempDir;
    use tokio::sync::Semaphore;

    use crate::RaftId;
    use crate::message::{HardState, HardStateMap, LogEntry};
    use crate::storage::log::entry::Index;
    use crate::storage::log::segment::LogSegment;
    use crate::storage::log::store::*;
    use crate::traits::{HardStateStorage, LogEntryStorage};

    fn create_test_entry(index: u64, term: u64) -> LogEntry {
        LogEntry {
            index,
            term,
            command: format!("command_{}", index).into_bytes(),
            is_config: false,
            client_request_id: None,
        }
    }

    fn test_raft_id(name: &str) -> RaftId {
        RaftId::new(format!("group_{}", name), format!("node_{}", name))
    }

    fn create_test_store(
        dir: &TempDir,
    ) -> (
        LogEntryStore,
        tokio::sync::mpsc::UnboundedReceiver<LogEntryOpRequest>,
    ) {
        let file_path = dir.path().join("test_segment.log");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&file_path)
            .unwrap();

        let segment = LogSegment {
            file_name: file_path,
            entry_index: Index::default(),
            file: Arc::new(file),
            io_semaphore: Arc::new(Semaphore::new(4)),
            hard_states: RwLock::new(HardStateMap::default()),
        };

        let inner = LogEntryStoreInner {
            dir: dir.path().to_path_buf(),
            cache_entries_size: 100,
            io_semaphore: Arc::new(Semaphore::new(4)),
            segments: RwLock::new(Vec::new()),
            current_segment: RwLock::new(segment),
            cache_table: RwLock::new(HashMap::new()),
            hard_states: RwLock::new(HashMap::new()),
        };

        let options = LogEntryStoreOptions {
            memtable_memory_size: 1024 * 1024,
            batch_size: 10,
            cache_entries_size: 100,
            max_io_threads: 4,
            max_segment_size: 64 * 1024 * 1024,
            dir: dir.path().to_path_buf(),
            sync_on_write: false,
        };

        LogEntryStore::new(options, inner)
    }

    #[tokio::test]
    async fn test_store_hard_state() {
        let dir = TempDir::new().unwrap();
        let (store, _rx) = create_test_store(&dir);
        let raft_id = test_raft_id("1");

        // Initially no hard state
        let result = store.load_hard_state(&raft_id).await.unwrap();
        assert!(result.is_none());

        // Save hard state
        let hard_state = HardState {
            raft_id: raft_id.clone(),
            term: 5,
            voted_for: Some(test_raft_id("2")),
        };
        store
            .save_hard_state(&raft_id, hard_state.clone())
            .await
            .unwrap();

        // Load hard state
        let loaded = store.load_hard_state(&raft_id).await.unwrap().unwrap();
        assert_eq!(loaded.term, 5);
        assert_eq!(loaded.voted_for, Some(test_raft_id("2")));
    }

    #[tokio::test]
    async fn test_store_append_and_get_entries() {
        let dir = TempDir::new().unwrap();
        let (store, rx) = create_test_store(&dir);
        let raft_id = test_raft_id("1");

        // Start the store background task
        store.start(rx);

        let entries = vec![
            create_test_entry(1, 1),
            create_test_entry(2, 1),
            create_test_entry(3, 2),
        ];

        // Append entries
        store.append_log_entries(&raft_id, &entries).await.unwrap();

        // Get entries
        let result = store.get_log_entries(&raft_id, 1, 4).await.unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].index, 1);
        assert_eq!(result[2].index, 3);
    }

    #[tokio::test]
    async fn test_store_get_entries_from_cache() {
        let dir = TempDir::new().unwrap();
        let (store, rx) = create_test_store(&dir);
        let raft_id = test_raft_id("1");

        store.start(rx);

        let entries = vec![
            create_test_entry(1, 1),
            create_test_entry(2, 1),
            create_test_entry(3, 2),
        ];

        store.append_log_entries(&raft_id, &entries).await.unwrap();

        // Get subset from cache
        let result = store.get_log_entries(&raft_id, 2, 4).await.unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].index, 2);
        assert_eq!(result[1].index, 3);
    }

    #[tokio::test]
    async fn test_store_truncate_suffix() {
        let dir = TempDir::new().unwrap();
        let (store, rx) = create_test_store(&dir);
        let raft_id = test_raft_id("1");

        store.start(rx);

        let entries = vec![
            create_test_entry(1, 1),
            create_test_entry(2, 1),
            create_test_entry(3, 2),
            create_test_entry(4, 2),
            create_test_entry(5, 3),
        ];

        store.append_log_entries(&raft_id, &entries).await.unwrap();
        store.truncate_log_suffix(&raft_id, 3).await.unwrap();

        // Only entries 1-3 should remain
        let result = store.get_log_entries(&raft_id, 1, 6).await.unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result.last().unwrap().index, 3);
    }

    #[tokio::test]
    async fn test_store_truncate_prefix() {
        let dir = TempDir::new().unwrap();
        let (store, rx) = create_test_store(&dir);
        let raft_id = test_raft_id("1");

        store.start(rx);

        let entries = vec![
            create_test_entry(1, 1),
            create_test_entry(2, 1),
            create_test_entry(3, 2),
            create_test_entry(4, 2),
            create_test_entry(5, 3),
        ];

        store.append_log_entries(&raft_id, &entries).await.unwrap();
        store.truncate_log_prefix(&raft_id, 3).await.unwrap();

        // Only entries 3-5 should remain
        let result = store.get_log_entries(&raft_id, 1, 6).await.unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result.first().unwrap().index, 3);
    }

    #[tokio::test]
    async fn test_store_get_last_log_index() {
        let dir = TempDir::new().unwrap();
        let (store, rx) = create_test_store(&dir);
        let raft_id = test_raft_id("1");

        store.start(rx);

        // Initially (0, 0)
        let (index, term) = store.get_last_log_index(&raft_id).await.unwrap();
        assert_eq!(index, 0);
        assert_eq!(term, 0);

        // After appending
        let entries = vec![
            create_test_entry(1, 1),
            create_test_entry(2, 1),
            create_test_entry(3, 2),
        ];
        store.append_log_entries(&raft_id, &entries).await.unwrap();

        let (index, term) = store.get_last_log_index(&raft_id).await.unwrap();
        assert_eq!(index, 3);
        assert_eq!(term, 2);
    }

    #[tokio::test]
    async fn test_store_get_log_term() {
        let dir = TempDir::new().unwrap();
        let (store, rx) = create_test_store(&dir);
        let raft_id = test_raft_id("1");

        store.start(rx);

        let entries = vec![
            create_test_entry(1, 1),
            create_test_entry(2, 1),
            create_test_entry(3, 2),
        ];
        store.append_log_entries(&raft_id, &entries).await.unwrap();

        let term = store.get_log_term(&raft_id, 2).await.unwrap();
        assert_eq!(term, 1);

        let term = store.get_log_term(&raft_id, 3).await.unwrap();
        assert_eq!(term, 2);
    }

    #[tokio::test]
    async fn test_store_get_log_entries_term() {
        let dir = TempDir::new().unwrap();
        let (store, rx) = create_test_store(&dir);
        let raft_id = test_raft_id("1");

        store.start(rx);

        let entries = vec![
            create_test_entry(1, 1),
            create_test_entry(2, 1),
            create_test_entry(3, 2),
        ];
        store.append_log_entries(&raft_id, &entries).await.unwrap();

        let terms = store.get_log_entries_term(&raft_id, 1, 4).await.unwrap();
        assert_eq!(terms.len(), 3);
        assert_eq!(terms[0], (1, 1));
        assert_eq!(terms[1], (2, 1));
        assert_eq!(terms[2], (3, 2));
    }

    #[tokio::test]
    async fn test_store_multi_raft() {
        let dir = TempDir::new().unwrap();
        let (store, rx) = create_test_store(&dir);

        let raft_id1 = test_raft_id("1");
        let raft_id2 = test_raft_id("2");

        store.start(rx);

        // Append entries for two different raft groups
        store
            .append_log_entries(
                &raft_id1,
                &[create_test_entry(1, 1), create_test_entry(2, 1)],
            )
            .await
            .unwrap();

        store
            .append_log_entries(
                &raft_id2,
                &[
                    create_test_entry(1, 2),
                    create_test_entry(2, 2),
                    create_test_entry(3, 3),
                ],
            )
            .await
            .unwrap();

        // Verify independent indices
        let (idx1, term1) = store.get_last_log_index(&raft_id1).await.unwrap();
        assert_eq!(idx1, 2);
        assert_eq!(term1, 1);

        let (idx2, term2) = store.get_last_log_index(&raft_id2).await.unwrap();
        assert_eq!(idx2, 3);
        assert_eq!(term2, 3);
    }

    #[tokio::test]
    async fn test_store_append_empty_entries() {
        let dir = TempDir::new().unwrap();
        let (store, rx) = create_test_store(&dir);
        let raft_id = test_raft_id("1");

        store.start(rx);

        // Appending empty entries should be a no-op
        store.append_log_entries(&raft_id, &[]).await.unwrap();

        let (index, term) = store.get_last_log_index(&raft_id).await.unwrap();
        assert_eq!(index, 0);
        assert_eq!(term, 0);
    }
}
