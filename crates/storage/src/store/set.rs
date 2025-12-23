//! Set Store implementation for HybridStore

use crate::memory::DataCow;
use crate::store::HybridStore;
use crate::traits::{SetStore, StoreError, StoreResult};
use bytes::Bytes;

impl SetStore for HybridStore {
    fn sadd(&self, key: &[u8], members: Vec<Bytes>) -> StoreResult<usize> {
        let slot_store = self.get_slot_store(key)?;
        let mut store_guard = slot_store.write();
        let mut count = 0;
        for member in members {
            if store_guard.memory_mut().add(key.to_vec(), member)? {
                count += 1;
            }
        }
        Ok(count)
    }

    fn srem(&self, key: &[u8], members: &[&[u8]]) -> StoreResult<usize> {
        let slot_store = self.get_slot_store(key)?;
        let mut store_guard = slot_store.write();
        let mut count = 0;
        for member in members {
            if store_guard.memory_mut().remove(key, member)? {
                count += 1;
            }
        }
        Ok(count)
    }

    fn smembers(&self, key: &[u8]) -> StoreResult<Vec<Bytes>> {
        let slot_store = self.get_slot_store(key)?;
        let store_guard = slot_store.read();
        // Get DataCow for the key and extract members
        if let Some(DataCow::Set(set)) = store_guard.memory().get(key) {
            Ok(set.members())
        } else {
            Err(StoreError::WrongType)
        }
    }

    fn sismember(&self, key: &[u8], member: &[u8]) -> StoreResult<bool> {
        let slot_store = self.get_slot_store(key)?;
        let store_guard = slot_store.read();
        Ok(store_guard.memory().contains(key, member))
    }

    fn scard(&self, key: &[u8]) -> StoreResult<usize> {
        let slot_store = self.get_slot_store(key)?;
        let store_guard = slot_store.read();
        store_guard
            .memory()
            .len(key)
            .ok_or(StoreError::WrongType)
    }
}

