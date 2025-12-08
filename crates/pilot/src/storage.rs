//! File persistent storage
//!
//! Persists cluster metadata to files

use std::path::{Path, PathBuf};
use tokio::fs;
use tracing::info;

use crate::metadata::ClusterMetadata;

/// Storage error
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("File not found: {0}")]
    NotFound(PathBuf),
}

/// File storage
pub struct FileStorage {
    /// Data directory
    data_dir: PathBuf,
    /// Metadata file path
    metadata_path: PathBuf,
}

impl FileStorage {
    /// Create file storage
    pub fn new<P: AsRef<Path>>(data_dir: P) -> Self {
        let data_dir = data_dir.as_ref().to_path_buf();
        let metadata_path = data_dir.join("cluster_metadata.json");
        Self {
            data_dir,
            metadata_path,
        }
    }

    /// Ensure data directory exists
    pub async fn ensure_dir(&self) -> Result<(), StorageError> {
        if !self.data_dir.exists() {
            fs::create_dir_all(&self.data_dir).await?;
            info!("Created data directory: {:?}", self.data_dir);
        }
        Ok(())
    }

    /// Load cluster metadata
    pub async fn load(&self) -> Result<Option<ClusterMetadata>, StorageError> {
        if !self.metadata_path.exists() {
            info!("Metadata file not found, will create new cluster");
            return Ok(None);
        }

        let content = fs::read_to_string(&self.metadata_path).await?;
        let metadata: ClusterMetadata = serde_json::from_str(&content)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        
        info!(
            "Loaded cluster metadata: {} nodes, {} shards, routing version {}",
            metadata.nodes.len(),
            metadata.shards.len(),
            metadata.routing_table.version
        );
        
        Ok(Some(metadata))
    }

    /// Save cluster metadata
    pub async fn save(&self, metadata: &ClusterMetadata) -> Result<(), StorageError> {
        self.ensure_dir().await?;

        // Write to temp file first, then atomically rename
        let temp_path = self.metadata_path.with_extension("json.tmp");
        let content = serde_json::to_string_pretty(metadata)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        
        fs::write(&temp_path, &content).await?;
        fs::rename(&temp_path, &self.metadata_path).await?;

        info!(
            "Saved cluster metadata: routing version {}",
            metadata.routing_table.version
        );

        Ok(())
    }

    /// Load or create cluster metadata
    pub async fn load_or_create(&self, cluster_name: &str) -> Result<ClusterMetadata, StorageError> {
        match self.load().await? {
            Some(metadata) => Ok(metadata),
            None => {
                let mut metadata = ClusterMetadata::new(cluster_name.to_string());
                metadata.init_shards();
                self.save(&metadata).await?;
                info!("Created new cluster '{}' with {} shards", cluster_name, metadata.shards.len());
                Ok(metadata)
            }
        }
    }

    /// Backup current metadata
    pub async fn backup(&self) -> Result<PathBuf, StorageError> {
        if !self.metadata_path.exists() {
            return Err(StorageError::NotFound(self.metadata_path.clone()));
        }

        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
        let backup_path = self.data_dir.join(format!("cluster_metadata_{}.json.bak", timestamp));
        
        fs::copy(&self.metadata_path, &backup_path).await?;
        info!("Backed up metadata to {:?}", backup_path);
        
        Ok(backup_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_save_and_load() {
        let dir = std::env::temp_dir().join(format!("pilot_test_{}", std::process::id()));
        let storage = FileStorage::new(&dir);

        let metadata = storage.load_or_create("test-cluster").await.unwrap();
        assert_eq!(metadata.name, "test-cluster");
        assert!(!metadata.shards.is_empty());

        // Reloading should get same data
        let loaded = storage.load().await.unwrap().unwrap();
        assert_eq!(loaded.name, metadata.name);
        assert_eq!(loaded.shards.len(), metadata.shards.len());

        // Cleanup
        let _ = std::fs::remove_dir_all(&dir);
    }
}
