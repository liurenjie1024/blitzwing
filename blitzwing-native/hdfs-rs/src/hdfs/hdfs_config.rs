use crate::config::ConfigRef;
use std::sync::Arc;
use crate::error::Result;

pub type HdfsClientConfigRef = Arc<HdfsClientConfig>;

const KEY_DFS_BLOCK_SIZE: &'static str = "dfs.block.size";
const DFS_BLOCK_SIZE_DEFAULT: u64 = 128*1024*1024;

const KEY_DFS_CLIENT_READ_PREFETCH_SIZE_KEY: &'static str = "dfs.client.read.prefetch.size";

pub struct HdfsClientConfig {
    inner: ConfigRef,
    
    default_block_size: u64,
    prefetch_size: u64
}

impl HdfsClientConfig {
    pub fn new(inner: ConfigRef) -> Result<HdfsClientConfigRef> {
        let default_block_size = inner.get_or(KEY_DFS_BLOCK_SIZE, DFS_BLOCK_SIZE_DEFAULT)?;
        let prefetch_size = inner.get_or(KEY_DFS_CLIENT_READ_PREFETCH_SIZE_KEY, 10 *
            default_block_size)?;
        
        Ok(Arc::new(Self {
            inner,
            default_block_size,
            prefetch_size,
        }))
    }
    
    pub fn get_default_block_size(&self) -> u64 {
        self.default_block_size
    }
    
    pub fn get_prefetch_size(&self) -> u64 {
        self.prefetch_size
    }
}

