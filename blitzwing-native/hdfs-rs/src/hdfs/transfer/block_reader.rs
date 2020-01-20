use crate::error::{Result, HdfsLibError};
use std::io::{Read, Error, Result as IoResult, Write};
use crate::hdfs::hdfs_config::HdfsClientConfigRef;

pub type BlockReaderRef = Box<dyn BlockReader>;

pub(super) trait BlockReader: Read {
    fn skip(&mut self, n: usize) -> Result<()>;
}

pub struct BlockReaderBuilder {
    config: HdfsClientConfigRef,
    endpoint: String,
    // offset in block
    block_offset: u64
}

impl BlockReaderBuilder {
    pub fn new(config: HdfsClientConfigRef) -> Self {
        Self {
            config,
            endpoint: "".to_string(),
            block_offset: 0
        }
    }
    
    pub fn with_endpoint<S: ToString>(mut self, endpoint: S) -> Self {
        self.endpoint = endpoint.to_string();
        self
    }
    
    pub fn with_offset(mut self, offset: u64) -> Self {
        self.block_offset = offset;
        self
    }
    
    /// Currently we only support tcp remote block reader
    pub fn build(self) -> Result<BlockReaderRef> {
    }
}

struct RemoteBlockReader<S: Read + Write> {
    io_stream: S,
    // offset in block
    offset: u64,
}

impl<S> Read for RemoteBlockReader<S>
    where S: Read + Write
{
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        unimplemented!()
    }
}

impl<S> BlockReader for RemoteBlockReader<S>
    where S: Read + Write
{
    fn skip(&mut self, n: usize) -> Result<()> {
        unimplemented!()
    }
}




