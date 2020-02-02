use crate::error::{HdfsLibError, Result};
use crate::hdfs::hdfs_config::HdfsClientConfigRef;
use std::io::{Error, Read, Result as IoResult, Write};
use crate::hdfs::transfer::packet::PacketReceiver;

pub(super) type BlockReaderRef = Box<dyn BlockReader>;

pub(super) trait BlockReader: Read {
    fn skip(&mut self, n: usize) -> Result<()>;
}

pub(super) struct BlockReaderBuilder {
    config: HdfsClientConfigRef,
    endpoint: String,
    offset_in_block: u64,
}

impl BlockReaderBuilder {
    pub fn new(config: HdfsClientConfigRef) -> Self {
        Self {
            config,
            endpoint: "".to_string(),
            offset_in_block: 0,
        }
    }

    pub fn with_endpoint<S: ToString>(mut self, endpoint: S) -> Self {
        self.endpoint = endpoint.to_string();
        self
    }

    pub fn with_offset(mut self, offset: u64) -> Self {
        self.offset_in_block = offset;
        self
    }

    /// Currently we only support tcp remote block reader
    fn build(self) -> Result<BlockReaderRef> {
        unimplemented!()
    }
}

struct RemoteBlockReader<S: Read + Write> {
    io_stream: S,
    offset_in_block: u64,
    
    bytes_to_read_remaining: u64,
    
    // Current packet
    packet_receiver: PacketReceiver,
    pos_in_packet: usize,
}

impl<S> RemoteBlockReader<S>
where S: Read + Write
{
    fn cur_packet_available(&self) -> u64 {
        self.packet_receiver
            .data_slice()
            .map(|s| s.len() - self.pos_in_packet)
            .unwrap_or(0) as u64
    }
}

impl<S> Read for RemoteBlockReader<S>
where
    S: Read + Write,
{
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        unimplemented!()
    }
}

impl<S> BlockReader for RemoteBlockReader<S>
where
    S: Read + Write,
{
    fn skip(&mut self, n: usize) -> Result<()> {
        unimplemented!()
    }
}
