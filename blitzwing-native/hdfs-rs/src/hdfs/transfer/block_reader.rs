use crate::error::{HdfsLibError, Result, HdfsLibErrorKind};
use crate::hdfs::hdfs_config::HdfsClientConfigRef;
use std::io::{Error, Read, Result as IoResult, Write};
use crate::hdfs::transfer::packet::{PacketReceiver, Packet};
use std::cmp::min;
use crate::hadoop_proto::datatransfer::{Status, ClientReadStatusProto};
use protobuf::Message;
use failure::ResultExt;

pub(super) type BlockReaderRef = Box<dyn BlockReader>;

pub(super) trait BlockReader: Read {
    fn skip(&mut self, n: usize) -> Result<usize>;
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
    // These fields should not be changed after initialization
    io_stream: S,
    offset_in_block: u64,
    verify_checksum: bool,
    bytes_per_checksum: usize,
    
    // fields for protocol
    last_seq_num: i64,
    bytes_to_read_remaining: u64,
    
    skip_buffer: Vec<u8>,
    
    // Current packet
    packet_receiver: PacketReceiver,
    pos_in_packet: usize,
}

impl<S> RemoteBlockReader<S>
where S: Read + Write
{
    fn bytes_left_in_current_packet(&self) -> u64 {
        self.packet_receiver
            .data_slice()
            .map(|s| s.len() - self.pos_in_packet)
            .unwrap_or(0) as u64
    }
    
    fn bytes_left_in_current_block(&self) -> u64 {
        self.bytes_left_in_current_packet() + self.bytes_to_read_remaining
    }
    
    fn current_packet(&self) -> Result<Packet> {
        self.packet_receiver.current_packet().ok_or_else(||
            HdfsLibError::from(HdfsLibErrorKind::IllegalStateError(format!("{}", "Packet should not be none"))))
    }

    fn read_next_packet(&mut self) -> Result<()> {
        self.packet_receiver.receive_next_packet(&mut self.io_stream)?;
    
        let packet = self.current_packet()?;
        let packet_offset_in_block = packet.header.offset_in_block();
    
        // Sanity check
        packet.sanity_check()?;
    
        if packet.header.data_len() > 0 {
            check_protocol_content!((self.last_seq_num + 1) == packet.header.seq_num());
            // TODO: Verify checksum
        
            self.bytes_to_read_remaining -= packet.header.data_len() as u64;
            self.pos_in_packet = 0;
            if packet_offset_in_block < self.offset_in_block {
                self.pos_in_packet = self.offset_in_block - packet_offset_in_block;
            }
        }
    
        self.last_seq_num = packet.header.seq_number();
        Ok(())
    }
    
    fn do_read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if buf.len() == 0 || self.bytes_left_in_current_block() == 0 {
            return Ok(0);
        }
        
        if self.bytes_left_in_current_packet() == 0 {
            self.read_next_packet()?;
        }
        
        let bytes_written = min(self.bytes_left_in_current_packet() as usize, buf.len());
        
        let packet_data = &self.current_packet()?.data[self.pos_in_packet..];
        buf.copy_from_slice(packet_data);
        
        self.pos_in_packet += bytes_written;
        
        if self.bytes_to_read_remaining == 0 {
            self.read_next_packet()?;
            let status = if self.verify_checksum {
                Status::CHECKSUM_OK
            } else {
                Status::SUCCESS
            };
            self.write_read_status(status)?;
        }
        
        Ok(bytes_written)
    }
    
    fn write_read_status(&mut self, status: Status) -> Result<()> {
        let mut read_status = ClientReadStatusProto::new();
        read_status.set_status(status);
        
        read_status.write_length_delimited_to_writer(&mut self.io_stream)
            .context(HdfsLibErrorKind::ProtobufError)?;
        
        self.io_stream.flush()?;
        Ok(())
    }
    
}

impl<S> Read for RemoteBlockReader<S>
where
    S: Read + Write,
{
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        self.do_read(buf)
            .map_err(|e| e.into_std_io_error())
    }
}

impl<S> BlockReader for RemoteBlockReader<S>
where
    S: Read + Write,
{
    fn skip(&mut self, n: usize) -> Result<usize> {
        self.skip_buffer.resize(self.bytes_per_checksum, 0);
        
        let mut bytes_skipped = 0;
        while bytes_skipped < n {
            let bytes_to_read = min(n - bytes_skipped, self.bytes_per_checksum);
            let bytes_read = self.do_read(&mut self.skip_buffer.as_mut_slice()[0..bytes_to_read])?;
            
            if bytes_read == 0 {
                return Ok(bytes_skipped);
            } else {
                bytes_skipped += bytes_read;
            }
        }
        
        Ok(bytes_skipped)
    }
}
