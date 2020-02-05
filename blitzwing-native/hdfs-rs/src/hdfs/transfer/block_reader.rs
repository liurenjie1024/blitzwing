use crate::error::{HdfsLibError, Result, HdfsLibErrorKind};
use crate::hdfs::hdfs_config::HdfsClientConfigRef;
use std::io::{Error, Read, Result as IoResult, Write};
use crate::hdfs::transfer::packet::{PacketReceiver, Packet};
use std::cmp::min;
use crate::hadoop_proto::datatransfer::{Status, ClientReadStatusProto};
use protobuf::Message;
use failure::ResultExt;
use std::cell::{RefCell, UnsafeCell};
use crate::hdfs::datanode::{DatanodeId, DatanodeInfo};
use crate::hdfs::block::ExtendedBlock;
use crate::hdfs::transfer::data_transfer_protocol::{DataTransferProtocol, ReadBlockRequest,
                                                    BaseBlockOpInfo};

pub(super) type BlockReaderRef = Box<dyn BlockReader>;

pub(super) trait BlockReader: Read {
    fn skip(&mut self, n: usize) -> Result<usize>;
}

pub(super) struct BlockReaderInfo<IN: Read, OUT: Write> {
    config: HdfsClientConfigRef,
    start_offset: u64,
    bytes_to_read: u64,
    verify_checksum: bool,
    client_name: String,
    datanode_info: DatanodeInfo,
    
    filename: String,
    block: ExtendedBlock,
    
    input: IN,
    output: OUT,
}

impl<IN: Read, OUT: Write> BlockReaderInfo<IN, OUT> {
    fn to_read_block_request(&self) -> ReadBlockRequest {
        let base_info = BaseBlockOpInfo::new(self.filename.clone(), self.block.clone());
        ReadBlockRequest::new(base_info, self.client_name.clone(), self.start_offset, self
            .bytes_to_read, self.verify_checksum)
    }
}

struct RemoteBlockReader<IN: Read, OUT: Write> {
    info: BlockReaderInfo<IN, OUT>,
    bytes_per_checksum: usize,
    
    // fields for protocol
    last_seq_num: i64,
    bytes_to_read_remaining: u64,
    
    skip_buffer: UnsafeCell<Vec<u8>>,
    
    // Current packet
    packet_receiver: PacketReceiver,
    pos_in_packet: usize,
}

impl<IN, OUT> RemoteBlockReader<IN, OUT>
where IN: Read,
      OUT: Write,
{
    pub(super) fn new(mut info: BlockReaderInfo<IN, OUT>) -> Result<Self> {
        let request = info.to_read_block_request();
        let mut protocol = DataTransferProtocol::new(&mut info.input, &mut info.output);

        let response =  protocol.read_block(request)?;
        
        Ok(Self {
            bytes_per_checksum: response.bytes_per_checksum() as usize,
            last_seq_num: -1,
            bytes_to_read_remaining: info.bytes_to_read + info.start_offset - response
                .first_chunk_offset(),
            
            skip_buffer: UnsafeCell::new(Vec::new()),
            
            packet_receiver: PacketReceiver::new(),
            pos_in_packet: 0,
            info,
        })
    }
    
    fn bytes_left_in_current_packet(&self) -> u64 {
        self.packet_receiver
            .current_packet()
            .map(|p| p.data.len() - self.pos_in_packet)
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
        self.packet_receiver.receive_next_packet(&mut self.info.input)?;
    
        let packet = self.current_packet()?;
        let packet_offset_in_block = packet.header.offset_in_block();
    
        // Sanity check
        packet.sanity_check()?;
        
        let cur_packet_data_len = packet.header.data_len() as u64;
        let cur_packet_seq_num = packet.header.seq_number();
    
        if packet.header.data_len() > 0 {
            check_protocol_content!((self.last_seq_num + 1) == cur_packet_seq_num );
            // TODO: Verify checksum
        
            self.bytes_to_read_remaining -= cur_packet_data_len;
            self.pos_in_packet = 0;
            if packet_offset_in_block < self.info.start_offset {
                self.pos_in_packet = (self.info.start_offset - packet_offset_in_block) as usize;
            }
        }
    
        self.last_seq_num = cur_packet_seq_num;
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
            let status = if self.info.verify_checksum {
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
        
        read_status.write_length_delimited_to_writer(&mut self.info.output)
            .context(HdfsLibErrorKind::ProtobufError)?;
        
        self.info.output.flush()
            .context(HdfsLibErrorKind::IoError)?;
        Ok(())
    }
    
}

impl<IN, OUT> Read for RemoteBlockReader<IN, OUT>
where
    IN: Read,
    OUT: Write,
{
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        self.do_read(buf)
            .map_err(|e| e.into_std_io_error())
    }
}


impl<IN, OUT> RemoteBlockReader<IN, OUT>
where
    IN: Read,
    OUT: Write,
{
    fn skip(&mut self, n: usize) -> Result<usize> {
        let skip_buffer = unsafe {
            &mut *self.skip_buffer.get()
        };
        skip_buffer.resize(self.bytes_per_checksum, 0);
        
        
        let mut bytes_skipped = 0;
        while bytes_skipped < n {
            let bytes_to_read = min(n - bytes_skipped, self.bytes_per_checksum);
            let bytes_read = self.do_read(&mut skip_buffer.as_mut_slice()[0..bytes_to_read])?;
            
            if bytes_read == 0 {
                return Ok(bytes_skipped);
            } else {
                bytes_skipped += bytes_read;
            }
        }
        
        Ok(bytes_skipped)
    }
}
