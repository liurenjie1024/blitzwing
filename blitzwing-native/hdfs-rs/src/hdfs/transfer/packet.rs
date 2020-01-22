use std::io::Read;
use bytes::{Bytes, BytesMut, Buf};
use std::mem::size_of;
use crate::error::{Result, HdfsLibErrorKind};
use crate::hadoop_proto::datatransfer::PacketHeaderProto;
use failure::ResultExt;
use protobuf::{parse_from_reader, parse_from_bytes};
use crate::error::HdfsLibErrorKind::ProtobufError;

const BODY_LENGTH_LEN: usize = size_of::<u32>();
const HEADER_LENGTH_LEN: usize = size_of::<u16>();
const PACKET_LENGTHS_LEN: usize = BODY_LENGTH_LEN + HEADER_LENGTH_LEN;

pub(super) struct PacketReceiver<S: Read> {
    stream: S,
    buffer: BytesMut,
    eof: bool,
}

pub(super) struct PacketHeader {
    packet_len: u32,
    proto: PacketHeaderProto,
}

pub(super) struct Packet<'a> {
    header: PacketHeader,
    checksum: &'a [u8],
    data: &'a [u8],
}

impl<S: Read> PacketReceiver<S> {
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            buffer: BytesMut::with_capacity(PACKET_LENGTHS_LEN),
            eof: false,
        }
    }
    
    pub fn next_packet(&mut self) -> Result<Option<Packet>> {
        if self.eof {
            return Ok(None)
        }
        unsafe {
            self.buffer.set_len(PACKET_LENGTHS_LEN);
        }
            
            self.stream.read_exact(self.buffer.as_mut())
                .context(HdfsLibErrorKind::IoError)?;
        
        let body_len: usize = self.buffer.get_u32() as usize;
        let header_len: usize = self.buffer.get_u16() as usize;
        
        self.buffer.resize(PACKET_LENGTHS_LEN + header_len + body_len - BODY_LENGTH_LEN, 0);
        
        self.stream.read_exact(self.buffer.as_mut())
            .context(HdfsLibErrorKind::IoError)?;
        
        let header_proto = parse_from_bytes::<PacketHeaderProto>(self.buffer.bytes())
            .context(ProtobufError)?;
        
        let data_len = header_proto.get_dataLen() as usize;
        let checksum_len = body_len - BODY_LENGTH_LEN - data_len;
        
        let checksum_start = PACKET_LENGTHS_LEN + header_len;
        let checksum_end = checksum_start + checksum_len;
        let checksum = &self.buffer.as_ref()[checksum_start..checksum_end];
        
        let data_start = checksum_end;
        let data_end = data_start + data_len;
        let data = &self.buffer.as_ref()[data_start..data_end];
        
        self.eof = header_proto.get_lastPacketInBlock();
        
        Ok(Some(Packet {
            header: PacketHeader {
                packet_len: header_len as u32,
                proto: header_proto,
            },
            checksum,
            data,
        }))
    }
}