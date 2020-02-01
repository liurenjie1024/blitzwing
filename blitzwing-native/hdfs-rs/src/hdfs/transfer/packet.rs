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

pub(super) struct PacketReceiver {
    buffer: BytesMut,
    eof: bool,
}

#[derive(Debug)]
pub(super) struct PacketHeader {
    packet_len: u32,
    proto: PacketHeaderProto,
}

#[derive(Debug)]
pub(super) struct Packet<'a> {
    header: PacketHeader,
    checksum: &'a [u8],
    data: &'a [u8],
}

impl PacketReceiver {
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(PACKET_LENGTHS_LEN),
            eof: false,
        }
    }
    
    pub fn next_packet<R: Read>(&mut self, stream: &mut R) -> Result<Option<Packet>> {
        if self.eof {
            return Ok(None)
        }
        unsafe {
            self.buffer.set_len(PACKET_LENGTHS_LEN);
        }
            
        stream.read_exact(self.buffer.as_mut())
            .context(HdfsLibErrorKind::IoError)?;
        
        let body_len: usize = self.buffer.get_u32() as usize;
        let header_len: usize = self.buffer.get_u16() as usize;
        
        self.buffer.resize(PACKET_LENGTHS_LEN + header_len + body_len - BODY_LENGTH_LEN, 0);
        
        stream.read_exact(self.buffer.as_mut())
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

#[cfg(test)]
mod tests {
    use std::io::prelude::*;
    use protobuf::Message;
    use bytes::BufMut;
    use super::*;
    use crate::error::Result;
    use failure::ResultExt;
    use std::io::Cursor;
    
    fn make_packet_header(data_len: i32, last_packet_in_block: bool, seq_no: i64)
                          -> PacketHeaderProto {
    
        let mut header = PacketHeaderProto::new();
        header.set_dataLen(data_len);
        header.set_lastPacketInBlock(last_packet_in_block);
        header.set_seqno(seq_no);
        header.set_syncBlock(false);
        
        header
    }
    
    fn prepare_packet(checksum: &[u8], data: &[u8]) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        
        let packet_len = checksum.len() + data.len() + BODY_LENGTH_LEN;
    
        let header = make_packet_header(data.len() as i32, false, 1);
        
        buffer.put_u32(packet_len as u32);
        buffer.put_u16(header.compute_size() as u16);
        
        header.write_to_vec(&mut buffer).context(HdfsLibErrorKind::ProtobufError)?;
        
        buffer.write_all(checksum).context(HdfsLibErrorKind::IoError)?;
        buffer.write_all(data).context(HdfsLibErrorKind::IoError)?;
        
        Ok(buffer)
    }
    
    #[test]
    fn test_read_packet() {
        let checksum = vec![1u8, 2u8, 3u8];
        let data = vec![2u8, 3u8, 4u8];
        let packet_data = prepare_packet(&checksum, &data)
            .expect("Failed to prepare packet data");
        
        let mut packet_receiver = PacketReceiver::new();
        
        let mut reader = Cursor::new(packet_data);
        let packet = packet_receiver.next_packet(&mut reader)
            .expect("Failed to read packet")
            .expect("Packet should not be None");
        
        let expected_header= make_packet_header(data.len() as i32, false, 1);
        assert_eq!(&expected_header, &packet.header.proto);
        assert_eq!(checksum.as_slice(), packet.checksum);
        assert_eq!(data.as_slice(), packet.data);
    }
}