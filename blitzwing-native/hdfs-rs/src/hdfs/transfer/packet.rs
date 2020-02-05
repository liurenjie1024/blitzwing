use crate::error::HdfsLibErrorKind::ProtobufError;
use crate::error::{HdfsLibErrorKind, Result};
use crate::hadoop_proto::datatransfer::PacketHeaderProto;
use bytes::{Buf};
use failure::ResultExt;

use protobuf::{parse_from_bytes};
use std::io::{Cursor, Read};
use std::mem::size_of;
use std::ops::Range;

const BODY_LENGTH_LEN: usize = size_of::<u32>();
const HEADER_LENGTH_LEN: usize = size_of::<u16>();
const PACKET_LENGTHS_LEN: usize = BODY_LENGTH_LEN + HEADER_LENGTH_LEN;


pub(super) struct PacketReceiver {
    buffer: Vec<u8>,
    cur_packet_info: Option<PacketInfo>,
}

#[derive(Debug, PartialEq, Clone)]
pub(super) struct PacketHeader {
    payload_len: u32,
    proto: PacketHeaderProto,
}

#[derive(Debug, PartialEq)]
pub(super) struct Packet<'a> {
    pub header: &'a PacketHeader,
    pub data: &'a [u8],
    pub checksum: &'a [u8],
}

struct PacketInfo {
    header: PacketHeader,
    data_idx: Range<usize>,
    checksum_idx: Range<usize>,
}

impl PacketHeader {
    pub(super) fn offset_in_block(&self) -> u64 {
        self.proto.get_offsetInBlock() as u64
    }
    
    pub(super) fn seq_number(&self) -> i64 {
        self.proto.get_seqno()
    }
    
    pub(super) fn last_packet_in_block(&self) -> bool {
        self.proto.get_lastPacketInBlock()
    }
    
    pub(super) fn data_len(&self) -> u32 {
        self.proto.get_dataLen() as u32
    }
}

impl PacketInfo {
    fn packet_of<'a, S: AsRef<[u8]>>(&'a self, buffer: &'a S) -> Packet<'a> {
        Packet {
            header: &self.header,
            data: &buffer.as_ref()[self.data_idx.clone()],
            checksum: &buffer.as_ref()[self.checksum_idx.clone()]
        }
    }
}

impl<'a> Packet<'a> {
    pub fn sanity_check(&self) -> Result<()> {
        if self.header.proto.get_lastPacketInBlock() {
            check_protocol_content!(self.header.proto.get_dataLen() == 0,
                "{}","Last packet should not contain data!")
        } else {
            check_protocol_content!(self.header.proto.get_dataLen() > 0,
                "{}","Not last packet should contain data!")
        }
        
        Ok(())
    }
}

impl PacketReceiver {
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(PACKET_LENGTHS_LEN),
            cur_packet_info: None,
        }
    }

    // This clears read pos
    pub(super) fn receive_next_packet<R: Read>(&mut self, stream: &mut R) -> Result<()> {
        self.cur_packet_info = None;
    
        self.buffer.resize(PACKET_LENGTHS_LEN, 0);
        stream.read_exact(&mut self.buffer)
            .context(HdfsLibErrorKind::IoError)?;

        let (body_len, header_len) = {
            let mut cursor = Cursor::new(self.buffer.as_slice());
            let body_len = cursor.get_u32() as usize;
            let header_len = cursor.get_u16() as usize;
            (body_len, header_len)
        };

        self.buffer.resize(
            PACKET_LENGTHS_LEN + header_len + body_len - BODY_LENGTH_LEN,
            0,
        );

        stream
            .read_exact(&mut self.buffer[PACKET_LENGTHS_LEN..])
            .context(HdfsLibErrorKind::IoError)?;

        let header_proto = parse_from_bytes::<PacketHeaderProto>(
            &self.buffer[PACKET_LENGTHS_LEN..(PACKET_LENGTHS_LEN + header_len)],
        )
        .context(ProtobufError)?;

        let data_len = header_proto.get_dataLen() as usize;
        let checksum_len = body_len - BODY_LENGTH_LEN - data_len;

        let checksum_start = PACKET_LENGTHS_LEN + header_len;
        let checksum_end = checksum_start + checksum_len;

        let data_start = checksum_end;
        let data_end = data_start + data_len;
        
        self.cur_packet_info = Some(PacketInfo {
            header: PacketHeader {
                payload_len: (body_len - BODY_LENGTH_LEN) as u32,
                proto: header_proto,
            },
            checksum_idx: checksum_start..checksum_end,
            data_idx: data_start..data_end,
        });
        
        Ok(())
    }
    
    // current packet
    pub(super) fn current_packet(&self) -> Option<Packet> {
        self.cur_packet_info.as_ref().map(|p| p.packet_of(&self.buffer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use bytes::BufMut;
    use failure::ResultExt;
    use protobuf::Message;
    use std::io::prelude::*;
    use std::io::Cursor;
    

    fn make_packet_header(
        data_len: i32,
        last_packet_in_block: bool,
        seq_no: i64,
    ) -> PacketHeaderProto {
        let mut header = PacketHeaderProto::new();
        header.set_dataLen(data_len);
        header.set_lastPacketInBlock(last_packet_in_block);
        header.set_offsetInBlock(1);
        header.set_seqno(seq_no);
        header.set_syncBlock(false);

        header
    }

    fn prepare_packet(header: &PacketHeaderProto, checksum: &[u8], data: &[u8]) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        let packet_len = checksum.len() + data.len() + BODY_LENGTH_LEN;

        buffer.put_u32(packet_len as u32);
        buffer.put_u16(header.compute_size() as u16);

        header
            .write_to_vec(&mut buffer)
            .context(HdfsLibErrorKind::ProtobufError)?;

        buffer
            .write_all(checksum)
            .context(HdfsLibErrorKind::IoError)?;
        buffer.write_all(data).context(HdfsLibErrorKind::IoError)?;

        Ok(buffer)
    }

    struct TestPacketData {
        packet_data: Vec<u8>,
        packet_info: PacketInfo,
    }

    impl TestPacketData {
        fn packet(&self) -> Packet {
            self.packet_info.packet_of(&self.packet_data)
        }
    }

    fn generate_test_packet(last_packet_in_block: bool) -> TestPacketData {
        let checksum = vec![1u8, 2u8, 3u8];
        let data = vec![2u8, 3u8, 4u8];
        let header = make_packet_header(data.len() as i32, last_packet_in_block, 1);
        let packet_data =
            prepare_packet(&header, &checksum, &data).expect("Failed to prepare packet data");

        let checksum_start = packet_data.len() - checksum.len() - data.len();
        let data_start = packet_data.len() - data.len();
        TestPacketData {
            packet_data,
            packet_info: PacketInfo {
                header: PacketHeader {
                    proto: header,
                    payload_len: (checksum.len() + data.len()) as u32,
                },
                checksum_idx: checksum_start..(checksum_start + checksum.len()),
                data_idx: data_start..(data_start + data.len())
            }
        }
    }

    #[test]
    fn test_read_packet() {
        let mut packet_data = Vec::new();
        let mut test_packets = Vec::new();

        let test_packet_num = 10;

        for i in 0..test_packet_num {
            let last_block = (i + 1) == (test_packet_num - 1);
            let test_packet_data = generate_test_packet(last_block);

            packet_data.extend_from_slice(test_packet_data.packet_data.as_slice());
            test_packets.push(test_packet_data);
        }

        //        println!("Packet data: {:?}", &packet_data);

        let mut packet_receiver = PacketReceiver::new();

        let mut reader = Cursor::new(packet_data);

        for i in 0..test_packet_num {
            packet_receiver
                .receive_next_packet(&mut reader)
                .expect(format!("Failed to read packet: {}.", i + 1).as_str());

            assert_eq!(Some(test_packets[i].packet()), packet_receiver.current_packet());
        }
    }
}
