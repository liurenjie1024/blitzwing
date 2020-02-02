use crate::error::HdfsLibErrorKind::ProtobufError;
use crate::error::{HdfsLibErrorKind, Result};
use crate::hadoop_proto::datatransfer::PacketHeaderProto;
use bytes::{Buf, Bytes, BytesMut};
use failure::ResultExt;
use failure::_core::ops::Deref;
use protobuf::{parse_from_bytes, parse_from_reader};
use std::io::Read;
use std::mem::size_of;

const BODY_LENGTH_LEN: usize = size_of::<u32>();
const HEADER_LENGTH_LEN: usize = size_of::<u16>();
const PACKET_LENGTHS_LEN: usize = BODY_LENGTH_LEN + HEADER_LENGTH_LEN;

pub(super) struct PacketReceiver {
    buffer: BytesMut,
    eof: bool,
}

#[derive(Debug, PartialEq, Clone)]
pub(super) struct PacketHeader {
    payload_len: u32,
    proto: PacketHeaderProto,
}

#[derive(Debug, PartialEq)]
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
            return Ok(None);
        }
        unsafe {
            self.buffer.set_len(PACKET_LENGTHS_LEN);
        }

        stream
            .read_exact(self.buffer.as_mut())
            .context(HdfsLibErrorKind::IoError)?;

        let body_len: usize = self.buffer.get_u32() as usize;
        let header_len: usize = self.buffer.get_u16() as usize;

        self.buffer.resize(
            PACKET_LENGTHS_LEN + header_len + body_len - BODY_LENGTH_LEN,
            0,
        );

        stream
            .read_exact(&mut self.buffer.as_mut()[PACKET_LENGTHS_LEN..])
            .context(HdfsLibErrorKind::IoError)?;

        let header_proto = parse_from_bytes::<PacketHeaderProto>(
            &self.buffer.bytes()[PACKET_LENGTHS_LEN..(PACKET_LENGTHS_LEN + header_len)],
        )
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
                payload_len: (body_len - BODY_LENGTH_LEN) as u32,
                proto: header_proto,
            },
            checksum,
            data,
        }))
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
    use std::rc::Rc;

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
        packet_data: Rc<Vec<u8>>,
        header: PacketHeader,
        checksum_start: usize,
        checksum_len: usize,
        data_start: usize,
        data_len: usize,
    }

    impl TestPacketData {
        fn add_offset(&self, offset: usize) -> Self {
            Self {
                packet_data: self.packet_data.clone(),
                header: self.header.clone(),
                checksum_start: self.checksum_start + offset,
                checksum_len: self.checksum_len,
                data_start: self.data_start + offset,
                data_len: self.data_len,
            }
        }

        fn packet(&self) -> Packet {
            let checksum_end = self.checksum_start + self.checksum_len;
            let data_end = self.data_start + self.data_len;
            Packet {
                header: self.header.clone(),
                checksum: &self.packet_data[self.checksum_start..checksum_end],
                data: &self.packet_data[self.data_start..data_end],
            }
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
            packet_data: Rc::new(packet_data),
            header: PacketHeader {
                proto: header,
                payload_len: (checksum.len() + data.len()) as u32,
            },
            checksum_start,
            checksum_len: checksum.len(),
            data_start,
            data_len: data.len(),
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

        // read before eof
        for i in 0..(test_packet_num - 1) {
            let packet = packet_receiver
                .next_packet(&mut reader)
                .expect(format!("Failed to read packet: {}.", i + 1).as_str())
                .expect(format!("Packet {} should not be None.", i + 1).as_str());

            assert_eq!(&test_packets[i].packet(), &packet);
        }

        // Read after eof should be Ok(None)
        assert!(packet_receiver
            .next_packet(&mut reader)
            .expect("Read after eof should be ok!")
            .is_none());
    }
}
