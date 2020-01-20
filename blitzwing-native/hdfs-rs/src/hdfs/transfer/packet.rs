use std::io::Read;
use bytes::Bytes;
use std::mem::size_of;
use crate::error::Result;
use crate::hadoop_proto::datatransfer::PacketHeaderProto;

const PACKET_LENGTHS_LEN: usize = size_of::<u32>() + size_of::<u16>();

pub(super) struct PacketReceiver<S: Read> {
    stream: S,
    buffer: Bytes,
}

pub(super) struct PacketHeader {
    packet_len: u32,
    proto: PacketHeaderProto,
}

pub(super) struct Packet {
    header: Packet,
    check_sums: Bytes,
    data: Bytes,
}

impl<S: Read> PacketReceiver<S> {
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            buffer: Bytes::with_capacity(PACKET_LENGTHS_LEN)
        }
    }
    
    pub fn next_packet(&mut self) -> Result<Option<Packet>> {
        unimplemented!()
    }
}