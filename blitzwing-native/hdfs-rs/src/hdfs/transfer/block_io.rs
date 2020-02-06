use crate::error::{HdfsLibError, HdfsLibErrorKind, Result};
use crate::hadoop_proto::datatransfer::{ClientReadStatusProto, Status};
use crate::hdfs::block::ExtendedBlock;
use crate::hdfs::datanode::DatanodeInfo;
use crate::hdfs::hdfs_config::HdfsClientConfigRef;
use crate::hdfs::transfer::connection::Connection;
use crate::hdfs::transfer::data_transfer_protocol::{
    BaseBlockOpInfo, DataTransferProtocol, ReadBlockRequest,
};
use crate::hdfs::transfer::packet::{Packet, PacketReceiver};
use failure::ResultExt;
use protobuf::Message;
use std::cell::UnsafeCell;
use std::cmp::min;
use std::io::{Read, Result as IoResult, Write};

pub(super) type BlockReaderRef = Box<dyn BlockReader>;

pub(super) trait BlockReader: Read {
    fn skip(&mut self, n: usize) -> Result<usize>;
}

#[derive(new, Setters)]
#[set = "pub"]
pub(super) struct BlockReaderInfo<C> {
    #[new(default)]
    config: HdfsClientConfigRef,
    #[new(value = "0")]
    start_offset: u64,
    #[new(default)]
    bytes_to_read: u64,
    #[new(value = "false")]
    verify_checksum: bool,
    #[new(default)]
    client_name: String,
    #[new(default)]
    datanode_info: DatanodeInfo,

    #[new(default)]
    filename: String,
    #[new(default)]
    block: ExtendedBlock,

    connection: C,
}

impl<C> BlockReaderInfo<C> {
    fn to_read_block_request(&self) -> ReadBlockRequest {
        let base_info = BaseBlockOpInfo::new(self.filename.clone(), self.block.clone());
        ReadBlockRequest::new(
            base_info,
            self.client_name.clone(),
            self.start_offset,
            self.bytes_to_read,
            self.verify_checksum,
        )
    }
}

struct RemoteBlockReader<C> {
    info: BlockReaderInfo<C>,
    bytes_per_checksum: usize,

    // fields for protocol
    last_seq_num: i64,
    bytes_to_read_remaining: u64,

    skip_buffer: UnsafeCell<Vec<u8>>,

    // Current packet
    packet_receiver: PacketReceiver,
    pos_in_packet: usize,
}

impl<C: Connection> RemoteBlockReader<C> {
    pub(super) fn new(mut info: BlockReaderInfo<C>) -> Result<Self> {
        let request = info.to_read_block_request();
        let mut protocol = DataTransferProtocol::new(&mut info.connection);

        let response = protocol.read_block(request)?;

        Ok(Self {
            bytes_per_checksum: response.bytes_per_checksum() as usize,
            last_seq_num: -1,
            bytes_to_read_remaining: info.bytes_to_read + info.start_offset
                - response.first_chunk_offset(),

            skip_buffer: UnsafeCell::new(Vec::new()),

            packet_receiver: PacketReceiver::new(),
            pos_in_packet: 0,
            info,
        })
    }

    fn bytes_left_in_current_packet(&self) -> u64 {
        if let Some(p) = self.packet_receiver.current_packet() {
            if p.data().len() >= self.pos_in_packet {
                (p.data().len() - self.pos_in_packet) as u64
            } else {
                0u64
            }
        } else {
            0u64
        }
    }

    fn bytes_left_in_current_block(&self) -> u64 {
        self.bytes_left_in_current_packet() + self.bytes_to_read_remaining
    }

    fn current_packet(&self) -> Result<Packet> {
        self.packet_receiver.current_packet().ok_or_else(|| {
            HdfsLibError::from(HdfsLibErrorKind::IllegalStateError(format!(
                "{}",
                "Packet should not be none"
            )))
        })
    }

    fn read_next_packet(&mut self) -> Result<()> {
        self.packet_receiver
            .receive_next_packet(self.info.connection.input_stream())?;

        let packet = self.current_packet()?;
        let packet_offset_in_block = packet.header().offset_in_block();

        // Sanity check
        packet.sanity_check()?;

        let cur_packet_data_len = packet.header().data_len() as u64;
        let cur_packet_seq_num = packet.header().seq_number();

        if packet.header().data_len() > 0 {
            check_protocol_content!((self.last_seq_num + 1) == cur_packet_seq_num);
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

        let packet_data = &self.current_packet()?.data()
            [self.pos_in_packet..(self.pos_in_packet + bytes_written)];
        (&mut buf[..bytes_written]).copy_from_slice(packet_data);

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

        read_status
            .write_length_delimited_to_writer(&mut self.info.connection.output_stream())
            .context(HdfsLibErrorKind::ProtobufError)?;

        self.info
            .connection
            .output_stream()
            .flush()
            .context(HdfsLibErrorKind::IoError)?;
        Ok(())
    }
}

impl<C: Connection> Read for RemoteBlockReader<C> {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        self.do_read(buf).map_err(|e| e.into_std_io_error())
    }
}

impl<C: Connection> RemoteBlockReader<C> {
    fn skip(&mut self, n: usize) -> Result<usize> {
        let skip_buffer = unsafe { &mut *self.skip_buffer.get() };
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

#[cfg(test)]
pub(crate) mod tests {
    use crate::hdfs::transfer::block_io::{BlockReaderInfo, RemoteBlockReader};
    use crate::hdfs::transfer::connection::Connection;
    use crate::hdfs::transfer::data_transfer_protocol::tests::make_read_checksum_response;
    use crate::hdfs::transfer::packet::tests::TestPacketGenerator;
    use protobuf::Message;
    use std::io::{Cursor, Read};
    use std::net::SocketAddr;

    #[derive(new, MutGetters, Getters, Setters)]
    #[set = "pub"]
    pub(super) struct TestConnection {
        #[get_mut = "pub"]
        input: Cursor<Vec<u8>>,
        #[new(default)]
        #[get_mut = "pub"]
        output: Vec<u8>,
        #[get = "pub"]
        address: SocketAddr,
    }

    impl Connection for TestConnection {
        type In = Cursor<Vec<u8>>;
        type Out = Vec<u8>;

        fn input_stream(&mut self) -> &mut Self::In {
            &mut self.input
        }

        fn output_stream(&mut self) -> &mut Self::Out {
            &mut self.output
        }

        fn local_address(&self) -> &SocketAddr {
            &self.address
        }

        fn remote_address(&self) -> &SocketAddr {
            &self.address
        }
    }

    #[test]
    fn test_read_block() {
        let mut network_input = Vec::new();

        let block_op_response = make_read_checksum_response(0, 4);
        block_op_response
            .write_length_delimited_to_vec(&mut network_input)
            .expect("Failed to write block op response");

        let test_packet_generator = TestPacketGenerator::new(4);
        let test_packets = test_packet_generator.generate();
        let block_content_len: usize = test_packets
            .packets()
            .iter()
            .map(|p| p.packet().data().len())
            .sum();

        network_input.extend_from_slice(test_packets.all_packet_data());

        let socket_address = "127.0.0.1:80"
            .parse()
            .expect("Failed to parse socket address!");
        let conn = TestConnection::new(Cursor::new(network_input), socket_address);

        let mut block_reader_info = BlockReaderInfo::new(conn);

        block_reader_info.set_bytes_to_read(block_content_len as u64);

        let mut block_reader = RemoteBlockReader::new(block_reader_info)
            .expect("Failed to build remote block reader!");

        let block_content: Vec<u8> = test_packets
            .packets()
            .iter()
            .flat_map(|p| *p.packet().data())
            .map(|v| *v)
            .collect();

        let mut read_content = Vec::with_capacity(block_content.len());
        block_reader
            .read_to_end(&mut read_content)
            .expect("Failed to read block content");

        assert_eq!(block_content, read_content);
    }
}
