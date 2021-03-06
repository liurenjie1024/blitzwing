use crate::{
  error::{HdfsLibError, HdfsLibErrorKind, Result},
  hadoop_proto::datatransfer::{ClientReadStatusProto, Status},
  hdfs::{
    block::ExtendedBlock,
    datanode::DatanodeInfo,
    hdfs_config::HdfsClientConfigRef,
    transfer::{
      connection::{make_connection, Connection},
      data_transfer_protocol::{BaseBlockOpInfo, DataTransferProtocol, ReadBlockRequest},
      packet::{Packet, PacketReceiver},
    },
  },
};
use failure::ResultExt;
use protobuf::Message;
use std::{
  cell::UnsafeCell,
  cmp::min,
  io::{Read, Result as IoResult, Write},
};

pub(in crate::hdfs) type BlockReaderRef = Box<dyn BlockReader>;
pub(in crate::hdfs) type BlockReaderFactoryRef = Box<dyn BlockReaderFactory>;

pub(in crate::hdfs) trait BlockReaderFactory {
  fn create(&mut self, args: BlockReaderArgs) -> Result<BlockReaderRef>;
}

pub(in crate::hdfs) struct RemoteBlockReaderFactory {
  config: HdfsClientConfigRef,
}

impl BlockReaderFactory for RemoteBlockReaderFactory {
  fn create(&mut self, args: BlockReaderArgs) -> Result<BlockReaderRef> {
    let connection = make_connection(
      self.config.clone(),
      args.datanode_info.datanode_id().get_xfer_address(self.config.connect_dn_via_hostname()),
    )?;

    let mut reader_info = BlockReaderInfo::new(connection);
    reader_info.set_config(self.config.clone());
    reader_info.set_start_offset(args.start_offset);
    reader_info.set_bytes_to_read(args.bytes_to_read);
    reader_info.set_verify_checksum(args.verify_checksum);
    reader_info.set_client_name(args.client_name.clone());
    reader_info.set_datanode_info(args.datanode_info.clone());
    reader_info.set_filename(args.filename.clone());
    reader_info.set_block(args.block.clone());

    Ok(Box::new(RemoteBlockReader::new(reader_info)?) as BlockReaderRef)
  }
}

impl RemoteBlockReaderFactory {
  pub(in crate::hdfs) fn new(config: HdfsClientConfigRef) -> Self {
    Self { config }
  }
}

pub(in crate::hdfs) trait BlockReader: Read {
  fn available(&self) -> Result<usize>;
  fn skip(&mut self, n: usize) -> Result<usize>;
}

#[derive(new)]
pub(in crate::hdfs) struct BlockReaderArgs {
  start_offset: usize,
  bytes_to_read: usize,
  verify_checksum: bool,
  client_name: String,
  datanode_info: DatanodeInfo,

  filename: String,
  block: ExtendedBlock,
}

#[derive(new, Setters)]
#[set = "pub"]
pub(super) struct BlockReaderInfo<C> {
  #[new(default)]
  config: HdfsClientConfigRef,
  #[new(value = "0")]
  start_offset: usize,
  #[new(default)]
  bytes_to_read: usize,
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
  bytes_to_read_remaining: usize,

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

  fn bytes_left_in_current_packet(&self) -> usize {
    if let Some(p) = self.packet_receiver.current_packet() {
      if p.data().len() >= self.pos_in_packet {
        p.data().len() - self.pos_in_packet
      } else {
        0
      }
    } else {
      0
    }
  }

  fn bytes_left_in_current_block(&self) -> usize {
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
    self.packet_receiver.receive_next_packet(self.info.connection.input_stream())?;

    let packet = self.current_packet()?;
    let packet_offset_in_block = packet.header().offset_in_block();

    // Sanity check
    packet.sanity_check()?;

    let cur_packet_data_len = packet.header().data_len();
    let cur_packet_seq_num = packet.header().seq_number();

    if packet.header().data_len() > 0 {
      check_protocol_content!((self.last_seq_num + 1) == cur_packet_seq_num);
      // TODO: Verify checksum

      self.bytes_to_read_remaining -= cur_packet_data_len;
      self.pos_in_packet = 0;
      if packet_offset_in_block < self.info.start_offset {
        self.pos_in_packet = self.info.start_offset - packet_offset_in_block;
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

    let bytes_written = min(self.bytes_left_in_current_packet(), buf.len());

    let packet_data =
      &self.current_packet()?.data()[self.pos_in_packet..(self.pos_in_packet + bytes_written)];
    (&mut buf[..bytes_written]).copy_from_slice(packet_data);

    self.pos_in_packet += bytes_written;

    if self.bytes_to_read_remaining == 0 && self.bytes_left_in_current_packet() == 0 {
      self.read_next_packet()?;
      let status = if self.info.verify_checksum { Status::CHECKSUM_OK } else { Status::SUCCESS };
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

    self.info.connection.output_stream().flush().context(HdfsLibErrorKind::IoError)?;
    Ok(())
  }
}

impl<C: Connection> Read for RemoteBlockReader<C> {
  fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
    self.do_read(buf).map_err(|e| e.into_std_io_error())
  }
}

impl<C: Connection> BlockReader for RemoteBlockReader<C> {
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
  fn available(&self) -> Result<usize> {
    Ok(self.bytes_left_in_current_block())
  }
}

#[cfg(test)]
pub(crate) mod tests {
  use crate::hdfs::transfer::{
    block_io::{BlockReader, BlockReaderInfo, RemoteBlockReader},
    connection::Connection,
    data_transfer_protocol::tests::make_read_checksum_response,
    packet::tests::TestPacketGenerator,
  };
  use protobuf::Message;
  use std::{
    io::{Cursor, Read},
    net::SocketAddr,
  };

  type TestRemoteBlockReader = RemoteBlockReader<TestConnection>;

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

  fn prepare_block_reader(packet_num: u32) -> (TestRemoteBlockReader, Vec<u8>) {
    let mut network_input = Vec::new();

    let block_op_response = make_read_checksum_response(0, 4);
    block_op_response
      .write_length_delimited_to_vec(&mut network_input)
      .expect("Failed to write block op response");

    let test_packet_generator = TestPacketGenerator::new(packet_num);
    let test_packets = test_packet_generator.generate();
    let block_content_len: usize =
      test_packets.packets().iter().map(|p| p.packet().data().len()).sum();

    network_input.extend_from_slice(test_packets.all_packet_data());

    let socket_address = "127.0.0.1:80".parse().expect("Failed to parse socket address!");
    let conn = TestConnection::new(Cursor::new(network_input), socket_address);

    let mut block_reader_info = BlockReaderInfo::new(conn);

    block_reader_info.set_bytes_to_read(block_content_len);

    let block_reader =
      RemoteBlockReader::new(block_reader_info).expect("Failed to build remote block reader!");

    let block_content: Vec<u8> =
      test_packets.packets().iter().flat_map(|p| *p.packet().data()).map(|v| *v).collect();

    (block_reader, block_content)
  }

  #[test]
  fn test_read_block() {
    let (mut block_reader, block_content) = prepare_block_reader(4);

    let mut read_content = Vec::with_capacity(block_content.len());
    block_reader.read_to_end(&mut read_content).expect("Failed to read block content");

    assert_eq!(block_content, read_content);
  }

  #[test]
  fn test_read_block_multi_parts() {
    let (mut block_reader, block_content) = prepare_block_reader(4);

    let mut buf = [0u8; 1];

    for idx in 0..block_content.len() {
      println!("Current idx: {}", idx);
      block_reader.read_exact(&mut buf).expect("Read one by one should be ok!");
      assert_eq!(block_content[idx], buf[0]);
      assert_eq!(
        block_content.len() - idx - 1,
        block_reader.available().expect("Get avaiable should succeed")
      );
    }

    assert_eq!(0, block_reader.read(&mut buf).expect("Read pass block should not fail"));
  }

  #[test]
  fn test_read_block_and_skip() {
    let (mut block_reader, block_content) = prepare_block_reader(4);

    let mut read_content = Vec::with_capacity(block_content.len());
    read_content.resize(2, 0u8);
    block_reader.read_exact(&mut read_content).expect("Failed to read first 2 bytes of block");

    assert_eq!(&block_content[0..2], read_content.as_slice());

    let skipped = block_reader.skip(1).expect("Failed to skip 1 byte of block reader!");
    assert_eq!(1, skipped);

    read_content.resize(3, 0);
    block_reader.read_exact(&mut read_content).expect("Failed to read last 3 bytes of block");
    assert_eq!(&block_content[3..], read_content.as_slice());
  }
}
