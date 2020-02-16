use crate::{
  error::{BlockOpErrorInfo, HdfsLibErrorKind, Result},
  hadoop_proto::datatransfer::{
    BaseHeaderProto, BlockOpResponseProto, ClientOperationHeaderProto, OpReadBlockProto, Status,
  },
  hdfs::{block::ExtendedBlock, transfer::connection::Connection},
  utils::proto::{parse_delimited_message, ProtobufTranslate},
};
use bytes::BufMut;
use failure::ResultExt;
use protobuf::Message;
use std::{io::Write, mem::size_of};

/// Operations codes
const OP_WRITE_BLOCK: u8 = 80;
const OP_READ_BLOCK: u8 = 81;
const OP_READ_METADATA: u8 = 82;
const OP_REPLACE_BLOCK: u8 = 83;
const OP_COPY_BLOCK: u8 = 84;
const OP_BLOCK_CHECKSUM: u8 = 85;
const OP_TRANSFER_BLOCK: u8 = 86;
const OP_REQUEST_SHORT_CIRCUIT_FDS: u8 = 87;
const OP_RELEASE_SHORT_CIRCUIT_FDS: u8 = 88;
const OP_REQUEST_SHORT_CIRCUIT_SHM: u8 = 89;

const DATA_TRANSFER_VERSION: i16 = 28;

#[derive(Debug, Clone, new, Getters, Eq, PartialEq)]
#[get = "pub"]
pub struct BaseBlockOpInfo {
  filename: String,
  block: ExtendedBlock,
}

#[derive(new, Getters, CopyGetters)]
pub struct ReadBlockRequest {
  #[get = "pub"]
  base_info: BaseBlockOpInfo,
  //TODO: token, caching strategy
  #[get = "pub"]
  client_name: String,
  #[get_copy = "pub"]
  start_offset_in_block: usize,
  #[get_copy = "pub"]
  bytes_to_read: usize,
  #[get_copy = "pub"]
  send_checksum: bool,
}

#[derive(new, CopyGetters)]
#[get_copy = "pub"]
pub struct ReadBlockResponse {
  first_chunk_offset: usize,
  bytes_per_checksum: usize,
}

pub struct DataTransferProtocol<'a, C> {
  connection: &'a mut C,
}

impl<'a, C: Connection> DataTransferProtocol<'a, C> {
  pub fn new(connection: &'a mut C) -> Self {
    Self { connection }
  }

  fn send<M: Message>(&mut self, op: u8, message: M) -> Result<()> {
    // We add 8 here because we need to include serialized message length
    let mut buffer: Vec<u8> =
      Vec::with_capacity(request_header_size() + 8 + message.compute_size() as usize);

    buffer.put_i16(DATA_TRANSFER_VERSION);
    buffer.put_u8(op);
    message
      .write_length_delimited_to_writer(&mut buffer)
      .context(HdfsLibErrorKind::ProtobufError)?;

    self.connection.output_stream().write(&buffer).context(HdfsLibErrorKind::IoError)?;
    self.connection.output_stream().flush().context(HdfsLibErrorKind::IoError)?;

    Ok(())
  }

  pub fn read_block(&mut self, request: ReadBlockRequest) -> Result<ReadBlockResponse> {
    let mut proto = OpReadBlockProto::new();
    proto.set_header(build_client_operation_header(
      &request.base_info.block,
      request.client_name.as_str(),
    )?);
    proto.set_offset(request.start_offset_in_block as u64);
    proto.set_len(request.bytes_to_read as u64);
    proto.set_sendChecksums(request.send_checksum);

    self.send(OP_READ_BLOCK, proto)?;

    let response_proto: BlockOpResponseProto =
      parse_delimited_message(self.connection.input_stream())?;

    check_block_operation_response(&request.base_info, &response_proto)?;

    let checksum_info = response_proto.get_readOpChecksumInfo();

    let bytes_per_checksum = checksum_info.get_checksum().get_bytesPerChecksum() as usize;
    let first_chunk_offset = checksum_info.get_chunkOffset() as usize;

    check_protocol_content!(
      first_chunk_offset <= request.start_offset_in_block
        && (first_chunk_offset + bytes_per_checksum) > request.start_offset_in_block,
      "Block reader error in first chunk offset: [{:?}], bytes_per_check_sum: [{}], \
        first chunk offset: [{}], start offset in block: [{}]",
      &request.base_info,
      bytes_per_checksum,
      first_chunk_offset,
      request.start_offset_in_block
    );

    Ok(ReadBlockResponse { first_chunk_offset, bytes_per_checksum })
  }
}

fn build_base_header(block: &ExtendedBlock) -> Result<BaseHeaderProto> {
  let mut proto = BaseHeaderProto::new();
  proto.set_block(block.try_write_to()?);

  Ok(proto)
}

fn build_client_operation_header(
  block: &ExtendedBlock,
  client_name: &str,
) -> Result<ClientOperationHeaderProto> {
  let mut proto = ClientOperationHeaderProto::new();
  proto.set_baseHeader(build_base_header(block)?);
  proto.set_clientName(client_name.to_string());

  Ok(proto)
}

fn check_block_operation_response(
  base_info: &BaseBlockOpInfo,
  response: &BlockOpResponseProto,
) -> Result<()> {
  match response.get_status() {
    Status::SUCCESS => Ok(()),
    _ => Err(BlockOpErrorInfo::new(base_info.clone(), response).to_err()),
  }
}

const fn request_header_size() -> usize {
  size_of::<u8>() + size_of::<i16>()
}

#[cfg(test)]
pub(crate) mod tests {
  use crate::hadoop_proto::{
    datatransfer::{BlockOpResponseProto, ChecksumProto, ReadOpChecksumInfoProto, Status},
    hdfs::ChecksumTypeProto,
  };

  pub(crate) fn make_read_checksum_response(
    first_chunk_offset: usize,
    bytes_per_checksum: u32,
  ) -> BlockOpResponseProto {
    let mut checksum_proto = ChecksumProto::new();
    checksum_proto.set_field_type(ChecksumTypeProto::CHECKSUM_NULL);
    checksum_proto.set_bytesPerChecksum(bytes_per_checksum);

    let mut read_op_checksum_info_proto = ReadOpChecksumInfoProto::new();
    read_op_checksum_info_proto.set_checksum(checksum_proto);
    read_op_checksum_info_proto.set_chunkOffset(first_chunk_offset as u64);

    let mut block_op_response_proto = BlockOpResponseProto::new();
    block_op_response_proto.set_status(Status::SUCCESS);
    block_op_response_proto.set_readOpChecksumInfo(read_op_checksum_info_proto);

    block_op_response_proto
  }
}
