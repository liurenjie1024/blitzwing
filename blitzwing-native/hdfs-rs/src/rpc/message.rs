use crate::{
  error::{
    HdfsLibErrorKind,
    HdfsLibErrorKind::{IoError, ProtobufError},
    Result, RpcRemoteErrorInfo,
  },
  hadoop_proto::RpcHeader::{
    RpcKindProto, RpcRequestHeaderProto, RpcRequestHeaderProto_OperationProto,
    RpcResponseHeaderProto, RpcResponseHeaderProto_RpcStatusProto,
  },
};
use bytes::{
  buf::ext::{BufExt, BufMutExt},
  BufMut, Bytes, BytesMut,
};
use failure::ResultExt;
use protobuf::{CodedInputStream, Message};
use std::{
  fmt::{Debug, Formatter},
  io::Write,
  ops::Deref,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(super) struct RpcResponse {
  pub(super) header: RpcResponseHeaderProto,
  pub(super) body: Bytes,
}

impl Debug for RpcResponse {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    let mut w = f.debug_list();
    for b in &self.body {
      w.entry(&b);
    }
    w.finish()
  }
}

impl RpcResponse {
  pub(super) fn get_message<R: Message>(self) -> Result<R> {
    match self.header.get_status() {
      RpcResponseHeaderProto_RpcStatusProto::SUCCESS => {
        let mut reader = self.body.reader();
        let mut input_stream = CodedInputStream::new(&mut reader);
        deserialize(&mut input_stream)
      }
      _ => Err(HdfsLibErrorKind::RpcRemoteError(RpcRemoteErrorInfo::from(&self.header)).into()),
    }
  }
}

pub trait RpcMessageSerialize {
  fn serialize<W: Write>(&self, buf: &mut W) -> Result<()>;
  fn get_serialized_len(&self) -> Result<usize>;
}

impl RpcMessageSerialize for &dyn Message {
  fn serialize<W: Write>(&self, buf: &mut W) -> Result<()> {
    self.write_length_delimited_to_writer(buf).context(HdfsLibErrorKind::ProtobufError)?;
    Ok(())
  }

  fn get_serialized_len(&self) -> Result<usize> {
    let size = self.compute_size();
    Ok((size_of_varint32(size) + size) as usize)
  }
}

pub struct Messages<'a, I, R>
where
  I: IntoIterator<Item = R> + Clone,
  R: Deref<Target = &'a dyn Message>,
{
  messages: I,
}

impl<'a, I, R> RpcMessageSerialize for Messages<'a, I, R>
where
  I: IntoIterator<Item = R> + Clone,
  R: Deref<Target = &'a dyn Message>,
{
  fn serialize<W: Write>(&self, buf: &mut W) -> Result<()> {
    for m in self.messages.clone().into_iter() {
      m.serialize(buf)?;
    }

    Ok(())
  }

  fn get_serialized_len(&self) -> Result<usize> {
    let mut sum = 0;
    for m in self.messages.clone().into_iter() {
      sum += m.get_serialized_len()?;
    }
    Ok(sum)
  }
}

impl<'a, I, R> Messages<'a, I, R>
where
  I: IntoIterator<Item = R> + Clone,
  R: Deref<Target = &'a dyn Message>,
{
  pub fn new(messages: I) -> Self {
    Self { messages }
  }
}

pub fn deserialize<M: Message>(input: &mut CodedInputStream) -> Result<M> {
  Ok(input.read_message().context(ProtobufError)?)
}

pub(crate) fn make_rpc_request_header<B: AsRef<[u8]>>(
  call_id: i32,
  retry_count: i32,
  rpc_op: RpcRequestHeaderProto_OperationProto,
  client_id: B,
) -> RpcRequestHeaderProto {
  let mut rpc_request_header = RpcRequestHeaderProto::new();
  rpc_request_header.set_rpcKind(RpcKindProto::RPC_PROTOCOL_BUFFER);
  rpc_request_header.set_rpcOp(rpc_op);
  rpc_request_header.set_callId(call_id);
  rpc_request_header.set_clientId(Vec::from(client_id.as_ref()));
  rpc_request_header.set_retryCount(retry_count);

  rpc_request_header
}

pub(super) async fn send_rpc_request<W: AsyncWriteExt + Unpin, M: Message>(
  w: &mut W,
  header: &RpcRequestHeaderProto,
  body: &M,
) -> Result<()> {
  let messages = [header as &dyn Message, body as &dyn Message];
  let request = Messages::new(&messages);
  let serialized_len = request.get_serialized_len()?;

  let mut buffer = BytesMut::with_capacity(4 + serialized_len);
  buffer.put_i32(serialized_len as i32);

  let mut writer = buffer.writer();
  request.serialize(&mut writer)?;

  let buffer = writer.into_inner();

  w.write_all(buffer.as_ref()).await.context(IoError)?;

  Ok(())
}

pub(super) async fn receive_rpc_response<R: AsyncReadExt + Unpin>(
  reader: &mut R,
) -> Result<RpcResponse> {
  let response_length = reader.read_i32().await.context(IoError)? as usize;
  let mut buffer = BytesMut::new();
  buffer.resize(response_length, 0);
  reader.read_exact(buffer.as_mut()).await.context(IoError)?;

  let buffer = buffer.freeze();

  let mut input_stream = CodedInputStream::from_bytes(buffer.as_ref());
  let header = deserialize::<RpcResponseHeaderProto>(&mut input_stream).context(ProtobufError)?;

  Ok(RpcResponse { header, body: buffer.slice(input_stream.pos() as usize..) })
}

fn size_of_varint32(mut value: u32) -> u32 {
  let mut i = 0;
  while (value & !0x7F) > 0 {
    value >>= 7;
    i += 1;
  }
  i + 1
}
