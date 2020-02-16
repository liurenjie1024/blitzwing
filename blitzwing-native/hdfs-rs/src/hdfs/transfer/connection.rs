use crate::{
  error::{
    HdfsLibError,
    HdfsLibErrorKind::{InvalidArgumentError, IoError},
    Result,
  },
  hdfs::hdfs_config::HdfsClientConfigRef,
};
use failure::ResultExt;
use std::{
  fmt::Debug,
  io::{Read, Write},
  net::{SocketAddr, TcpStream, ToSocketAddrs},
  time::Duration,
};

pub trait Connection {
  type In: Read;
  type Out: Write;

  fn input_stream(&mut self) -> &mut Self::In;
  fn output_stream(&mut self) -> &mut Self::Out;

  fn local_address(&self) -> &SocketAddr;
  fn remote_address(&self) -> &SocketAddr;
}

struct TcpConnection {
  tcp_stream: TcpStream,
  local_address: SocketAddr,
  peer_address: SocketAddr,
}

impl Connection for TcpConnection {
  type In = TcpStream;
  type Out = TcpStream;

  fn input_stream(&mut self) -> &mut Self::In {
    &mut self.tcp_stream
  }

  fn output_stream(&mut self) -> &mut Self::Out {
    &mut self.tcp_stream
  }

  fn local_address(&self) -> &SocketAddr {
    &self.local_address
  }

  fn remote_address(&self) -> &SocketAddr {
    &self.peer_address
  }
}

pub(crate) fn make_connection<A>(
  config: HdfsClientConfigRef,
  remote_address: A,
) -> Result<impl Connection>
where
  A: ToSocketAddrs + Debug,
{
  for remote_addr in remote_address.to_socket_addrs().context(IoError)? {
    let tcp_stream =
      TcpStream::connect_timeout(&remote_addr, Duration::from_millis(config.socket_timeout()))
        .context(IoError)?;
    let local_address = tcp_stream.local_addr().context(IoError)?;
    let peer_address = tcp_stream.peer_addr().context(IoError)?;

    return Ok(TcpConnection { tcp_stream, local_address, peer_address });
  }

  Err(HdfsLibError::from(InvalidArgumentError(format!(
    "Unable to connect to {:?}",
    &remote_address
  ))))
}
