use crate::rpc::message::receive_rpc_response;
use crate::rpc::message::RpcResponse;
use crate::rpc::message::send_rpc_request;
use crate::rpc::constants::RPC_INVALID_RETRY_COUNT;
use crate::rpc::constants::RPC_CURRENT_VERSION;
use crate::rpc::constants::RPC_CONNECTION_CONTEXT_CALL_ID;
use crate::rpc::constants::RPC_HEADER;
use crate::rpc::constants::RPC_SERVICE_CLASS_DEFAULT;
use crate::rpc::auth::AuthMethod::Simple;
use crate::rpc::message::make_rpc_request_header;
use crate::{
  error::{
    HdfsLibError, HdfsLibErrorKind,
    HdfsLibErrorKind::{
      IoError, LockError, SocketAddressParseError, SyncError, SystemError,
      TaskJoinError,
    },
    Result,
  },
  hadoop_proto::{
    IpcConnectionContext::{IpcConnectionContextProto, UserInformationProto},
    RpcHeader::{RpcKindProto, RpcRequestHeaderProto, RpcRequestHeaderProto_OperationProto},
  },
  rpc::auth::{AuthMethod, AuthProtocol},
};

use crate::{
  config::ConfigRef,
  hadoop_proto::{
    ProtobufRpcEngine::RequestHeaderProto,
  },
  rpc::message::{Messages, RpcMessageSerialize},
  rt::get_runtime,
};
use bytes::{
  buf::ext::BufMutExt,
  BufMut, Bytes, BytesMut,
};
use failure::ResultExt;
use protobuf::Message;
use std::{
  collections::HashMap,
  fmt::{Debug, Formatter},
};

use std::{
  net::{SocketAddr, ToSocketAddrs},
  sync::{
    atomic::{AtomicBool, AtomicI32, Ordering},
    mpsc::{sync_channel as mpsc_sync_channel, SyncSender as MpscSyncSender},
    Arc, Mutex,
  },
  time::Duration,
};
use tokio::{
  io::{split, AsyncRead, AsyncWrite, AsyncWriteExt},
  net::TcpStream,
  sync::mpsc::{channel as t_mpsc_channel, Receiver as TMpscReceiver, Sender as TMpscSender},
};


static CALL_ID: AtomicI32 = AtomicI32::new(1024);

type ConnectionId = String;


struct RpcCall {
  id: i32,
  request_header: RequestHeaderProto,
  request_body: Box<dyn Message>,
}

enum Event {
  Call(RpcCall),
  #[allow(dead_code)]
  Stop,
}

struct BasicRpcClientConfig {
  _inner: ConfigRef,
}

struct BasicRpcClientContext {
  endpoint: SocketAddr,
  service_class: i32,
  auth_protocol: AuthProtocol,
  _auth_method: AuthMethod,
  client_id: Bytes,
}

struct ConnectionContext {
  rpc_client_context: Arc<BasicRpcClientContext>,
  protocol: String,
  calls: Arc<Mutex<HashMap<i32, MpscSyncSender<Result<RpcResponse>>>>>,
  stopped: AtomicBool,
}

struct ConnectionLoop {
  endpoint: SocketAddr,
  context: Arc<ConnectionContext>,
  event_queue: Option<TMpscReceiver<Event>>,
}

struct ConnectionReader {
  context: Arc<ConnectionContext>,
}

struct ConnectionWriter {
  context: Arc<ConnectionContext>,
  input_events: TMpscReceiver<Event>,
}

struct Connection {
  context: Arc<ConnectionContext>,
  sender: TMpscSender<Event>,
}

pub struct BasicRpcClient {
  _config: Arc<BasicRpcClientConfig>,
  context: Arc<BasicRpcClientContext>,
  connections: Mutex<HashMap<ConnectionId, Connection>>,
}

pub struct BasicRpcClientBuilder<'a> {
  remote_address_str: &'a str,
  client_id: Bytes,
  config: ConfigRef,
}

impl ConnectionContext {
  fn make_ipc_connection_context(&self) -> IpcConnectionContextProto {
    let mut user_info_proto = UserInformationProto::new();
    user_info_proto.set_effectiveUser("renliu".to_string());

    let mut ipc_conn_context_proto = IpcConnectionContextProto::new();
    ipc_conn_context_proto.set_protocol(self.protocol.to_string());
    ipc_conn_context_proto.set_userInfo(user_info_proto);

    ipc_conn_context_proto
  }

  fn make_rpc_request_header(
    &self,
    call_id: i32,
    retry_count: i32,
    rpc_op: RpcRequestHeaderProto_OperationProto,
  ) -> RpcRequestHeaderProto {
    make_rpc_request_header(call_id, retry_count, rpc_op, &self.rpc_client_context.client_id)
  }

  fn is_stopped(&self) -> bool {
    self.stopped.load(Ordering::SeqCst)
  }

  fn stop(&self) {
    self.stopped.store(true, Ordering::SeqCst)
  }
}

impl<'a> BasicRpcClientBuilder<'a> {
  pub fn new(remote_address_str: &'a str, client_id: Bytes, config: ConfigRef) -> Self {
    Self { remote_address_str, client_id, config }
  }

  pub fn build(self) -> Result<BasicRpcClient> {
    // TODO: Handle multi ip case
    let endpoint = self
      .remote_address_str
      .to_socket_addrs()
      .context(SocketAddressParseError(self.remote_address_str.to_string()))?
      .next()
      .ok_or_else(|| SocketAddressParseError(self.remote_address_str.to_string()))?;

    let config = Arc::new(BasicRpcClientConfig { _inner: self.config });
    let context = Arc::new(BasicRpcClientContext {
      endpoint,
      service_class: RPC_SERVICE_CLASS_DEFAULT,
      auth_protocol: AuthProtocol::None,
      _auth_method: Simple,
      client_id: self.client_id,
    });

    Ok(BasicRpcClient { _config: config, context, connections: Mutex::new(HashMap::new()) })
  }
}

impl BasicRpcClient {
  fn create_and_start_connection(&self, connection_id: &str) -> Result<Connection> {
    info!("Creating connection for connection id: {}", connection_id);

    let (sender, receiver) = t_mpsc_channel(100);
    debug!("Message queue constructed!");

    let conn_context = Arc::new(ConnectionContext {
      rpc_client_context: self.context.clone(),
      protocol: connection_id.to_string(),
      calls: Arc::new(Mutex::new(HashMap::new())),
      stopped: AtomicBool::new(false),
    });
    debug!("Connection context constructed!");

    let conn_loop = ConnectionLoop {
      endpoint: self.context.endpoint,
      context: conn_context.clone(),
      event_queue: Some(receiver),
    };

    get_runtime().spawn(async move { conn_loop.run().await });

    Ok(Connection { context: conn_context.clone(), sender })
  }

  pub fn call<Request, Response>(
    &self,
    header: RequestHeaderProto,
    body: Request,
  ) -> Result<Response>
  where
    Request: Message,
    Response: Message,
  {
    let mut conn_map = match self.connections.lock() {
      Ok(conn_map) => conn_map,
      Err(e) => {
        error!("Wrong status of rpc client, we should close this: {:?}", e);
        return Err(HdfsLibErrorKind::LockError.into());
      }
    };
    let conn_id = header.get_declaringClassProtocolName();

    // If connection not exits or stopped, we both need to create a new connection for it
    if conn_map.get(conn_id).map(|c| c.context.is_stopped()).unwrap_or(true) {
      let conn = self.create_and_start_connection(conn_id)?;
      conn_map.insert(conn_id.to_string(), conn);
    }

    (&conn_map[conn_id]).call(header, body)
  }
}

impl Connection {
  pub fn call<Request, Response>(
    &self,
    header: RequestHeaderProto,
    body: Request,
  ) -> Result<Response>
  where
    Request: Message,
    Response: Message,
  {
    let call_id = CALL_ID.fetch_add(1, Ordering::SeqCst);
    let (sender, receiver) = mpsc_sync_channel(0);

    let rpc_call = RpcCall {
      id: call_id,
      request_header: header,
      request_body: Box::new(body) as Box<dyn Message>,
    };

    {
      match self.context.calls.lock() {
        Ok(mut calls) => {
          calls.insert(call_id, sender);
        }
        Err(e) => {
          error!(
            "Failed to lock calls for connection [{:?}], call id [{}], {}",
            self.context, call_id, e
          );
          return Err(LockError.into());
        }
      }
    }

    let event = Event::Call(rpc_call);
    let mut event_sender = self.sender.clone();
    get_runtime().spawn(async move { event_sender.send(event).await });

    let timeout = Duration::from_secs(10);
    receiver.recv_timeout(timeout).context(HdfsLibErrorKind::TimeOutError(timeout))?.and_then(
      |resp| { resp.get_message() },
    )
  }
}

impl ConnectionReader {
  async fn run<T: AsyncRead + Unpin>(self, reader: T) {
    info!("Connection reader for [{:?}] started.", self.context);
    match self.do_run(reader).await {
      Ok(_) => info!("Connection reader for [{:?}] exited normally.", self.context),
      Err(e) => error!("Connection reader for [{:?}] exited with error: {}.", self.context, e),
    }
  }

  async fn do_run<T: AsyncRead + Unpin>(&self, mut reader: T) -> Result<()> {
    loop {
      if self.context.is_stopped() {
        info!("Stopping connection reader for [{:?}]", self.context);
        break;
      }

      let resp = receive_rpc_response(&mut reader).await?;
      let call_id = resp.header.get_callId() as i32;

      match self.context.calls.lock() {
        Ok(calls) => {
          if let Err(e) = calls
            .get(&call_id)
            .ok_or_else(|| {
              HdfsLibError::from(SystemError(format!(
                "Failed to find sender of call id: {}",
                call_id
              )))
            })
            .and_then(|s| s.send(Ok(resp)).context(SyncError).map_err(|e| e.into()))
          {
            error!("Failed to process result of rpc call [{}], {}", call_id, e);
          }
        }
        Err(e) => {
          error!("Failed to acquire calls lock for connection [{:?}]: {}", &self.context, e);
        }
      }
    }

    Ok(())
  }
}

impl ConnectionWriter {
  async fn run<T: AsyncWrite + Unpin>(mut self, w: T) {
    info!("Connection writer for [{:?}] started.", &self.context);

    match self.do_run(w).await {
      Ok(_) => info!("Connection writer for [{:?}] exited normally.", &self.context),
      Err(e) => error!("Connection writer for [{:?}] exited with error: {}.", &self.context, e),
    }
  }

  async fn do_run<T: AsyncWrite + Unpin>(&mut self, mut w: T) -> Result<()> {
    loop {
      if self.context.is_stopped() {
        break;
      }
      if let Some(event) = self.input_events.recv().await {
        match event {
          Event::Call(rpc_call) => {
            // Send request
            let rpc_request_header = self.make_rpc_request_header(
              rpc_call.id,
              0,
              RpcRequestHeaderProto_OperationProto::RPC_FINAL_PACKET,
            );

            let messages = [
              &rpc_request_header as &dyn Message,
              &rpc_call.request_header as &dyn Message,
              rpc_call.request_body.as_ref(),
            ];

            let request = Messages::new(&messages);

            let serialized_len = request.get_serialized_len()?;
            let mut buffer = BytesMut::with_capacity(4 + serialized_len);
            buffer.put_i32(serialized_len as i32);

            let mut writer = buffer.writer();
            request.serialize(&mut writer)?;

            let buffer = writer.into_inner();
            debug!("Write content for [{:?}] is {:?}", &self.context, buffer.as_ref());

            w.write_all(buffer.as_ref()).await.context(IoError)?;
          }
          Event::Stop => break,
        }
      }
    }

    Ok(())
  }

  fn make_rpc_request_header(
    &self,
    call_id: i32,
    retry_count: i32,
    rpc_op: RpcRequestHeaderProto_OperationProto,
  ) -> RpcRequestHeaderProto {
    let mut rpc_request_header = RpcRequestHeaderProto::new();
    rpc_request_header.set_rpcKind(RpcKindProto::RPC_PROTOCOL_BUFFER);
    rpc_request_header.set_rpcOp(rpc_op);
    rpc_request_header.set_callId(call_id);
    rpc_request_header.set_clientId(self.context.rpc_client_context.client_id.to_vec());
    rpc_request_header.set_retryCount(retry_count);

    rpc_request_header
  }
}

impl ConnectionLoop {
  const fn connection_header_len() -> usize {
    RPC_HEADER.as_bytes().len() + 3
  }

  async fn write_connection_header<W: AsyncWriteExt + Unpin>(
    &mut self,
    writer: &mut W,
  ) -> Result<()> {
    let mut dst = [0 as u8; ConnectionLoop::connection_header_len()];

    {
      let mut buffer: &mut [u8] = &mut dst;
      buffer.put_slice(RPC_HEADER.as_bytes());
      buffer.put_i8(RPC_CURRENT_VERSION as i8);

      let service_class = self.context.rpc_client_context.service_class;
      buffer.put_i8(service_class as i8);

      let call_id = self.context.rpc_client_context.auth_protocol.call_id();
      buffer.put_i8(call_id as i8);
    }

    debug!("Writing connection header for [{:?}]: {:?}", &self, &dst);
    writer.write_all(&dst).await.context(IoError)?;
    info!("Finished writing connection header for [{:?}]", &self);

    Ok(())
  }

  async fn write_connection_context<W: AsyncWriteExt + Unpin>(&mut self, w: &mut W) -> Result<()> {
    let body = self.context.make_ipc_connection_context();
    let header = self.context.make_rpc_request_header(
      RPC_CONNECTION_CONTEXT_CALL_ID,
      RPC_INVALID_RETRY_COUNT,
      RpcRequestHeaderProto_OperationProto::RPC_FINAL_PACKET,
    );

    debug!("Writing connection context for [{:?}]", &self);
    send_rpc_request(w, &header, &body).await?;
    info!("Finished writing connection context for [{:?}]", &self);

    Ok(())
  }

  async fn build_tcp_stream(&mut self) -> Result<TcpStream> {
    let tcp_stream = TcpStream::connect(self.endpoint).await.context(IoError)?;

    tcp_stream.set_nodelay(true).context(IoError)?;
    tcp_stream.set_keepalive(None).context(IoError)?;

    Ok(tcp_stream)
  }

  async fn run(mut self) {
    match self.do_run().await {
      Ok(()) => info!("Connection loop [{:?}] stopped normally.", self),
      Err(e) => error!("Connection loop [{:?}] exited with error: {}", self, e),
    }

    self.context.stop()
  }

  async fn do_run(&mut self) -> Result<()> {
    info!("Connection loop {:?} started!", self);

    info!("Starting to create tcp connection for {:?}", self);
    let mut tcp_stream = self.build_tcp_stream().await?;
    info!("Tcp connection for {:?} created", self);

    debug!("Starting to write connection header for {:?}", self);
    self.write_connection_header(&mut tcp_stream).await?;
    debug!("Finished writing connection header for {:?}", self);

    // if self.context.rpc_client_context.auth_protocol == AuthProtocol::Sasl {
    //   // let sasl_rpc_client = SaslProtocol::new(self.context.protocol.clone(), )
    // }

    debug!("Starting to write connection context for {:?}", self);
    self.write_connection_context(&mut tcp_stream).await?;
    debug!("Finished writing connection context for {:?}", self);

    let (tcp_reader, tcp_writer) = split(tcp_stream);
    let conn_reader = ConnectionReader { context: self.context.clone() };

    let event_queue = std::mem::replace(&mut self.event_queue, None).ok_or_else(|| {
      HdfsLibErrorKind::SystemError(
        "Connection event queue should have \
                 been initialized!"
          .to_string(),
      )
    })?;

    let conn_writer = ConnectionWriter { context: self.context.clone(), input_events: event_queue };

    let reader_future = get_runtime().spawn(async move { conn_reader.run(tcp_reader).await });

    let writer_future = get_runtime().spawn(async move { conn_writer.run(tcp_writer).await });

    reader_future.await.context(TaskJoinError)?;
    writer_future.await.context(TaskJoinError)?;

    Ok(())
  }
}

impl Debug for ConnectionLoop {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("ConnectionLoop")
      .field("endpoint", &self.endpoint)
      .field("protocol", &self.context.protocol.as_str())
      .finish()
  }
}

impl Debug for ConnectionContext {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("ConnectionContext")
      .field("endpoint", &self.rpc_client_context.endpoint)
      .field("protocol", &self.protocol)
      .finish()
  }
}