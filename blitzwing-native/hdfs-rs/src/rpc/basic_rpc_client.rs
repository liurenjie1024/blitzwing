use crate::error::HdfsLibErrorKind::{IoError, SocketAddressParseError};
use crate::error::{HdfsLibErrorKind, Result, RpcRemoteErrorInfo};
use crate::hadoop_proto::{
    IpcConnectionContext::{IpcConnectionContextProto, UserInformationProto},
    RpcHeader::{RpcKindProto, RpcRequestHeaderProto, RpcRequestHeaderProto_OperationProto},
};
use crate::rpc::auth::{AuthMethod, AuthProtocol, AUTH_PROTOCOL_NONE, AUTH_METHOD_SIMPLE};

use bytes::{Bytes, BytesMut, BufMut, Buf, IntoBuf};
use failure::ResultExt;
use protobuf::{Message, CodedInputStream};
use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncWriteExt, AsyncReadExt, AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{
    mpsc::{channel as t_mpsc_channel, Receiver as TMpscReceiver, Sender as TMpscSender}
};
use crate::rpc::message::{Messages, RpcMessageSerialize, deserialize};
use crate::hadoop_proto::ProtobufRpcEngine::RequestHeaderProto;
use crate::hadoop_proto::RpcHeader::{RpcResponseHeaderProto, RpcResponseHeaderProto_RpcStatusProto};
use std::sync::mpsc::{channel as mpsc_channel, Sender as MpscSender};
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;
use crate::config::ConfigRef;
use crate::rt::get_runtime;
use std::fmt::{Debug, Formatter};
use tokio::io::{ReadHalf, WriteHalf, split};


const RPC_HEADER: &'static str = "hrpc";
const RPC_CURRENT_VERSION: u8 = 9;
const RPC_SERVICE_CLASS_DEFAULT: i32 = 0;

const RPC_CONNECTION_CONTEXT_CALL_ID: i32 = -3;
const RPC_INVALID_RETRY_COUNT: i32 = -1;

static CALL_ID: AtomicI32 = AtomicI32::new(1024);

type ConnectionId = String;

struct Response {
    body: Bytes,
}

impl Debug for Response {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut w = f.debug_list();
        for b in &self.body {
            w.entry(&b);
        }
        w.finish()
    }
}

struct RpcCall {
    id: i32,
    request_header: RequestHeaderProto,
    request_body: Box<dyn Message>,
}

enum Event {
    Call(RpcCall),
    #[allow(dead_code)]
    Stop
}

struct BasicRpcClientConfig {
    _inner: ConfigRef
}

struct BasicRpcClientContext {
    endpoint: SocketAddr,
    service_class: i32,
    auth_protocol: &'static AuthProtocol,
    _auth_method: &'static AuthMethod,
    client_id: Bytes,
}

struct ConnectionLoop {
    _endpoint: SocketAddr,
    context: Arc<BasicRpcClientContext>,
    reader: Option<ConnectionReader>,
    writer: Option<ConnectionWriter>,
    protocol: String,
}

struct ConnectionReader {
    _context: Arc<BasicRpcClientContext>,
    calls: Arc<Mutex<HashMap<i32, MpscSender<Result<Response>>>>>
}

struct ConnectionWriter {
    context: Arc<BasicRpcClientContext>,
    input_events: TMpscReceiver<Event>,
    _protocol: String,
}

struct Connection {
    calls: Arc<Mutex<HashMap<i32, MpscSender<Result<Response>>>>>,
    sender: TMpscSender<Event>,
}

pub struct BasicRpcClient {
    _config: Arc<BasicRpcClientConfig>,
    context: Arc<BasicRpcClientContext>,
    connections: Mutex<HashMap<ConnectionId, Connection>>,
    calls: Arc<Mutex<HashMap<i32, MpscSender<Result<Response>>>>>
}

pub struct BasicRpcClientBuilder<'a> {
    remote_address_str: &'a str,
    client_id: Bytes,
    config: ConfigRef
}

impl<'a> BasicRpcClientBuilder<'a> {
    pub fn new(remote_address_str: &'a str, client_id: Bytes, config: ConfigRef) -> Self {
        Self {
            remote_address_str,
            client_id,
            config
        }
    }
    
    pub fn build(self) -> Result<BasicRpcClient> {
        // TODO: Handle multi ip case
        let endpoint = self.remote_address_str.to_socket_addrs()
            .context(SocketAddressParseError(self.remote_address_str.to_string()))?
            .next()
            .ok_or_else(|| SocketAddressParseError(self.remote_address_str.to_string()))?;
            
        
        let config = Arc::new(BasicRpcClientConfig { _inner: self.config });
        let context = Arc::new(BasicRpcClientContext {
            endpoint,
            service_class: RPC_SERVICE_CLASS_DEFAULT,
            auth_protocol: &AUTH_PROTOCOL_NONE,
            _auth_method: &AUTH_METHOD_SIMPLE,
            client_id: self.client_id
        });
        
        Ok(BasicRpcClient {
            _config: config,
            context,
            connections: Mutex::new(HashMap::new()),
            calls: Arc::new(Mutex::new(HashMap::new()))
        })
    }
}

impl BasicRpcClient {
    fn create_and_start_connection(&self, connection_id: &str) -> Result<Connection> {
        let (sender, receiver) = t_mpsc_channel(100);
        
        let conn_loop = ConnectionLoop {
            _endpoint: self.context.endpoint,
            context: self.context.clone(),
            reader: Some(ConnectionReader {
                _context: self.context.clone(),
                calls: self.calls.clone(),
            }),
            writer: Some(ConnectionWriter {
                context: self.context.clone(),
                input_events: receiver,
                _protocol: connection_id.to_string(),
            }),
            protocol: connection_id.to_string(),
        };
        
        get_runtime().spawn(async move {
            conn_loop.run().await
        });
        
        Ok(Connection {
            calls: self.calls.clone(),
            sender
        })
    }
}

impl BasicRpcClient {
    pub fn call<Request, Response>(&self, header: RequestHeaderProto, body: Request)
        -> Result<Response>
    where Request: Message,
          Response: Message
    {
        let mut conn_map = match self.connections.lock() {
            Ok(conn_map) => conn_map,
            Err(e) => {
                error!("Wrong status of rpc client, we should close this: {:?}", e);
                return Err(HdfsLibErrorKind::LockError.into())
            }
        };
        let conn_id = header.get_declaringClassProtocolName();
        
        if !conn_map.contains_key(conn_id) {
            let conn = self.create_and_start_connection(conn_id)?;
            conn_map.insert(conn_id.to_string(), conn);
        }
        
        (&conn_map[conn_id]).call(header, body)
    }

//    pub fn close(&self) -> Result<()> {
//        unimplemented!()
//    }
}


impl Connection {
    pub fn call<Request, Response>(&self, header: RequestHeaderProto, body: Request)
                                   -> Result<Response>
        where Request: Message,
              Response: Message
    {
        let call_id = CALL_ID.fetch_add(1, Ordering::SeqCst);
        let (sender, receiver) = mpsc_channel();

        let rpc_call = RpcCall {
            id: call_id,
            request_header: header,
            request_body: Box::new(body) as Box<dyn Message>,
        };
        
        {
            let mut calls = self.calls.lock().expect("Failed to lock calls");
            calls.insert(call_id, sender);
        }
        

        let event = Event::Call(rpc_call);
        let mut event_sender = self.sender.clone();
        get_runtime().spawn(async move {
            event_sender.send(event).await
        });

        let timeout = Duration::from_secs(10);
        receiver.recv_timeout(timeout).context(HdfsLibErrorKind::TimeOutError(timeout))?
            .and_then(|resp| {
                let mut reader = resp.body.into_buf().reader();
                let mut input_stream = CodedInputStream::new(&mut reader);
                let header = deserialize::<RpcResponseHeaderProto>(
                    &mut input_stream)?;
    
                match header.get_status() {
                    RpcResponseHeaderProto_RpcStatusProto::SUCCESS => {
                        Ok(deserialize::<Response>(&mut input_stream)?)
                    },
                    _ => {
                        Err(HdfsLibErrorKind::RpcRemoteError(RpcRemoteErrorInfo::from(&header)).into())
                    }
                }
            })
    }
}


impl ConnectionReader {
    async fn run<T: AsyncRead>(self, mut reader: ReadHalf<T>) -> Result<()> {
        loop {
            let response_length = reader.read_i32().await
                .context(IoError)? as usize;
            let mut buffer = BytesMut::new();
            buffer.resize(response_length, 0);
            debug!("Response length is: {}", response_length);
    
            reader.read_exact(buffer.as_mut()).await.context(IoError)?;
    
            let buffer = buffer.freeze();
            let mut input_stream = CodedInputStream::from_bytes(buffer.as_ref());
            let header = deserialize::<RpcResponseHeaderProto>(
                &mut input_stream).expect("Failed to parse header");
    
            let call_id = header.get_callId() as i32;
    
            self.calls.lock().expect("Failed to lock calls").get(&call_id)
                .expect(format!("Failed to find sender of call id: {}", call_id).as_str())
                .send(Ok(Response  { body: buffer.clone()}))
                .expect("Failed to send");
        }
    }
}

impl ConnectionWriter {
    async fn run<T: AsyncWrite>(mut self, mut w: WriteHalf<T>) -> Result<()> {
        loop {
            if let Some(event) = self.input_events.recv().await {
                match event {
                    Event::Call(rpc_call) => {
                        // Send request
                        let rpc_request_header = self.make_rpc_request_header(rpc_call.id, 0,
                                                                              RpcRequestHeaderProto_OperationProto::RPC_FINAL_PACKET);
                    
                        let messages = [&rpc_request_header as &dyn Message,
                            &rpc_call.request_header as &dyn Message, rpc_call.request_body.as_ref()];
                    
                        let request = Messages::new(&messages);
                    
                        let serialized_len = request.get_serialized_len()?;
                        let mut buffer = BytesMut::with_capacity(4 + serialized_len);
                        buffer.put_i32_be(serialized_len as i32);
                    
                        let mut writer = buffer.writer();
                        request.serialize(&mut writer)?;
                    
                        let buffer = writer.into_inner();
                        w.write_all(buffer.as_ref())
                            .await
                            .context(IoError)?;
                    },
                    Event::Stop => return Ok(())
                }
            }
        }
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
        rpc_request_header.set_clientId(self.context.client_id.to_vec());
        rpc_request_header.set_retryCount(retry_count);
        
        rpc_request_header
    }
}

impl ConnectionLoop {
    async fn write_connection_header<W: AsyncWriteExt + Unpin>(&mut self, writer: &mut W)
        -> Result<()> {
        writer.write_all(RPC_HEADER.as_bytes())
            .await
            .context(IoError)?;
        writer.write_i8(RPC_CURRENT_VERSION as i8)
            .await
            .context(IoError)?;
        
        let service_class = self.context.service_class;
        writer.write_i8(service_class as i8)
            .await
            .context(IoError)?;
        let call_id = self.context.auth_protocol.call_id();
        writer.write_i8(call_id as i8)
            .await
            .context(IoError)?;

        Ok(())
    }

    async fn write_connection_context<W: AsyncWriteExt + Unpin>(&mut self, w: &mut W) -> Result<()> {
        let body = self.make_ipc_connection_context();
        let header = self.make_rpc_request_header(
            RPC_CONNECTION_CONTEXT_CALL_ID,
            RPC_INVALID_RETRY_COUNT,
            RpcRequestHeaderProto_OperationProto::RPC_FINAL_PACKET);
        
        let messages = [&header as &dyn Message, &body as &dyn Message];
        let request = Messages::new(&messages);
        let serialized_len = request.get_serialized_len()?;
        
        let mut buffer = BytesMut::with_capacity(4 + serialized_len);
        buffer.put_i32_be(serialized_len as i32);
    
        let mut writer = buffer.writer();
        request.serialize(&mut writer)?;
        
        let buffer = writer.into_inner();
        
        w.write_all(buffer.as_ref())
            .await
            .context(IoError)?;
        
        Ok(())
    }
    
    async fn build_tcp_stream(&mut self) -> Result<TcpStream> {
        let tcp_stream = TcpStream::connect(self.context.endpoint).await.context(IoError)?;
        
        tcp_stream.set_nodelay(true).context(IoError)?;
        tcp_stream.set_keepalive(None).context(IoError)?;
        
        Ok(tcp_stream)
    }
    
    async fn run(mut self) {
        let mut tcp_stream = match self.build_tcp_stream().await  {
            Ok(t) => t,
            Err(e) => {
                error!("Failed to connect to remote sever: {}", e);
                return;
            }
        };
        
        if let Err(e) = self.write_connection_header(&mut tcp_stream).await {
            error!("Failed to write connection header: {}", e);
            return;
        }
        
        if let Err(e) = self.write_connection_context(&mut tcp_stream).await {
            error!("Failed to write connection context: {}", e);
            return;
        }
        
        
        let (reader, writer) = split(tcp_stream);
        let conn_reader = std::mem::replace(&mut self.reader, None).unwrap();
        let conn_writer = std::mem::replace(&mut self.writer, None).unwrap();
        
        get_runtime().spawn(async move {
            conn_reader.run(reader).await.expect("Reader failed")
        });
        
        get_runtime().spawn(async move {
            conn_writer.run(writer).await.expect("Writer failed")
        });
        
//        loop {
//            if let Some(event) = self.input_events.recv().await {
//                match event {
//                    Event::Call(rpc_call) => {
//                        if let Err(e) = self.process_call(rpc_call).await {
//                            error!("Error when processing rpc call: {}", e);
//                            break;
//                        }
//                    },
//                    Event::Stop => break,
//                }
//            }
//        }
    }
    
    
//    async fn process_call(&mut self, rpc_call: RpcCall) -> Result<()> {
//        assert!(self.tcp_stream.is_some());
//        // Send request
//        let rpc_request_header = self.make_rpc_request_header(rpc_call.id, 0,
//                                                              RpcRequestHeaderProto_OperationProto::RPC_FINAL_PACKET);
//
//        let messages = [&rpc_request_header as &dyn Message,
//            &rpc_call.request_header as &dyn Message, rpc_call.request_body.as_ref()];
//
//        let request = Messages::new(&messages);
//
//        let serialized_len = request.get_serialized_len()?;
//        let mut buffer = BytesMut::with_capacity(4 + serialized_len);
//        buffer.put_i32_be(serialized_len as i32);
//
//        let mut writer = buffer.writer();
//        request.serialize(&mut writer)?;
//
//        let buffer = writer.into_inner();
//        self.stream().write_all(buffer.as_ref())
//            .await
//            .context(IoError)?;
//
//        // Responses
//        let response_length = self.stream().read_i32().await
//            .context(IoError)? as usize;
//        let mut buffer = BytesMut::new();
//        buffer.resize(response_length, 0);
//        debug!("Response length is: {}", response_length);
//
//        self.stream().read_exact(buffer.as_mut()).await.context(IoError)?;
//
//        rpc_call.response_sender
//            .send(Ok(Response  { body: buffer.freeze()}))
//            .context(HdfsLibErrorKind::IoError)?;
//        Ok(())
//    }
//

    fn make_ipc_connection_context(&self) -> IpcConnectionContextProto {
        let mut user_info_proto = UserInformationProto::new();
        user_info_proto.set_effectiveUser("renliu".to_string());
//        user_info_proto.set_realUser("renliu".to_string());

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
        let mut rpc_request_header = RpcRequestHeaderProto::new();
        rpc_request_header.set_rpcKind(RpcKindProto::RPC_PROTOCOL_BUFFER);
        rpc_request_header.set_rpcOp(rpc_op);
        rpc_request_header.set_callId(call_id);
        rpc_request_header.set_clientId(self.context.client_id.to_vec());
        rpc_request_header.set_retryCount(retry_count);

        rpc_request_header
    }
    
//    fn stream(&mut self) -> &mut TcpStream {
//        assert!(self.tcp_stream.is_some());
//        self.tcp_stream.as_mut().unwrap()
//    }
}

// async fn process_loop<A: ToSocketAddrs>(
//     data: Arc<RpcClientInner>,
//     server_addr: A,
//     init_result_sender: SpmcSender<EventLoopState>,
// ) {
//     match do_process_loop(data, server_addr).await {
//         Ok(_) => init_result_sender.broadcast(EventLoopState::Exited),
//         Err(e) => init_result_sender.broadcast(EventLoopState::Failed(e.kind().clone())),
//     };
// }

// async fn do_process_loop<A: ToSocketAddrs>(
//     rpc_client_inner: Arc<RpcClientInner>,
//     server_addr: A,
// ) -> Result<()> {
//     // TODO: Add        retry logic here
//     //    let tcp_stream = rpc_client_inner.build_tcp_stream(server_addr).await?;
//     unimplemented!()
// }

//#[cfg(test)]
//mod tests {
//    use bytes::BytesMut;
//    use std::net::{SocketAddr, ToSocketAddrs};
//    use std::str::FromStr;
//
//    #[test]
//    fn test_bytes() {
//        let mut b = BytesMut::new();
//        b.resize(64, 0);
//        println!("Size of b is: {}", b.as_mut().len());
//    }
//
//    #[test]
//    fn test_socket_addr() {
//        let socket_addr = "hadoop-docker-build-3648195.lvs02.dev.ebayc3.com:9000".to_socket_addrs();
//        assert!(socket_addr.is_ok());
//        assert_eq!(9000, socket_addr.unwrap().next().unwrap().port());
//    }
//}
