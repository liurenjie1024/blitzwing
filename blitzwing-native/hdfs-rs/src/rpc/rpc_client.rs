use crate::{
  config::ConfigRef,
  error::{HdfsLibErrorKind::LockError, Result},
  hadoop_proto::ProtobufRpcEngine::RequestHeaderProto,
  rpc::{
    basic_rpc_client::{BasicRpcClient, BasicRpcClientBuilder},
    user::SubjectRef,
  },
};
use bytes::Bytes;
use protobuf::Message;
use std::{
  collections::HashMap,
  sync::{Arc, Mutex},
};
use uuid::Uuid;

pub type RpcClientRef = Arc<RpcClient>;

lazy_static! {
  static ref RPC_CLIENT_REGISTRY: Mutex<HashMap<String, RpcClientRef>> = Mutex::new(HashMap::new());
}

pub struct RpcClient {
  inner: BasicRpcClient,
}

impl RpcClient {
  pub fn call<Request: Message, Response: Message>(
    &self,
    header: RequestHeaderProto,
    body: Request,
  ) -> Result<Response> {
    self.inner.call(header, body)
  }

  //    fn close(&self) -> Result<()> {
  //        self.inner.close()
  //    }
}

pub struct RpcClientBuilder<'a> {
  authority: &'a str,
  config: ConfigRef,
  user: SubjectRef,
}

impl<'a> RpcClientBuilder<'a> {
  pub fn new(authority: &'a str, config: ConfigRef, user: SubjectRef) -> Self {
    Self { authority, config, user }
  }

  pub fn build(self) -> Result<RpcClientRef> {
    match RPC_CLIENT_REGISTRY.lock() {
      Ok(mut map) => {
        if !map.contains_key(self.authority) {
          map.insert(self.authority.to_string(), self.create_rpc_client()?);
        }

        Ok(map[self.authority].clone())
      }
      Err(e) => {
        error!("Rpc client mutex lock error: {}", e);
        Err(LockError.into())
      }
    }
  }

  fn create_rpc_client(&self) -> Result<RpcClientRef> {
    let client_uuid = Uuid::new_v4();
    let client_id = Bytes::copy_from_slice(client_uuid.as_bytes() as &[u8]);

    let inner = BasicRpcClientBuilder::new(
      self.authority,
      client_id.clone(),
      self.config.clone(),
      self.user.clone(),
    )
    .build()?;

    Ok(Arc::new(RpcClient { inner }))
  }
}
