use crate::error::Result;
use protobuf::Message;
use crate::hadoop_proto::ProtobufRpcEngine::RequestHeaderProto;
use crate::rpc::basic_rpc_client::{BasicRpcClient, BasicRpcClientBuilder};
use std::sync::{Mutex, Arc};
use std::collections::HashMap;
use crate::config::ConfigRef;
use bytes::Bytes;
use crate::error::HdfsLibErrorKind::LockError;
use uuid::Uuid;

pub type RpcClientRef = Arc<RpcClient>;

lazy_static! {
    static ref RPC_CLIENT_REGISTRY: Mutex<HashMap<String, RpcClientRef>> = Mutex::new(HashMap::new
    ());
}

pub struct RpcClient {
    inner: BasicRpcClient
}

impl RpcClient {
    pub fn call<Request: Message, Response: Message>(
        &self,
        header: RequestHeaderProto,
        body:   Request,
    ) -> Result<Response> {
        self.inner.call(header, body)
    }
    
//    fn close(&self) -> Result<()> {
//        self.inner.close()
//    }
}

pub struct RpcClientBuilder<'a> {
    authority: &'a str,
    config: ConfigRef
}

impl<'a> RpcClientBuilder<'a> {
    pub fn new(authority: &'a str, config: ConfigRef) -> Self {
        Self {
            authority,
            config
        }
    }
    
    pub fn build(self) -> Result<RpcClientRef> {
        match RPC_CLIENT_REGISTRY.lock() {
            Ok(mut map) => {
                if !map.contains_key(self.authority)  {
                    map.insert(self.authority.to_string(), self.create_rpc_client()?);
                }
                
                Ok(map[self.authority].clone())
            },
            Err(e) => {
                error!("Rpc client mutex lock error: {}", e);
                Err(LockError.into())
            }
        }
    }
    
    fn create_rpc_client(&self) -> Result<RpcClientRef> {
        let client_id = Bytes::from(Uuid::new_v4().as_bytes() as &[u8]) ;
        
        let inner = BasicRpcClientBuilder::new(self.authority, client_id.clone(),
                                               self.config.clone())
            .build()?;
        
        Ok(Arc::new(RpcClient { inner }))
    }
}



