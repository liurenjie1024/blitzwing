use gsasl_rs::client::SaslClient;
use crate::hadoop_proto::RpcHeader::RpcSaslProto_SaslAuth;
use crate::rpc::message::receive_rpc_response;
use crate::rpc::message::send_rpc_request;
use tokio::io::AsyncReadExt;
use protobuf::Message;
use crate::rpc::auth::AuthMethod;
use tokio::io::AsyncWriteExt;
use crate::hadoop_proto::RpcHeader::RpcRequestHeaderProto;
use crate::rpc::constants::RPC_INVALID_RETRY_COUNT;
use crate::rpc::auth::AuthProtocol::Sasl;
use crate::hadoop_proto::RpcHeader::RpcRequestHeaderProto_OperationProto::RPC_FINAL_PACKET;
use crate::rpc::message::make_rpc_request_header;
use tokio::io::{AsyncRead, AsyncWrite};
use crate::hadoop_proto::RpcHeader::{RpcSaslProto, RpcSaslProto_SaslState};
use crate::error::Result;

lazy_static! {
  static ref SASL_HEADER: RpcRequestHeaderProto = make_rpc_request_header(Sasl.call_id(), RPC_INVALID_RETRY_COUNT, RPC_FINAL_PACKET, "");
}
pub(crate) struct SaslProtocol {
}

pub(crate) struct SaslRpcClient {
}

impl SaslProtocol {
  pub(crate) async fn sasl_connect<In, Out>(self, mut input: In, mut output: Out) -> Result<SaslRpcClient> 
  where In: AsyncReadExt + Unpin,
        Out: AsyncWriteExt + Unpin
  {
    let negotiate_request = {
      let mut r = RpcSaslProto::new();
      r.set_state(RpcSaslProto_SaslState::NEGOTIATE);
      r
    };

    let request_body: RpcSaslProto = negotiate_request;

    loop {
      send_rpc_request(&mut output, &SASL_HEADER, &request_body).await?;
      let response = receive_rpc_response(&mut input).await?
      .get_message::<RpcSaslProto>()?;
      debug!("Received sasl respons: {:?}", response);

      match response.get_state() {
        RpcSaslProto_SaslState::NEGOTIATE => {
        },
        RpcSaslProto_SaslState::SUCCESS => {

        },
        RpcSaslProto_SaslState::CHALLENGE => {

        },
        s => check_protocol_content!(false, "Sasl client does not support sasl state: {:?}", s)
      }
    }

    unimplemented!()
  }

  fn select_sasl_auth(&self, auth_types: &[RpcSaslProto_SaslAuth]) -> Result<(&RpcSaslProto_SaslAuth, Option<SaslClient>)> {
    let sasl_client_ret = None;
    let auth_type_ret = 

    for auth_type in auth_types {
      match AuthMethod::value_of(auth_type.get_method()) {
        Ok(auth_method) => {
          if auth_method.mechanism_name() != auth_type.get_mechanism() {
            warn!("Auth method {:?} mechanism not match in server[{}] and client[{}]", auth_method, auth_type.get_mechanism(), auth_method.mechanism_name());
            continue;
          }

          if auth_method == AuthMethod::Simple {
            break;
          }

          if let Ok(sasl_client) = self.create_sasl_client(auth_type) {
            sasl_client_ret = Some(sasl_client);
            break;
          } else {
            info!("Failed to create sasl client for auth type: {:?}, will try next.", auth_type);
          }
        },
        Err(e) => info!("Failed to find auth method: {}, will try next.", e)
      } 
   }
    unimplemented!()
  }

  fn create_sasl_client(&self, auth_type: &RpcSaslProto_SaslAuth) -> Result<SaslClient> {
    unimplemented!()
  }
}