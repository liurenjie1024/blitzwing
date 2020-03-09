use gsasl_rs::client::SaslClient;
use crate::hadoop_proto::RpcHeader::RpcSaslProto_SaslAuth;
use crate::rpc::message::receive_rpc_response;
use crate::rpc::message::send_rpc_request;
use tokio::io::AsyncReadExt;
use protobuf::Message;
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

  fn select_sasl_client(&self, auth_types: &[RpcSaslProto_SaslAuth]) -> Result<(&RpcSaslProto_SaslAuth, Option<SaslClient>)> {
    unimplemented!()
  }
}