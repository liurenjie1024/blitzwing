use crate::error::HdfsLibErrorKind::SaslError;
use crate::rpc::user::SubjectRef;
use gsasl_rs::client::SaslClient;
use crate::hadoop_proto::RpcHeader::RpcSaslProto_SaslAuth;
use crate::rpc::message::receive_rpc_response;
use crate::rpc::message::send_rpc_request;
use tokio::io::AsyncReadExt;
use crate::rpc::auth::AuthMethod;
use tokio::io::AsyncWriteExt;
use crate::hadoop_proto::RpcHeader::RpcRequestHeaderProto;
use crate::rpc::constants::RPC_INVALID_RETRY_COUNT;
use crate::rpc::auth::AuthProtocol::Sasl;
use crate::hadoop_proto::RpcHeader::RpcRequestHeaderProto_OperationProto::RPC_FINAL_PACKET;
use crate::rpc::message::make_rpc_request_header;
use crate::hadoop_proto::RpcHeader::{RpcSaslProto, RpcSaslProto_SaslState};
use crate::error::Result;
use failure::ResultExt;
use gsasl_rs::client::GssApiInfo;

lazy_static! {
  static ref SASL_HEADER: RpcRequestHeaderProto = make_rpc_request_header(Sasl.call_id(), RPC_INVALID_RETRY_COUNT, RPC_FINAL_PACKET, "");
}
pub(crate) struct SaslProtocol {
  rpc_protocol: String,
  subject: SubjectRef,
}

pub(crate) struct SaslRpcClient {
  subject: SubjectRef,
  sasl_client: Option<SaslClient>,
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

    let mut request_body: RpcSaslProto = negotiate_request;
    let mut sasl_client_ret = None;

    loop {
      send_rpc_request(&mut output, &SASL_HEADER, &request_body).await?;
      let response = receive_rpc_response(&mut input).await?
      .get_message::<RpcSaslProto>()?;
      debug!("Received sasl respons: {:?}", response);

      match response.get_state() {
        RpcSaslProto_SaslState::NEGOTIATE => {
          let (auth_type, sasl_client_opt) = self.select_sasl_auth(response.get_auths())?;

          if let Some(mut sasl_client) = sasl_client_opt {
            let reply_token= if auth_type.has_challenge() {
              // Server provide challenge first
              sasl_client.evaluate(auth_type.get_challenge())
                .context(SaslError)?
            } else if sasl_client.sends_data_first() {
              // Client sends data first
              sasl_client.evaluate(&[])
                .context(SaslError)?
            } else {
              Vec::new()
            };

            let mut sasl_auth = auth_type.clone();
            sasl_auth.clear_challenge();
            request_body = SaslProtocol::create_sasl_reply(sasl_auth, RpcSaslProto_SaslState::INITIATE, reply_token);
            sasl_client_ret = Some(sasl_client);
          } else {
            // Simple auth, return
            sasl_client_ret = None;
            break;
          }
        },
        RpcSaslProto_SaslState::CHALLENGE => {
          if let Some(ref mut sasl_client) = sasl_client_ret {
            check_protocol_content!(response.has_token(), "Sasl server sent challenge response without token: {:?}", response);
            let reply_token = sasl_client.evaluate(response.get_token()).context(SaslError)?;

            request_body = SaslProtocol::create_sasl_reply(RpcSaslProto_SaslAuth::new(), RpcSaslProto_SaslState::RESPONSE, reply_token);
          } else {
            invalid_state!("Server sent unsolicited challenge: {:?}", &response);
          }
        },
        RpcSaslProto_SaslState::SUCCESS => {
          if let Some(ref mut sasl_client) = sasl_client_ret {
            if response.has_token() {
              let reply_token = sasl_client.evaluate(response.get_token()).context(SaslError)?;
              check_state!(reply_token.len() == 0, "{}", "Sasl client generated spurious response");
            }

            check_state!(sasl_client.is_complete(), "{}", "Sasl client is not sync with server.");
          }
        },
        s => check_protocol_content!(false, "Sasl client does not support sasl state: {:?}", s)
      }
    }

    Ok(SaslRpcClient {
      subject: self.subject.clone(),
      sasl_client: sasl_client_ret
    })
  }

  fn create_sasl_reply(auth_type: RpcSaslProto_SaslAuth, state: RpcSaslProto_SaslState, response_token: Vec<u8>) -> RpcSaslProto {
    let mut reply = RpcSaslProto::new();
    reply.set_state(state);
    reply.set_token(response_token);
    reply.mut_auths().push(auth_type);
    reply
  }

  fn select_sasl_auth<'a>(&self, auth_types: &'a [RpcSaslProto_SaslAuth]) -> Result<(&'a RpcSaslProto_SaslAuth, Option<SaslClient>)> {
    let mut sasl_client_ret = None;
    let mut auth_type_ret = None;

    for auth_type in auth_types {
      match AuthMethod::value_of(auth_type.get_method()) {
        Ok(auth_method) => {
          if auth_method.mechanism_name() != auth_type.get_mechanism() {
            warn!("Auth method {:?} mechanism not match in server[{}] and client[{}]", auth_method, auth_type.get_mechanism(), auth_method.mechanism_name());
            continue;
          }

          if auth_method == AuthMethod::Simple {
            auth_type_ret = Some(auth_type);
            break;
          }

          match self.create_sasl_client(auth_type) {
            Ok(Some(sasl_client)) => {
              sasl_client_ret = Some(sasl_client);
              auth_type_ret = Some(auth_type);
              break;
            },
            Ok(None) => {
              info!("Can't create sasl client for auth type {:?}, will try next auth type!", auth_type);
            }
            Err(e) => {
              info!("Failed to create sasl client for auth type {:?}: {}, will try next auth type!", auth_type, e);
            }
          }
        },
        Err(e) => info!("Failed to find auth method: {}, will try next.", e)
      } 
   }

   check_state!(auth_type_ret.is_some(), "Client can't authenticaet via {:?}", auth_types);

   debug!("Choose {:?} for rpc protocol {}", auth_type_ret, &self.rpc_protocol);

   Ok((auth_type_ret.expect("Auth type should not be none!"), sasl_client_ret))
  }

  fn create_sasl_client(&self, auth_type: &RpcSaslProto_SaslAuth) -> Result<Option<SaslClient>> {
    let auth_method = AuthMethod::value_of(auth_type.get_method())?;
    if !self.subject.match_auth_method(auth_method) {
      return Ok(None);
    }

    match auth_method {
      AuthMethod::Kerberos => {
        self.create_gssapi_sasl_client(auth_type).map(|c| Some(c))
      },
      _ => Ok(None)
    }
  }

  fn create_gssapi_sasl_client(&self, auth_type: &RpcSaslProto_SaslAuth) -> Result<SaslClient> {
    let gss_api_info = GssApiInfo::new(self.subject.user().fullname().clone(), auth_type.get_protocol().to_string(), auth_type.get_serverId().to_string());

    Ok(SaslClient::use_gss_api(gss_api_info)
      .context(SaslError)?)
  }
}

impl SaslRpcClient {
  pub(crate) fn is_simple(&self) -> bool {
    self.sasl_client.is_none()
  }
}

