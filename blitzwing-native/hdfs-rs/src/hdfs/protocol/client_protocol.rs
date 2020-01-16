use crate::error::Result;
use crate::fs::file_status::{BuildArgs as FileStatusBuilderArgs, FileStatus};
use crate::fs::path::FsPathRef;
use crate::hadoop_proto::ClientNamenodeProtocol::{
    GetFileInfoRequestProto, GetFileInfoResponseProto,
};
use crate::hadoop_proto::ProtobufRpcEngine::RequestHeaderProto;
use crate::rpc::rpc_client::RpcClientRef;
use std::convert::TryFrom;

pub trait ClientProtocol {
    fn get_file_info(&self, path: &str) -> Result<FileStatus>;
//    fn get_block_locations(&self, path: &str, offset: i64, length: i64) ->
}

const PROTOCOL_NAME: &'static str = "org.apache.hadoop.hdfs.protocol.ClientProtocol";
const PROTOCOL_VERSION: i32 = 1;

pub struct RpcClientProtocol {
    base_uri: FsPathRef,
    rpc_client: RpcClientRef,
}

impl ClientProtocol for RpcClientProtocol {
    fn get_file_info(&self, path: &str) -> Result<FileStatus> {
        let header = RpcClientProtocol::create_request_header("getFileInfo");
        let mut body = GetFileInfoRequestProto::new();
        body.set_src(path.to_string());

        self.rpc_client
            .call::<GetFileInfoRequestProto, GetFileInfoResponseProto>(header, body)
            .and_then(|resp| {
                let builder =
                    FileStatusBuilderArgs::new(resp.get_fs(), self.base_uri.as_ref(), path);
                FileStatus::try_from(builder)
            })
    }
}

impl RpcClientProtocol {
    pub fn new(base_uri: FsPathRef, rpc_client: RpcClientRef) -> Self {
        Self {
            base_uri,
            rpc_client,
        }
    }

    fn create_request_header(method_name: &str) -> RequestHeaderProto {
        let mut header = RequestHeaderProto::new();
        header.set_declaringClassProtocolName(PROTOCOL_NAME.to_string());
        header.set_clientProtocolVersion(PROTOCOL_VERSION as u64);
        header.set_methodName(method_name.to_string());
        header
    }
}
