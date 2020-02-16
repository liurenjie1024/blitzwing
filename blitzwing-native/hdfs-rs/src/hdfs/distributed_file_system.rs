use crate::{
  config::ConfigRef,
  error::{HdfsLibErrorKind::InvalidArgumentError, Result},
  fs::{
    file_status::FileStatus,
    file_system::{FileSystem, FileSystemRef},
    path::{FsPath, FsPathRef},
  },
  hdfs::protocol::client_protocol::{ClientProtocol, RpcClientProtocol},
  rpc::rpc_client::RpcClientBuilder,
};
use std::{convert::TryFrom, sync::Arc};

const DFS_SCHEMA: &'static str = "hdfs";

pub struct DistributedFileSystem {
  _base_uri: FsPathRef,
  name_node: Arc<dyn ClientProtocol>,
}

impl FileSystem for DistributedFileSystem {
  fn get_file_status(&self, path: &str) -> Result<FileStatus> {
    self.name_node.get_file_info(path)
  }
}

pub struct DFSBuilder<'a> {
  path: &'a str,
  config: ConfigRef,
}

impl<'a> DFSBuilder<'a> {
  pub fn new(path: &'a str, config: ConfigRef) -> Self {
    Self { path, config }
  }

  pub fn build(self) -> Result<FileSystemRef> {
    let path = FsPath::try_from(self.path)?;
    if path.scheme() != DFS_SCHEMA {
      return Err(
        InvalidArgumentError(format!("[{}]'s schema is not {}", self.path, DFS_SCHEMA)).into(),
      );
    }

    // TODO: Check authority

    let base_uri = Arc::new(path.base()?);
    let rpc_client_ref =
      RpcClientBuilder::new(&base_uri.authority(), self.config.clone()).build()?;

    let name_node = Arc::new(RpcClientProtocol::new(base_uri.clone(), rpc_client_ref.clone()));

    Ok(Arc::new(DistributedFileSystem { _base_uri: base_uri, name_node }))
  }
}
