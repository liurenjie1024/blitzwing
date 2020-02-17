use crate::{
  config::ConfigRef,
  error::{HdfsLibErrorKind::InvalidArgumentError, Result},
  fs::{
    file_status::FileStatus,
    file_system::{FileSystem, FileSystemRef},
    input_stream::FsInputStreamRef,
    path::{FsPath, FsPathRef},
  },
  hdfs::{
    hdfs_config::{HdfsClientConfig, HdfsClientConfigRef},
    protocol::client_protocol::{ClientProtocolRef, RpcClientProtocol},
    transfer::{block_io::RemoteBlockReaderFactory, dfs_input_stream::DFSInputStream},
  },
  rpc::rpc_client::RpcClientBuilder,
};
use std::{convert::TryFrom, sync::Arc};

const DFS_SCHEMA: &'static str = "hdfs";

pub struct DistributedFileSystem {
  _base_uri: FsPathRef,
  namenode: ClientProtocolRef,
  config: HdfsClientConfigRef,
}

impl FileSystem for DistributedFileSystem {
  fn get_file_status(&self, path: &str) -> Result<FileStatus> {
    self.namenode.get_file_info(path)
  }

  fn open(&self, path: &str) -> Result<FsInputStreamRef> {
    let block_reader_factory = Box::new(RemoteBlockReaderFactory::new(self.config.clone()));
    Ok(Box::new(DFSInputStream::new(
      self.config.clone(),
      self.namenode.clone(),
      block_reader_factory,
      path,
    )?) as FsInputStreamRef)
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

    let namenode = Arc::new(RpcClientProtocol::new(base_uri.clone(), rpc_client_ref.clone()));
    let hdfs_config = HdfsClientConfig::new(self.config.clone())?;

    Ok(Arc::new(DistributedFileSystem { _base_uri: base_uri, namenode, config: hdfs_config }))
  }
}
