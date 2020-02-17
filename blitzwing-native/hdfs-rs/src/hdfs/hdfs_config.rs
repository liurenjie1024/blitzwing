use crate::{config::ConfigRef, error::Result};

use std::sync::Arc;

pub(in crate::hdfs) type HdfsClientConfigRef = Arc<HdfsClientConfig>;

const DFS_BLOCK_SIZE_KEY: &'static str = "dfs.block.size";
const DFS_BLOCK_SIZE_DEFAULT: usize = 128 * 1024 * 1024;

const DFS_CLIENT_READ_PREFETCH_SIZE_KEY: &'static str = "dfs.client.read.prefetch.size";

const DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY: &'static str =
  "dfs.client.max.block.acquire.failures";
const DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT: u32 = 3;

const DFS_CLIENT_SOCKET_TIMEOUT_KEY: &'static str = "dfs.client.socket-timeout";
// Socket timeout in milliseconds
const DFS_CLIENT_SOCKET_TIMEOUT_DEFAULT: u64 = 60 * 1000;

const DFS_CLIENT_USE_DN_HOSTNAME: &'static str = "dfs.client.use.datanode.hostname";
const DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT: bool = false;

#[derive(Default, CopyGetters)]
pub(in crate::hdfs) struct HdfsClientConfig {
  inner: ConfigRef,

  #[get_copy = "pub"]
  default_block_size: usize,
  #[get_copy = "pub"]
  prefetch_size: usize,
  #[get_copy = "pub"]
  max_block_acquire_failures: u32,
  #[get_copy = "pub"]
  socket_timeout: u64,
  #[get_copy = "pub"]
  connect_dn_via_hostname: bool,
}

impl HdfsClientConfig {
  pub(in crate::hdfs) fn new(inner: ConfigRef) -> Result<HdfsClientConfigRef> {
    let default_block_size = inner.get_or(DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_DEFAULT)?;
    let prefetch_size = inner.get_or(DFS_CLIENT_READ_PREFETCH_SIZE_KEY, 10 * default_block_size)?;
    let max_block_acquire_failures = inner.get_or(
      DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY,
      DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT,
    )?;

    let socket_timeout =
      inner.get_or(DFS_CLIENT_SOCKET_TIMEOUT_KEY, DFS_CLIENT_SOCKET_TIMEOUT_DEFAULT)?;

    let connect_dn_via_hostname =
      inner.get_or(DFS_CLIENT_USE_DN_HOSTNAME, DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT)?;
    Ok(Arc::new(Self {
      inner,
      default_block_size,
      prefetch_size,
      max_block_acquire_failures,
      socket_timeout,
      connect_dn_via_hostname,
    }))
  }
}
