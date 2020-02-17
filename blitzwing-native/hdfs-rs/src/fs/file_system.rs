use crate::{
  config::ConfigRef,
  error::Result,
  fs::{file_status::FileStatus, input_stream::FsInputStreamRef},
  hdfs::distributed_file_system::DFSBuilder,
};
use std::sync::Arc;

pub type FileSystemRef = Arc<dyn FileSystem>;

pub trait FileSystem {
  fn get_file_status(&self, path: &str) -> Result<FileStatus>;
  fn open(&self, path: &str) -> Result<FsInputStreamRef>;
}

// Methods for creating file systems
pub fn make_file_system(fs_path: &str, config: ConfigRef) -> Result<FileSystemRef> {
  DFSBuilder::new(fs_path, config).build()
}
