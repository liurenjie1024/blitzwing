use crate::{
  config::ConfigRef, error::Result, fs::file_status::FileStatus,
  hdfs::distributed_file_system::DFSBuilder,
};
use std::sync::Arc;

pub type FileSystemRef = Arc<dyn FileSystem>;

pub trait FileSystem {
  fn get_file_status(&self, path: &str) -> Result<FileStatus>;
}

// Methods for creating file systems
pub fn make_file_system(fs_path: &str, config: ConfigRef) -> Result<FileSystemRef> {
  DFSBuilder::new(fs_path, config).build()
}
