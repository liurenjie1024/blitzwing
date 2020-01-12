use crate::error::Result;
use crate::fs::file_status::FileStatus;
use crate::config::ConfigRef;
use std::sync::Arc;
use crate::hdfs::distributed_file_system::DFSBuilder;

pub type FileSystemRef = Arc<dyn FileSystem>;

pub trait FileSystem {
    fn get_file_status(&self, path: &str) -> Result<FileStatus>;
}

// Methods for creating file systems
pub fn make_file_system(fs_path: &str, config: ConfigRef) -> Result<FileSystemRef> {
    DFSBuilder::new(fs_path, config)
        .build()
}
