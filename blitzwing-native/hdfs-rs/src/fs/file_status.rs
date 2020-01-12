use crate::fs::path::{FsPath, FsPathBuilder};
use crate::hadoop_proto::hdfs::{HdfsFileStatusProto, HdfsFileStatusProto_FileType};
use crate::error::{HdfsLibError, Result};
use std::convert::TryFrom;


#[derive(Debug, Clone)]
pub enum FileStatus {
    File(FileInfo),
    Dir(DirInfo),
}

#[derive(Debug, Clone)]
pub struct FileStat {
    modification_time: i64,
    access_time: u64,
    owner: String,
    group: String,
}

#[derive(Debug, Clone)]
pub struct FileInfo {
    path: FsPath,
    length: u64,
    block_replication: u16,
    block_size: u64,
    file_stat: FileStat,
    // TODO: fs permissions
}

#[derive(Debug, Clone)]
pub struct DirInfo {
    path: FsPath,
    file_stat: FileStat
}

#[derive(Debug)]
pub struct BuildArgs<'a> {
    proto: &'a HdfsFileStatusProto,
    base_uri: &'a FsPath,
    parent_path: &'a str,
}

impl<'a> BuildArgs<'a> {
    pub fn new(proto: &'a HdfsFileStatusProto,
               base_uri: &'a FsPath,
               parent_path: &'a str) -> Self {
        Self {
            proto,
            base_uri,
            parent_path
        }
    }
    
    fn build_full_path(&self) -> Result<FsPath> {
        FsPathBuilder::new(self.base_uri.as_str())
            .and_then(|b| b.append(self.parent_path))
            .and_then(|b| b.append(self.proto.get_path()))
            .and_then(|b| b.build())
    }
}


impl<'a> TryFrom<BuildArgs<'a>> for FileStatus {
    type Error = HdfsLibError;
    
    fn try_from(args: BuildArgs<'a>) -> Result<Self> {
        debug!("file status: {:?}", args);
        Ok(match args.proto.get_fileType() {
            HdfsFileStatusProto_FileType::IS_FILE => FileStatus::Dir(DirInfo::try_from(args)?),
            HdfsFileStatusProto_FileType::IS_DIR => FileStatus::File(FileInfo::try_from(args)?),
            _ => unimplemented!()
        })
    }
}

impl<'a> TryFrom<BuildArgs<'a>> for FileInfo {
    type Error = HdfsLibError;
    
    fn try_from(value: BuildArgs<'a>) -> Result<Self> {
        Ok(FileInfo {
            path: value.build_full_path()?,
            length: value.proto.get_length(),
            block_replication: value.proto.get_block_replication() as u16,
            block_size: value.proto.get_blocksize(),
            file_stat: FileStat::try_from(value)?,
        })
    }
}

impl<'a> TryFrom<BuildArgs<'a>> for DirInfo {
    type Error = HdfsLibError;
    
    fn try_from(value: BuildArgs<'a>) -> Result<Self> {
        Ok(DirInfo {
            path: value.build_full_path()?,
            file_stat: FileStat::try_from(value)?,
        })
    }
}

impl<'a> TryFrom<BuildArgs<'a>> for FileStat {
    type Error = HdfsLibError;
    
    fn try_from(value: BuildArgs<'a>) -> Result<Self> {
        Ok(FileStat {
            modification_time: value.proto.get_modification_time() as i64,
            access_time: value.proto.get_access_time(),
            owner: value.proto.get_owner().to_string(),
            group: value.proto.get_group().to_string()
        })
    }
}

