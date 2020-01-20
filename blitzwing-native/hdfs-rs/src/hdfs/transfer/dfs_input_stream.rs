use crate::error::HdfsLibErrorKind::InvalidArgumentError;
use crate::error::{Result, HdfsLibErrorKind};
use crate::fs::input_stream::FsInputStream;
use crate::hdfs::block::LocatedBlocks;
use crate::hdfs::protocol::client_protocol::{ClientProtocol, ClientProtocolRef};
use std::io::{Error as IoError, Read, Seek, SeekFrom};
use std::result::Result as StdResult;
use std::sync::Arc;
use crate::hdfs::transfer::block_reader::{BlockReader, BlockReaderRef};
use crate::hdfs::datanode::DatanodeInfo;
use crate::hdfs::hdfs_config::HdfsClientConfigRef;

struct DFSInputStream {
    config: HdfsClientConfigRef,
    name_node: ClientProtocolRef,
    blocks: LocatedBlocks,
    file_path: String,

    pos: u64,
    
    // We should keep updating these three fields in a transactional way
    current_block_idx: Option<usize>,
    current_data_node: Option<DatanodeInfo>,
    block_reader: Option<BlockReaderRef>,
}

impl Read for DFSInputStream {
    fn read(&mut self, buf: &mut [u8]) -> StdResult<usize, IoError> {
        unimplemented!()
    }
}

impl Seek for DFSInputStream {
    fn seek(&mut self, pos: SeekFrom) -> StdResult<u64, IoError> {
        self.do_seek(pos).map_err(|e| e.into_std_io_error())
    }
}

impl FsInputStream for DFSInputStream {}

impl DFSInputStream {
    pub fn get_len(&self) -> Result<u64> {
        Ok(self.blocks.get_file_len())
    }

    fn do_seek(&mut self, pos: SeekFrom) -> Result<u64> {
        match pos {
            SeekFrom::Start(p) => {
                self.check_pos(p as i64)?;
                self.pos = p;
            }
            SeekFrom::Current(p) => {
                let new_pos = p + self.pos as i64;
                self.check_pos(new_pos)?;
                self.pos = new_pos as u64;
            }
            SeekFrom::End(_) => {
                return Err(InvalidArgumentError(format!(
                    "Currently seek from end is not \
                     supported!"
                ))
                .into())
            }
        };
        Ok(self.pos)
    }

    fn check_pos(&self, new_pos: i64) -> Result<()> {
        let len = self.get_len()?;
        if new_pos < 0 || (new_pos as u64) >= len {
            Err(InvalidArgumentError(format!(
                "Invalid position {}, which should be between {}, \
                 {}.",
                new_pos, 0, len
            ))
            .into())
        } else {
            Ok(())
        }
    }
    
    fn do_read(&mut self, buf: &mut [u8]) -> Result<usize> {
        unimplemented!()
    }
    
    fn seek_to_target_block(&mut self) -> Result<()> {
        // Check whether we need to seek to new block
        if let Some(idx) = self.current_block_idx {
            match self.blocks.in_range(idx, self.pos) {
                Ok(true) => {
                    debug!("Current position {} in block [{:?}], will skip seeking to new block",
                           self.pos, self.blocks.get_block(idx));
                    return Ok(());
                },
                Ok(false) => {
                    debug!("Current position {} not in block [{:?}], will seek to new block",
                           self.pos, self.blocks.get_block(idx));
                },
                Err(e) => {
                    warn!("Failed to check whether current position {} in block range: {}",
                        self.pos, e);
                }
            }
        }
        
        Ok(())
    }
    
    fn do_seek_to_new_block(&mut self) -> Result<()> {
        
        let new_block_idx = self.fetch_located_block()?;
        let new_data_node = self.choose_datanode(new_block_idx)?;
        
        
        
        unimplemented!()
    }
    
    fn fetch_located_block(&mut self) -> Result<usize> {
        if let Some(idx)  = self.blocks.search_block(self.pos) {
            return Ok(idx);
        }
        
        debug!("Trying to fetch block location of file [{}] with offset [{}]", &self.file_path,
               self.pos);
        
        let located_blocks = self.name_node.get_block_locations(&self.file_path, self.pos, 1)?;
        
        located_blocks.blocks().try_for_each(|b| self.blocks.add_block(b).map(|v| ()))?;
        
        self.blocks.search_block(self.pos)
            .ok_or_else(|| HdfsLibErrorKind::SystemError(format!("Unable to find block with \
            offset [{}] for file [{}].", self.pos, self.file_path)).into())
    }
    
    fn choose_datanode(&self, block_idx: usize) -> Result<DatanodeInfo> {
        self.blocks.get_block(block_idx)
            .and_then(|b| b.location(0))
            .map(|datanode|datanode.datanode_info())
            .ok_or_else(|| sys_err!("Unable to find datanode information for block index: {}", block_idx))
    }
}


