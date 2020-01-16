use crate::hdfs::protocol::client_protocol::ClientProtocol;
use std::sync::Arc;
use crate::hdfs::block::LocatedBlocks;
use crate::fs::input_stream::FsInputStream;
use std::io::{Read, Error as IoError, Seek, SeekFrom};
use std::result::{Result as StdResult};
use crate::error::HdfsLibErrorKind::InvalidArgumentError;
use crate::error::Result;

struct DFSInputStream {
    name_node: Arc<dyn ClientProtocol>,
    blocks: LocatedBlocks,
    file_path: String,
    
    pos: u64,
}

impl Read for DFSInputStream {
    fn read(&mut self, buf: &mut [u8]) -> StdResult<usize, IoError> {
        unimplemented!()
    }
}

impl Seek for DFSInputStream {
    fn seek(&mut self, pos: SeekFrom) -> StdResult<u64, IoError> {
        self.do_seek(pos)
            .map_err(|e| e.into_std_io_error())
    }
}

impl FsInputStream for DFSInputStream {}

impl DFSInputStream {
    pub fn get_len(&self) -> Result<u64> {
        unimplemented!()
    }
    
    fn do_seek(&mut self, pos: SeekFrom) -> Result<u64> {
        match pos {
            SeekFrom::Start(p) => {
                self.check_pos(p as i64)?;
                self.pos = p;
            },
            SeekFrom::Current(p) => {
                let new_pos = p + self.pos as i64;
                self.check_pos(new_pos)?;
                self.pos = new_pos as u64;
            },
            SeekFrom::End(_) => {
                return Err(InvalidArgumentError(format!("Currently seek from end is not \
                supported!")).into())
            }
        };
        Ok(self.pos)
    }
    
    fn check_pos(&self, new_pos: i64) -> Result<()> {
        let len = self.get_len()?;
        if new_pos < 0 || (new_pos as u64) >= len {
            Err(InvalidArgumentError(format!("Invalid position {}, which should be between {}, \
            {}.", new_pos, 0, len)).into())
        } else {
            Ok(())
        }
    }
}

