use crate::{
  error::{
    HdfsLibError, HdfsLibErrorKind,
    HdfsLibErrorKind::{IllegalStateError, MissingBlockError},
    MissingBlockErrorInfo, Result,
  },
  fs::input_stream::FsInputStream,
  hdfs::{
    block::{LocatedBlock, LocatedBlocks},
    datanode::DatanodeInfo,
    hdfs_config::HdfsClientConfigRef,
    protocol::client_protocol::ClientProtocolRef,
    transfer::block_io::{BlockReaderArgs, BlockReaderFactoryRef, BlockReaderRef},
  },
};
use failure::ResultExt;
use std::{
  collections::HashSet,
  io::{Error as IoError, Read, Seek, SeekFrom},
  result::Result as StdResult,
};

pub(in crate::hdfs) struct DFSInputStream {
  config: HdfsClientConfigRef,
  namenode: ClientProtocolRef,
  block_reader_factory: BlockReaderFactoryRef,
  blocks: LocatedBlocks,
  filename: String,

  pos: usize,
  seek_pos: usize,

  dead_nodes: HashSet<DatanodeInfo>,
  read_block_failures: u32,

  // Current block related information, these fields should be updated transactionally
  cur_block_reader: Option<BlockReaderRef>,
  cur_block: Option<LocatedBlock>,
  cur_datanode: Option<DatanodeInfo>,
}

impl DFSInputStream {
  pub(in crate::hdfs) fn new(
    config: HdfsClientConfigRef,
    namenode: ClientProtocolRef,
    block_reader_factory: BlockReaderFactoryRef,
    filename: &str,
  ) -> Result<Self> {
    let blocks = namenode.get_block_locations(filename, 0, config.prefetch_size())?;
    Ok(Self {
      config,
      namenode,
      block_reader_factory,
      blocks,
      filename: filename.to_string(),
      pos: 0,
      seek_pos: 0,
      dead_nodes: HashSet::new(),
      read_block_failures: 0,
      cur_block_reader: None,
      cur_block: None,
      cur_datanode: None,
    })
  }
}

impl Read for DFSInputStream {
  fn read(&mut self, buf: &mut [u8]) -> StdResult<usize, IoError> {
    self.do_read(buf).map_err(|e| e.into_std_io_error())
  }
}

impl Seek for DFSInputStream {
  fn seek(&mut self, pos: SeekFrom) -> StdResult<u64, IoError> {
    self.do_seek(pos).map(|r| r as u64).map_err(|e| e.into_std_io_error())
  }
}

impl FsInputStream for DFSInputStream {
  fn skip(&mut self, len: usize) -> Result<usize> {
    self.do_seek(SeekFrom::Current(len as i64))
  }
}

impl DFSInputStream {
  pub fn get_len(&self) -> Result<usize> {
    Ok(self.blocks.get_file_len())
  }

  fn do_seek(&mut self, pos: SeekFrom) -> Result<usize> {
    let new_pos = match pos {
      SeekFrom::Start(p) => p as usize,
      SeekFrom::Current(p) => {
        if p >= 0 {
          self.seek_pos + (p as usize)
        } else {
          self.seek_pos - (-1 * p) as usize
        }
      }
      SeekFrom::End(p) => {
        check_args!(p <= 0);
        self.seek_pos - (-1 * p) as usize
      }
    };

    self.check_pos(new_pos)?;
    self.seek_pos = new_pos;
    Ok(self.seek_pos)
  }

  fn check_pos(&self, new_pos: usize) -> Result<()> {
    check_args!(new_pos < self.get_len()?);
    Ok(())
  }

  fn do_read(&mut self, buf: &mut [u8]) -> Result<usize> {
    if self.seek_pos >= self.get_len()? {
      return Ok(0);
    }

    self.do_seek_to()?;

    if let Some(ref mut block_reader) = self.cur_block_reader {
      let bytes_read = block_reader.read(buf).context(HdfsLibErrorKind::IoError)?;

      self.pos += bytes_read;
      self.seek_pos += bytes_read;

      Ok(bytes_read)
    } else {
      Err(HdfsLibError::from(IllegalStateError(format!("{}", "Block reader should not be empty!"))))
    }
  }

  fn do_seek_to(&mut self) -> Result<()> {
    if self.seek_pos >= self.pos {
      if let Some(ref mut block_reader) = self.cur_block_reader {
        let diff = self.seek_pos - self.pos;
        if diff < block_reader.available()? {
          let skipped = block_reader.skip(diff)?;
          check_state!(
            skipped == diff,
            "Block reader error: only skipped {} bytes, expected to skip {} bytes",
            skipped,
            diff
          );

          self.pos = self.seek_pos;
          return Ok(());
        }
      }
    }

    self.read_block_failures = 0;
    // We need to seek to new block now
    while self.read_block_failures < self.config.max_block_acquire_failures() {
      if let Err(e) = self.do_seek_to_new_block() {
        self.read_block_failures += 1;
        warn!(
          "Failed to read block seek to pos [{}] of file [{}] for {} times, error: {:?}",
          self.seek_pos, self.filename, self.read_block_failures, e
        );

        if let Some(ref datanode) = self.cur_datanode {
          self.dead_nodes.insert(datanode.clone());
        }
      } else {
        break;
      }
    }

    if self.read_block_failures >= self.config.max_block_acquire_failures() {
      let error_info = MissingBlockErrorInfo::new(
        self.cur_block.as_ref().map(|b| b.block().clone()),
        self.filename.clone(),
        self.seek_pos,
      );
      error!("Failed to seek to {} for {} times, which exceeded max failures: {}, will throw missing block error", self.seek_pos, self.read_block_failures, self.config.max_block_acquire_failures());
      return Err(HdfsLibError::from(MissingBlockError(error_info)));
    }

    self.pos = self.seek_pos;
    Ok(())
  }

  fn do_seek_to_new_block(&mut self) -> Result<()> {
    debug!("Trying to seek to pos [{}] of file [{}]", self.seek_pos, self.filename);
    let block = self.do_find_block(self.seek_pos)?;
    debug!("Find block [{:?}], pos [{}], file [{}]", &block, self.seek_pos, self.filename);
    match self.choose_datanode(&block) {
      Ok(datanode) => {
        debug!(
          "Choose datanode for block [{:?}], offset: [{}], datanode: [{:?}]",
          block, self.seek_pos, datanode
        );

        let block_start_offset = self.seek_pos - block.offset();
        let block_bytes_to_read = block.get_len() - block_start_offset;
        let block_reader_args = BlockReaderArgs::new(
          block_start_offset,
          block_bytes_to_read,
          false,
          "test_client".to_string(),
          datanode.clone(),
          self.filename.clone(),
          block.block().clone(),
        );

        self.cur_block = Some(block);
        self.cur_datanode = Some(datanode);

        let block_reader = self.block_reader_factory.create(block_reader_args)?;

        self.cur_block_reader = Some(block_reader);
        Ok(())
      }
      Err(e) => {
        self.fetch_block_locations(self.seek_pos)?;
        Err(e)
      }
    }
  }

  fn do_find_block(&mut self, pos: usize) -> Result<LocatedBlock> {
    let new_block = self.blocks.find_block(pos);

    if new_block.is_none() {
      self.fetch_block_locations(pos)?;
    }

    if let Some(new_block) = self.blocks.find_block(pos) {
      Ok(new_block.clone())
    } else {
      Err(HdfsLibError::from(IllegalStateError(format!(
        "Can't find block at pos [{}] in [{}]",
        self.pos, self.filename
      ))))
    }
  }

  fn fetch_block_locations(&mut self, pos: usize) -> Result<()> {
    debug!(
      "Trying to fetch block location of file [{}] with offset [{}]",
      &self.filename, self.pos
    );

    let located_blocks = self.namenode.get_block_locations(&self.filename, pos, 1)?;

    located_blocks.blocks().try_for_each(|b| self.blocks.add_block(b).map(|_v| ()))?;

    debug!("New all block locations: [{:?}], filename: [{:?}]", self.blocks, self.filename);

    Ok(())
  }

  fn choose_datanode(&self, cur_block: &LocatedBlock) -> Result<DatanodeInfo> {
    for loc in cur_block.locations() {
      if !self.dead_nodes.contains(loc.datanode_info()) {
        return Ok(loc.datanode_info().clone());
      }
    }

    Err(HdfsLibError::from(IllegalStateError(format!(
      "No live datanode for block: {:?}, file: {:?}, deadnodes: {:?}",
      cur_block, self.filename, self.dead_nodes
    ))))
  }
}
