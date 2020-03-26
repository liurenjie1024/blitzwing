use crate::{
  error::{HdfsLibErrorKind, Result},
  hadoop_proto::hdfs::{
    DatanodeInfoProto, ExtendedBlockProto, LocatedBlockProto, LocatedBlocksProto,
  },
  hdfs::datanode::{DatanodeInfo, DatanodeInfoWithStorage},
};

use crate::utils::proto::ProtobufTranslate;
use protobuf::RepeatedField;

#[derive(Debug, Clone)]
pub struct LocatedBlocks {
  file_len: usize,
  // blocks are sorted by their offset
  blocks: Vec<LocatedBlock>,
  under_construction: bool,
  last_located_block: Option<LocatedBlock>,
  last_block_complete: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Getters, CopyGetters, Default)]
pub struct LocatedBlock {
  #[get = "pub"]
  block: ExtendedBlock,
  #[get_copy = "pub"]
  offset: usize,
  #[get = "pub"]
  locations: Vec<DatanodeInfoWithStorage>,
  #[get_copy = "pub"]
  corrupt: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Default, Getters)]
#[get = "pub"]
pub struct ExtendedBlock {
  pool_id: String,
  block: Block,
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct Block {
  block_id: u64,
  num_bytes: usize,
  generation_stamp: u64,
}

impl LocatedBlocks {
  pub fn get_file_len(&self) -> usize {
    self.file_len
  }

  pub fn in_range(&self, block_idx: usize, offset: usize) -> Result<bool> {
    self.blocks.get(block_idx).map(|b| b.in_range(offset)).ok_or_else(|| {
      HdfsLibErrorKind::InvalidArgumentError(format!(
        "Index {} exceeded vector size {}",
        block_idx,
        self.blocks.len()
      ))
      .into()
    })
  }

  pub fn block_at(&self, block_idx: usize) -> Option<&LocatedBlock> {
    self.blocks.get(block_idx)
  }

  pub fn find_block(&self, offset: usize) -> Option<&LocatedBlock> {
    match self.blocks.binary_search_by_key(&offset, |b| b.offset) {
      // The offset exactly hits the start of a block
      Ok(idx) => self.block_at(idx),
      Err(idx) => {
        if idx == 0 {
          // The offset is even smaller than the first block, this is possible because client may fetch block not starting from 0
          None
        } else {
          // We need to double check whether previous block contains this offset
          let prev_block = &self.blocks[idx - 1];
          if prev_block.in_range(offset) {
            Some(prev_block)
          } else {
            None
          }
        }
      }
    }
  }

  pub fn blocks(self) -> impl Iterator<Item = LocatedBlock> {
    self.blocks.into_iter()
  }

  /// Add a new block or replace old block with same offset
  pub fn add_block(&mut self, block: LocatedBlock) -> Result<usize> {
    match self.blocks.binary_search_by_key(&block.offset, |b| b.offset) {
      Ok(idx) => {
        debug!(
          "Old block [{:?}] already exists, will be replaced with new block [{:?}].",
          &self.blocks[idx], &block
        );

        *&mut self.blocks[idx] = block;
        Ok(idx)
      }
      Err(idx) => {
        // block not found, will add to it
        // TODO: Check that it doesn't overlap with other blocks
        self.blocks.insert(idx, block);
        Ok(idx)
      }
    }
  }
}

impl LocatedBlock {
  pub fn in_range(&self, offset: usize) -> bool {
    offset >= self.offset && offset < (self.offset + self.get_len())
  }

  pub fn get_len(&self) -> usize {
    self.block.block.num_bytes
  }

  pub fn location(&self, idx: usize) -> Option<&DatanodeInfoWithStorage> {
    self.locations.get(idx)
  }
}

impl ProtobufTranslate<ExtendedBlockProto> for ExtendedBlock {
  fn try_read_from(proto: &ExtendedBlockProto) -> Result<Self> {
    Ok(Self {
      pool_id: proto.get_poolId().to_string(),
      block: Block {
        block_id: proto.get_blockId(),
        num_bytes: proto.get_numBytes() as usize,
        generation_stamp: proto.get_generationStamp(),
      },
    })
  }

  fn try_write_to(&self) -> Result<ExtendedBlockProto> {
    let mut proto = ExtendedBlockProto::new();
    proto.set_poolId(self.pool_id.clone());
    proto.set_blockId(self.block.block_id);
    proto.set_numBytes(self.block.num_bytes as u64);
    proto.set_generationStamp(self.block.generation_stamp);

    Ok(proto)
  }
}

impl ProtobufTranslate<LocatedBlockProto> for LocatedBlock {
  fn try_read_from(proto: &LocatedBlockProto) -> Result<Self> {
    check_args!(proto.get_locs().len() == proto.get_storageIDs().len());

    let locations = proto
      .get_locs()
      .iter()
      .zip(proto.get_storageIDs())
      .map(|(datanode_info_proto, storage_id)| {
        DatanodeInfo::try_read_from(datanode_info_proto)
          .map(|datanode| DatanodeInfoWithStorage::new(datanode, storage_id))
      })
      .collect::<Result<Vec<DatanodeInfoWithStorage>>>()?;

    Ok(Self {
      block: ExtendedBlock::try_read_from(proto.get_b())?,
      offset: proto.get_offset() as usize,
      locations,
      corrupt: proto.get_corrupt(),
    })
  }

  fn try_write_to(&self) -> Result<LocatedBlockProto> {
    let mut proto = LocatedBlockProto::new();
    proto.set_b(self.block.try_write_to()?);
    proto.set_offset(self.offset as u64);

    let storage_ids: Vec<String> =
      self.locations.iter().map(|loc| loc.storage_id().to_string()).collect();
    proto.set_storageIDs(storage_ids.into());

    let datanode_info: Result<Vec<DatanodeInfoProto>> =
      self.locations.iter().map(|dn| dn.datanode_info().try_write_to()).collect();
    proto.set_locs(RepeatedField::from_vec(datanode_info?));

    proto.set_corrupt(self.corrupt);

    Ok(proto)
  }
}

impl ProtobufTranslate<LocatedBlocksProto> for LocatedBlocks {
  fn try_read_from(proto: &LocatedBlocksProto) -> Result<Self> {
    Ok(Self {
      file_len: proto.get_fileLength() as usize,
      blocks: proto
        .get_blocks()
        .iter()
        .map(LocatedBlock::try_read_from)
        .collect::<Result<Vec<LocatedBlock>>>()?,
      under_construction: proto.get_underConstruction(),
      last_located_block: None,
      last_block_complete: proto.get_isLastBlockComplete(),
    })
  }

  fn try_write_to(&self) -> Result<LocatedBlocksProto> {
    let mut proto = LocatedBlocksProto::new();

    proto.set_fileLength(self.file_len as u64);

    let blocks = self
      .blocks
      .iter()
      .map(LocatedBlock::try_write_to)
      .collect::<Result<Vec<LocatedBlockProto>>>()?;
    proto.set_blocks(blocks.into());

    proto.set_underConstruction(self.under_construction);
    proto.set_isLastBlockComplete(self.last_block_complete);

    Ok(proto)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_find_block() {
    let mut block1 = LocatedBlock::default();
    block1.offset = 0;
    block1.block.block.num_bytes = 10;

    let mut block2 = LocatedBlock::default();
    block2.offset = 15;
    block2.block.block.num_bytes = 10;

    let blocks = LocatedBlocks {
      file_len: 100,
      last_block_complete: true,
      last_located_block: None,
      blocks: vec![block1.clone(), block2.clone()],
      under_construction: false,
    };

    assert_eq!(Some(&block1), blocks.find_block(0));
    assert_eq!(Some(&block1), blocks.find_block(9));
    assert_eq!(None, blocks.find_block(10));
    assert_eq!(None, blocks.find_block(11));

    assert_eq!(Some(&block2), blocks.find_block(15));
    assert_eq!(Some(&block2), blocks.find_block(20));
    assert_eq!(None, blocks.find_block(25));
    assert_eq!(None, blocks.find_block(30));
  }
}
