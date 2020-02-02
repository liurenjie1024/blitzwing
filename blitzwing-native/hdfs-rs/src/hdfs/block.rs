use crate::error::{HdfsLibError, HdfsLibErrorKind, Result};
use crate::hadoop_proto::hdfs::{ExtendedBlockProto, LocatedBlockProto, LocatedBlocksProto};
use crate::hdfs::block::OffsetOrRange::{Offset, Range as ORange};
use crate::hdfs::datanode::{DatanodeInfo, DatanodeInfoWithStorage};
use failure::_core::cmp::Ordering;
use std::convert::TryFrom;
use std::ops::Range;

#[derive(Debug, Clone)]
pub struct LocatedBlocks {
    file_len: u64,
    // blocks are sorted by their offset
    blocks: Vec<LocatedBlock>,
    under_construction: bool,
    last_located_block: Option<LocatedBlock>,
    last_block_complete: bool,
}

#[derive(Debug, Clone)]
pub struct LocatedBlock {
    block: ExtendedBlock,
    offset: u64,
    locations: Vec<DatanodeInfoWithStorage>,
    corrupt: bool,
}

#[derive(Debug, Clone)]
pub struct ExtendedBlock {
    pool_id: String,
    block: Block,
}

#[derive(Debug, Clone)]
pub struct Block {
    block_id: u64,
    num_bytes: u64,
    generation_stamp: u64,
}

enum OffsetOrRange {
    Offset(u64),
    Range(Range<u64>),
}

impl OffsetOrRange {
    pub fn offset(v: u64) -> Self {
        OffsetOrRange::Offset(v)
    }

    pub fn range(v: Range<u64>) -> Self {
        OffsetOrRange::Range(v)
    }
}

impl OffsetOrRange {
    fn order_of(offset: u64, range: &Range<u64>) -> Ordering {
        if offset < range.start {
            Ordering::Less
        } else if range.end >= offset {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    }

    fn order_of_range(r1: &Range<u64>, r2: &Range<u64>) -> Ordering {
        if r1.start != r2.start {
            r1.start.cmp(&r2.start)
        } else {
            r1.end.cmp(&r2.end)
        }
    }
}

impl Eq for OffsetOrRange {}

impl PartialEq for OffsetOrRange {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl PartialOrd for OffsetOrRange {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OffsetOrRange {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Offset(s), Offset(o)) => s.cmp(o),
            (ORange(s), ORange(o)) => OffsetOrRange::order_of_range(s, o),
            (Offset(s), ORange(o)) => OffsetOrRange::order_of(*s, o),
            (ORange(s), Offset(o)) => OffsetOrRange::order_of(*o, s),
        }
    }
}

impl LocatedBlocks {
    pub fn get_file_len(&self) -> u64 {
        self.file_len
    }

    pub fn in_range(&self, block_idx: usize, offset: u64) -> Result<bool> {
        self.blocks
            .get(block_idx)
            .map(|b| b.in_range(offset))
            .ok_or_else(|| {
                HdfsLibErrorKind::InvalidArgumentError(format!(
                    "Index {} exceeded vector size {}",
                    block_idx,
                    self.blocks.len()
                ))
                .into()
            })
    }

    pub fn get_block(&self, block_idx: usize) -> Option<&LocatedBlock> {
        self.blocks.get(block_idx)
    }

    pub fn search_block(&self, offset: u64) -> Option<usize> {
        self.blocks
            .binary_search_by_key(&OffsetOrRange::offset(offset), |b| b.offset_range())
            .ok()
    }

    pub fn blocks(self) -> impl Iterator<Item = LocatedBlock> {
        self.blocks.into_iter()
    }

    /// Add a new block or replace old block with same offset
    pub fn add_block(&mut self, block: LocatedBlock) -> Result<usize> {
        let new_offset_range = block.offset_range();
        match self
            .blocks
            .binary_search_by_key(&new_offset_range, |b| b.offset_range())
        {
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
                self.blocks.insert(idx, block);
                Ok(idx)
            }
        }
    }
}

impl LocatedBlock {
    pub fn in_range(&self, offset: u64) -> bool {
        offset >= self.offset && offset < (self.offset + self.get_len())
    }

    pub fn get_len(&self) -> u64 {
        self.block.block.num_bytes
    }

    fn offset_range(&self) -> OffsetOrRange {
        OffsetOrRange::range(self.offset..(self.offset + self.get_len()))
    }

    pub fn location(&self, idx: usize) -> Option<&DatanodeInfoWithStorage> {
        self.locations.get(idx)
    }
}

impl TryFrom<&'_ LocatedBlocksProto> for LocatedBlocks {
    type Error = HdfsLibError;

    fn try_from(proto: &'_ LocatedBlocksProto) -> Result<Self> {
        Ok(Self {
            file_len: proto.get_fileLength(),
            blocks: proto
                .get_blocks()
                .iter()
                .map(LocatedBlock::try_from)
                .collect::<Result<Vec<LocatedBlock>>>()?,
            under_construction: proto.get_underConstruction(),
            last_located_block: None,
            last_block_complete: proto.get_isLastBlockComplete(),
        })
    }
}

impl TryFrom<&'_ LocatedBlockProto> for LocatedBlock {
    type Error = HdfsLibError;

    fn try_from(proto: &LocatedBlockProto) -> Result<Self> {
        check_args!(proto.get_locs().len() == proto.get_storageIDs().len());

        let locations = proto
            .get_locs()
            .iter()
            .zip(proto.get_storageIDs())
            .map(|(datanode_info_proto, storage_id)| {
                DatanodeInfo::try_from(datanode_info_proto)
                    .map(|datanode| DatanodeInfoWithStorage::new(datanode, storage_id))
            })
            .collect::<Result<Vec<DatanodeInfoWithStorage>>>()?;

        Ok(Self {
            block: ExtendedBlock::try_from(proto.get_b())?,
            offset: proto.get_offset(),
            locations,
            corrupt: proto.get_corrupt(),
        })
    }
}

impl TryFrom<&'_ ExtendedBlockProto> for ExtendedBlock {
    type Error = HdfsLibError;

    fn try_from(proto: &ExtendedBlockProto) -> Result<Self> {
        Ok(Self {
            pool_id: proto.get_poolId().to_string(),
            block: Block {
                block_id: proto.get_blockId(),
                num_bytes: proto.get_numBytes(),
                generation_stamp: proto.get_generationStamp(),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::hdfs::block::OffsetOrRange;

    #[test]
    fn test_compare_offset_or_range() {
        let offset = OffsetOrRange::Offset(10);
        let range = OffsetOrRange::Range(5..10);

        assert!(offset != range);
    }
}
