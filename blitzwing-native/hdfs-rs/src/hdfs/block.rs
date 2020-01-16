use crate::hadoop_proto::hdfs::{LocatedBlocksProto, LocatedBlockProto, ExtendedBlockProto};
use crate::error::{Result, HdfsLibError};
use std::convert::TryFrom;
use crate::hdfs::datanode::{DatanodeInfoWithStorage, DatanodeInfo};
use crate::error::HdfsLibErrorKind::InvalidArgumentError;

pub struct LocatedBlocks {
    file_len: u64,
    blocks: Vec<LocatedBlock>,
    under_construction: bool,
    last_located_block: Option<LocatedBlock>,
    last_block_complete: bool,
}


pub struct LocatedBlock {
    block: ExtendedBlock,
    offset: u64,
    locations: Vec<DatanodeInfoWithStorage>,
    corrupt: bool,
}

pub struct ExtendedBlock {
    pool_id: String,
    block: Block
}

pub struct Block {
    block_id: u64,
    num_bytes: u64,
    generation_stamp: u64
}

impl TryFrom<&'_ LocatedBlocksProto> for LocatedBlocks {
    type Error = HdfsLibError;
    
    fn try_from(proto: &'_ LocatedBlocksProto) -> Result<Self> {
        Ok(Self {
            file_len: proto.get_fileLength(),
            blocks: proto.get_blocks()
                .iter()
                .map(LocatedBlock::try_from)
                .collect::<Result<Vec<LocatedBlock>>>()?,
            under_construction: proto.get_underConstruction(),
            last_located_block: None,
            last_block_complete: proto.get_isLastBlockComplete()
        })
    }
}

impl TryFrom<&'_ LocatedBlockProto> for LocatedBlock {
    type Error = HdfsLibError;
    
    fn try_from(proto: &LocatedBlockProto) -> Result<Self> {
        check_args!(proto.get_locs().len() == proto.get_storageIDs().len());
        
        let locations = proto.get_locs().iter()
            .zip(proto.get_storageIDs())
            .map(|(datanode_info_proto, storage_id)| DatanodeInfo::try_from(datanode_info_proto)
                .map(|datanode| DatanodeInfoWithStorage::new(datanode, storage_id)))
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
                generation_stamp: proto.get_generationStamp()
            }
        })
    }
}
