use crate::error::{HdfsLibError, Result};
use crate::hadoop_proto::hdfs::{DatanodeIDProto, DatanodeInfoProto};
use std::convert::TryFrom;

#[derive(Debug, Clone)]
pub struct DatanodeId {
    ip_addr: String,
    hostname: String,
    xfer_port: u32,
    info_port: u32,
    info_secure_port: u32,
    ipc_port: u32,
    datanode_uuid: String,
}

#[derive(Debug, Clone)]
pub struct DatanodeInfo {
    datanode_id: DatanodeId,
}

#[derive(Debug, Clone)]
pub struct DatanodeInfoWithStorage {
    datanode_info: DatanodeInfo,
    storage_id: String,
}

impl DatanodeInfoWithStorage {
    pub fn new(datanode_info: DatanodeInfo, storage_id: &str) -> Self {
        Self {
            datanode_info,
            storage_id: storage_id.to_string(),
        }
    }

    pub fn datanode_info(&self) -> DatanodeInfo {
        self.datanode_info.clone()
    }
}

impl TryFrom<&'_ DatanodeInfoProto> for DatanodeInfo {
    type Error = HdfsLibError;

    fn try_from(proto: &DatanodeInfoProto) -> Result<Self> {
        Ok(Self {
            datanode_id: DatanodeId::try_from(proto.get_id())?,
        })
    }
}

impl TryFrom<&'_ DatanodeIDProto> for DatanodeId {
    type Error = HdfsLibError;

    fn try_from(proto: &DatanodeIDProto) -> Result<Self> {
        Ok(Self {
            ip_addr: proto.get_ipAddr().to_string(),
            hostname: proto.get_hostName().to_string(),
            xfer_port: proto.get_xferPort(),
            info_port: proto.get_infoPort(),
            info_secure_port: proto.get_infoSecurePort(),
            ipc_port: proto.get_ipcPort(),
            datanode_uuid: proto.get_datanodeUuid().to_string(),
        })
    }
}
