use crate::error::{Result};
use crate::hadoop_proto::hdfs::{DatanodeIDProto, DatanodeInfoProto};

use crate::utils::proto::ProtobufTranslate;

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
    
    pub fn storage_id(&self) -> &str {
        self.storage_id.as_str()
    }
}

impl ProtobufTranslate<DatanodeIDProto> for DatanodeId {
    fn try_read_from(proto: &DatanodeIDProto) -> Result<Self> {
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
    
    fn try_write_to(&self) -> Result<DatanodeIDProto> {
        let mut proto = DatanodeIDProto::new();
        proto.set_ipAddr(self.ip_addr.clone());
        proto.set_hostName(self.hostname.clone());
        proto.set_xferPort(self.xfer_port);
        proto.set_infoPort(self.info_port);
        proto.set_infoSecurePort(self.info_secure_port);
        proto.set_ipcPort(self.ipc_port);
        proto.set_datanodeUuid(self.datanode_uuid.clone());
        
        Ok(proto)
    }
}

impl ProtobufTranslate<DatanodeInfoProto> for DatanodeInfo {
    fn try_read_from(proto: &DatanodeInfoProto) -> Result<Self> {
        Ok(Self {
            datanode_id: DatanodeId::try_read_from(proto.get_id())?
        })
    }
    
    fn try_write_to(&self) -> Result<DatanodeInfoProto> {
        let mut proto = DatanodeInfoProto::new();
        proto.set_id(self.datanode_id.try_write_to()?);
        
        Ok(proto)
    }
}
