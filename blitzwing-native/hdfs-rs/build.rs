extern crate protoc_rust;

use protoc_rust::Customize;

fn main() {
    let input_protos = vec![
        "proto/acl.proto",
        "proto/ClientDatanodeProtocol.proto",
        "proto/ClientNamenodeProtocol.proto",
        "proto/DatanodeLifelineProtocol.proto",
        "proto/DatanodeProtocol.proto",
        "proto/datatransfer.proto",
        "proto/encryption.proto",
        "proto/fsimage.proto",
        "proto/GenericRefreshProtocol.proto",
        "proto/GetUserMappingsProtocol.proto",
        "proto/HAServiceProtocol.proto",
        "proto/HAZKInfo.proto",
        "proto/hdfs.proto",
        "proto/inotify.proto",
        "proto/InterDatanodeProtocol.proto",
        "proto/IpcConnectionContext.proto",
        "proto/JournalProtocol.proto",
        "proto/NamenodeProtocol.proto",
        "proto/ProtobufRpcEngine.proto",
        "proto/ProtocolInfo.proto",
        "proto/QJournalProtocol.proto",
        "proto/ReconfigurationProtocol.proto",
        "proto/RefreshAuthorizationPolicyProtocol.proto",
        "proto/RefreshCallQueueProtocol.proto",
        "proto/RefreshUserMappingsProtocol.proto",
        "proto/RpcHeader.proto",
        "proto/Security.proto",
        "proto/TraceAdmin.proto",
        "proto/xattr.proto",
        "proto/ZKFCProtocol.proto",
    ];

    protoc_rust::run(protoc_rust::Args {
        out_dir: "src/hadoop_proto",
        input: &input_protos,
        includes: &["proto"],
        customize: Customize::default(),
    })
    .expect("protoc");
}
