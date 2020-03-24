use crate::proto::parquet::ColumnDescProto;
use std::rc::Rc;
use parquet::basic::{Compression, Type};
use crate::proto::parquet::{ParquetProto_Compression as CompressionProto, ParquetProto_PhysicalType as TypeProto};
use std::convert::TryFrom;
use crate::error::{BlitzwingError, Result};

pub(crate) type ColumnDescProtoPtr = Rc<ColumnDescProto>;

impl TryFrom<TypeProto> for Type {
  type Error = BlitzwingError;
  fn try_from(proto: TypeProto) -> Result<Self> { 
    Ok(match proto {
      TypeProto::BOOLEAN => Type::BOOLEAN,
      TypeProto::BYTE_ARRAY => Type::BYTE_ARRAY,
      TypeProto::DOUBLE => Type::DOUBLE,
      TypeProto::FIXED_LEN_BYTE_ARRAY => Type::FIXED_LEN_BYTE_ARRAY,
      TypeProto::FLOAT => Type::FLOAT,
      TypeProto::INT32 => Type::INT32,
      TypeProto::INT64 => Type::INT64,
      TypeProto::INT96 => Type::INT96
    })
 }
}

impl TryFrom<CompressionProto> for Compression {
  type Error = BlitzwingError;
  fn try_from(proto: CompressionProto) -> Result<Self> {
    Ok(match proto {
      CompressionProto::BROTLI => Compression::BROTLI,
      CompressionProto::GZIP => Compression::GZIP,
      CompressionProto::LZ4 => Compression::LZ4,
      CompressionProto::LZO => Compression::LZO,
      CompressionProto::SNAPPY => Compression::SNAPPY,
      CompressionProto::UNCOMPRESSED => Compression::UNCOMPRESSED,
      CompressionProto::ZSTD => Compression::ZSTD,
    })
  }
}