use crate::{
  error::{BlitzwingError, Result},
  proto::{
    parquet::{
      ColumnDescProto, ParquetProto_Compression as CompressionProto,
      ParquetProto_PhysicalType as TypeProto,
    },
    record_batch::{JniBufferNodeProto, JniRecordBatchProto, JniValueNodeProto},
  },
};
use arrow::record_batch::RecordBatch;
use parquet::basic::{Compression, Type};
use std::{convert::TryFrom, rc::Rc};

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
      TypeProto::INT96 => Type::INT96,
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

impl TryFrom<RecordBatch> for JniRecordBatchProto {
  type Error = BlitzwingError;
  fn try_from(record_batch: RecordBatch) -> Result<Self> {
    let mut jni_record_batch = JniRecordBatchProto::new();
    jni_record_batch.set_length(record_batch.num_rows() as i32);
    for i in 0..record_batch.num_columns() {
      let array = record_batch.column(i);

      let mut jni_value_node = JniValueNodeProto::new();
      jni_value_node.set_length(array.len() as i32);
      jni_value_node.set_null_count(array.null_count() as i32);
      jni_record_batch.mut_nodes().push(jni_value_node);

      if let Some(null_buffer) = array.data_ref().null_buffer() {
        let mut jni_buffer_node = JniBufferNodeProto::new();
        jni_buffer_node.set_address(null_buffer.raw_data() as i64);
        jni_buffer_node.set_length(null_buffer.len() as i32);

        jni_record_batch.mut_buffers().push(jni_buffer_node);
      } else {
        let mut jni_buffer_node = JniBufferNodeProto::new();
        jni_buffer_node.set_address(0i64);
        jni_buffer_node.set_length(0i32);

        jni_record_batch.mut_buffers().push(jni_buffer_node);
      }

      for buffer in array.data_ref().buffers() {
        let mut jni_buffer_node = JniBufferNodeProto::new();
        jni_buffer_node.set_address(buffer.raw_data() as i64);
        jni_buffer_node.set_length(buffer.len() as i32);

        jni_record_batch.mut_buffers().push(jni_buffer_node);
      }
    }

    Ok(jni_record_batch)
  }
}
