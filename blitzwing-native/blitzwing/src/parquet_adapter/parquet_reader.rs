use crate::{
  error::{
    BlitzwingErrorKind::{ArrowError, IllegalStateError, InvalidArgumentError},
    Result,
  },
  parquet_adapter::array_reader::{ArrayReaderRef, *},
  proto::parquet::{ParquetReaderProto, RowGroupProto},
  types::ColumnDescProtoPtr,
  util::{
    buffer::manager::{BufferDataManagerRef, BufferManager, CachedManager, RootManager},
    reader::{create_page_reader, PageReaderIteratorRef, PageReaderRef},
    shared_queue::SharedQueue,
  },
};
use arrow::{
  datatypes::{DataType, SchemaRef},
  ipc::convert::schema_from_bytes,
  record_batch::RecordBatch,
};
use failure::ResultExt;
use parquet::basic::Type;
use std::{collections::HashMap, convert::TryInto, rc::Rc, sync::Arc};

struct ColumnInfo {
  array_reader: ArrayReaderRef,
  page_readers: SharedQueue<PageReaderRef>,
  column_desc: ColumnDescProtoPtr,
  buffer_data_manager: BufferDataManagerRef,
}

pub(crate) struct ParquetReader {
  schema: SchemaRef,
  columns: Vec<ColumnInfo>,
  /// Key: column name
  /// Value: index into `columns`
  name_to_column: HashMap<String, usize>,
  meta: ParquetReaderProto,
  /// Key: buffer address
  /// Value: index into `columns`
  buffers: HashMap<*mut u8, usize>,
  root_buffer_manager: BufferDataManagerRef,
}

impl ParquetReader {
  fn column_by_name<S: AsRef<str>>(&self, name: S) -> Option<&ColumnInfo> {
    self.name_to_column.get(name.as_ref()).and_then(|idx| self.columns.get(*idx))
  }

  fn column_by_name_mut<S: AsRef<str>>(&mut self, name: S) -> Option<&mut ColumnInfo> {
    self
      .name_to_column
      .get(name.as_ref())
      .map(|idx| *idx)
      .and_then(move |idx| self.columns.get_mut(idx))
  }

  pub(crate) fn set_data(&mut self, row_group_meta: RowGroupProto) -> Result<()> {
    for column_chunk in row_group_meta.get_columns() {
      if let Some(column) = self.column_by_name_mut(column_chunk.get_column_name()) {
        let page_reader = Box::new(create_page_reader(column.column_desc.as_ref(), column_chunk)?);
        column.page_readers.push(page_reader)?;
      } else {
        return Err(InvalidArgumentError(format!(
          "Column name not found {}",
          column_chunk.get_column_name()
        )))?;
      }
    }

    Ok(())
  }

  pub(crate) fn next_batch(&mut self) -> Result<usize> {
    let mut last_size = None;
    for c in &mut self.columns {
      let cur_size = c.array_reader.next_batch()?;
      match last_size {
        Some(s) => {
          if s != cur_size {
            return Err(IllegalStateError(format!(
              "Column batch size not match, previous: {}, current: {}",
              s, cur_size
            )))?;
          }
        }
        None => {
          last_size = Some(cur_size);
        }
      }
    }

    Ok(last_size.unwrap())
  }

  pub(crate) fn collect(&mut self) -> Result<RecordBatch> {
    let mut columns = Vec::with_capacity(self.columns.len());
    for (idx, column_info) in self.columns.iter_mut().enumerate() {
      let array = column_info.array_reader.collect()?;
      columns.push(array.clone());

      for b in array.data_ref().buffers() {
        self.buffers.insert(b.raw_data() as *mut u8, idx);
      }
    }

    Ok(RecordBatch::try_new(self.schema.clone(), columns).context(ArrowError)?)
  }

  pub(crate) fn close(&mut self) -> Result<()> {
    Ok(())
  }

  pub(crate) fn free_buffer(&mut self, address: *const u8, _length: usize) -> Result<()> {
    let address = address as *mut u8;
    if let Some(idx) = self.buffers.remove(&address) {
      (&mut self.columns[idx]).array_reader.free_buffer(address)?;
    }

    Ok(())
  }
}

pub(crate) fn create_parquet_reader(meta: ParquetReaderProto) -> Result<ParquetReader> {
  let mut columns = Vec::with_capacity(meta.get_column_desc().len());
  let mut name_to_column = HashMap::<String, usize>::with_capacity(meta.get_column_desc().len());

  let schema = schema_from_bytes(meta.get_schema())
    .ok_or_else(|| InvalidArgumentError("Can't build arrow schema!".to_string()))?;

  let root_manager = Arc::new(RootManager {});

  for column in meta.get_column_desc() {
    debug!("Begin to create column column reader for {:?}", column);
    if let Some((_, field)) = schema.column_with_name(column.get_column_name()) {
      let column_desc = Rc::new(column.clone());
      let batch_size = meta.get_batch_size() as usize;
      let page_readers = SharedQueue::new();

      let page_reader_iter: PageReaderIteratorRef = Box::new(page_readers.clone().into_iterator());
      let column_desc_ptr = column_desc.clone();
      let buffer_data_manager = Arc::new(CachedManager::new(root_manager.clone()));
      let buffer_manager = BufferManager::new(buffer_data_manager.clone());

      let array_reader: ArrayReaderRef =
        match (field.data_type(), column.get_physical_type().try_into()?) {
          (&DataType::Int8, Type::INT32) => Box::new(Int8ArrayReader::new(
            batch_size,
            true,
            column_desc_ptr,
            page_reader_iter,
            buffer_manager,
          )?),
          (&DataType::Int16, Type::INT32) => Box::new(Int16ArrayReader::new(
            batch_size,
            true,
            column_desc_ptr,
            page_reader_iter,
            buffer_manager,
          )?),
          (&DataType::Int32, Type::INT32) => Box::new(Int32ArrayReader::new(
            batch_size,
            false,
            column_desc_ptr,
            page_reader_iter,
            buffer_manager,
          )?),
          (&DataType::UInt8, Type::INT32) => Box::new(UInt8ArrayReader::new(
            batch_size,
            true,
            column_desc_ptr,
            page_reader_iter,
            buffer_manager,
          )?),
          (&DataType::UInt16, Type::INT32) => Box::new(UInt16ArrayReader::new(
            batch_size,
            true,
            column_desc_ptr,
            page_reader_iter,
            buffer_manager,
          )?),
          (&DataType::UInt32, Type::INT32) => Box::new(UInt32ArrayReader::new(
            batch_size,
            false,
            column_desc_ptr,
            page_reader_iter,
            buffer_manager,
          )?),
          (&DataType::Int64, Type::INT64) => Box::new(Int64ArrayReader::new(
            batch_size,
            false,
            column_desc_ptr,
            page_reader_iter,
            buffer_manager,
          )?),
          (&DataType::UInt64, Type::INT64) => Box::new(UInt64ArrayReader::new(
            batch_size,
            false,
            column_desc_ptr,
            page_reader_iter,
            buffer_manager,
          )?),
          (&DataType::Float32, Type::FLOAT) => Box::new(Float32ArrayReader::new(
            batch_size,
            false,
            column_desc_ptr,
            page_reader_iter,
            buffer_manager,
          )?),
          (&DataType::Float64, Type::DOUBLE) => Box::new(Float64ArrayReader::new(
            batch_size,
            false,
            column_desc_ptr,
            page_reader_iter,
            buffer_manager,
          )?),
          (&DataType::Utf8, Type::BYTE_ARRAY) => Box::new(UTF8ArrayReader::new(
            batch_size,
            column_desc_ptr,
            DataType::Utf8,
            page_reader_iter,
            buffer_manager,
          )?),
          (dt, parquet_type) => {
            return Err(nyi!(
              "Reading {:?} array from parquet type {:?} is not supported!",
              dt,
              parquet_type
            ))
          }
        };

      let column_info = ColumnInfo { array_reader, page_readers, column_desc, buffer_data_manager };
      columns.push(column_info);
      name_to_column.insert(column.get_column_name().to_string(), columns.len() - 1);
    } else {
      return Err(InvalidArgumentError(format!(
        "Column [{}] not found in schema.",
        column.get_column_name()
      )))?;
    }
    debug!("Finished creating column column reader for {:?}", column);
  }

  let buffers = HashMap::with_capacity(columns.len() * 3);
  Ok(ParquetReader {
    schema: Arc::new(schema),
    columns,
    name_to_column,
    meta,
    buffers,
    root_buffer_manager: root_manager,
  })
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::proto::parquet::{ColumnDescProto, ParquetProto_PhysicalType, ParquetReaderProto};
  use arrow::{
    datatypes::Schema,
    ipc::{convert::schema_to_fb_offset, MessageBuilder, MessageHeader, MetadataVersion},
  };
  use flatbuffers::{FlatBufferBuilder, WIPOffset};
  use serde_json::{self, Value};

  fn serialize_schema(schema: &Schema) -> Vec<u8> {
    let mut buffer = FlatBufferBuilder::new();

    let offset = schema_to_fb_offset(&mut buffer, schema).value();

    let mut message = MessageBuilder::new(&mut buffer);
    message.add_version(MetadataVersion::V4);
    message.add_bodyLength(0);
    message.add_header(WIPOffset::new(offset));
    message.add_header_type(MessageHeader::Schema);
    let message_offset = message.finish();

    buffer.finish(message_offset, None);

    let (mut all, start) = buffer.collapse();
    all.split_off(start)
  }

  fn create_column_desc_proto(
    name: &str,
    max_def_level: i32,
    type_len: i32,
    physical_type: ParquetProto_PhysicalType,
  ) -> ColumnDescProto {
    let mut ret = ColumnDescProto::new();
    ret.set_column_name(name.to_string());
    ret.set_max_def_level(max_def_level);
    ret.set_type_length(type_len);
    ret.set_physical_type(physical_type);

    ret
  }

  #[test]
  fn test_create_parquet_reader() {
    let parquet_reader_proto = {
      let mut proto = ParquetReaderProto::new();
      proto.set_batch_size(1024);
      proto.mut_column_desc().push(create_column_desc_proto(
        "name",
        1,
        0,
        ParquetProto_PhysicalType::BYTE_ARRAY,
      ));
      proto.mut_column_desc().push(create_column_desc_proto(
        "age",
        1,
        0,
        ParquetProto_PhysicalType::INT32,
      ));

      let schema_json = r#"{
        "fields": [
            {
                "name": "name",
                "nullable": true,
                "type": {
                    "name": "utf8"
                },
                "children": []
            },
            {
                "name": "age",
                "nullable": true,
                "type": {
                  "name": "int", 
                  "isSigned": true, 
                  "bitWidth": 32
                },
                "children": []
            }
        ]
    }"#;
      let value: Value = serde_json::from_str(&schema_json).unwrap();
      let schema = Schema::from(&value).unwrap();
      proto.set_schema(serialize_schema(&schema));

      proto
    };

    create_parquet_reader(parquet_reader_proto).unwrap();
  }
}
