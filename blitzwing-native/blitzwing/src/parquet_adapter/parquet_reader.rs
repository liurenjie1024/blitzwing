use crate::{
  error::{
    BlitzwingErrorKind::{ArrowError, InvalidArgumentError, IllegalStateError},
    Result,
  },
  parquet_adapter::array_reader::{ArrayReaderRef, *},
  proto::parquet::{ParquetReaderProto, RowGroupProto},
  types::ColumnDescProtoPtr,
  util::{
    reader::{create_page_reader, PageReaderIteratorRef, PageReaderRef},
    shared_queue::SharedQueue,
  },
};
use arrow::{
  array::ArrayRef,
  datatypes::{DataType, SchemaRef},
  ipc::convert::schema_from_bytes,
  record_batch::RecordBatch,
};
use failure::ResultExt;
use parquet::basic::Type;
use std::{collections::HashMap, convert::TryInto, rc::Rc, sync::Arc};
use crate::util::buffer::manager::BufferDataManagerRef;
use crate::util::buffer::buffer::BufferData;
use crate::util::buffer::manager::{CachedManager, BufferManager, RootManager};

struct ColumnInfo {
  array_reader: ArrayReaderRef,
  page_readers: SharedQueue<PageReaderRef>,
  column_desc: ColumnDescProtoPtr,
  buffer_data_manager: BufferDataManagerRef
}

pub(crate) struct ParquetReader {
  schema: SchemaRef,
  columns: HashMap<String, ColumnInfo>,
  meta: ParquetReaderProto,
  buffers: HashMap<BufferData, BufferDataManagerRef>,
  root_buffer_manager: BufferDataManagerRef,
}

impl ParquetReader {
  pub(crate) fn set_data(&mut self, row_group_meta: RowGroupProto) -> Result<()> {
    for column_chunk in row_group_meta.get_columns() {
      if let Some(column) = self.columns.get_mut(column_chunk.get_column_name()) {
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
    for c in self.columns.values_mut() {
      let cur_size = c.array_reader.next_batch()?;
      match last_size {
        Some(s) => {
          if s != cur_size {
            return Err(IllegalStateError(format!("Column batch size not match, previous: {}, current: {}", s, cur_size)))?;
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
    for column_info in self.columns.values_mut() {
      let array = column_info.array_reader.collect()?;
      columns.push(array.clone());

      for b in array.data_ref().buffers() {
        let buffer_data = BufferData::from_arrow_buffer_without_len(b);
        self.buffers.insert(buffer_data, column_info.buffer_data_manager.clone());
      }
    }

    Ok(RecordBatch::try_new(self.schema.clone(), columns).context(ArrowError)?)
  }


  pub(crate) fn close(&mut self) -> Result<()> {
    Ok(())
  }

  pub(crate) fn free_buffer(&mut self, address: *const u8, length: usize) -> Result<()> {
    let buffer_data = BufferData::new(address as *mut u8, length);
    if let Some(buffer_manager) = self.buffers.remove(&buffer_data) {
      buffer_manager.deallocate(&buffer_data)?;
    } 

    Ok(())
  }
}

pub(crate) fn create_parquet_reader(meta: ParquetReaderProto) -> Result<ParquetReader> {
  let mut columns = HashMap::<String, ColumnInfo>::with_capacity(meta.get_column_desc().len());

  let schema = schema_from_bytes(meta.get_schema())
    .ok_or_else(|| InvalidArgumentError("Can't build arrow schema!".to_string()))?;
  
  let root_manager = Arc::new(RootManager {});

  for column in meta.get_column_desc() {
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
          (&DataType::Int8, Type::INT32) => {
            Box::new(Int8ArrayReader::new(batch_size, true, column_desc_ptr, page_reader_iter, buffer_manager)?)
          }
          (&DataType::Int16, Type::INT32) => {
            Box::new(Int16ArrayReader::new(batch_size, true, column_desc_ptr, page_reader_iter, buffer_manager)?)
          }
          (&DataType::Int32, Type::INT32) => {
            Box::new(Int32ArrayReader::new(batch_size, false, column_desc_ptr, page_reader_iter, buffer_manager)?)
          }
          (&DataType::UInt8, Type::INT32) => {
            Box::new(UInt8ArrayReader::new(batch_size, true, column_desc_ptr, page_reader_iter, buffer_manager)?)
          }
          (&DataType::UInt16, Type::INT32) => {
            Box::new(UInt16ArrayReader::new(batch_size, true, column_desc_ptr, page_reader_iter, buffer_manager)?)
          }
          (&DataType::UInt32, Type::INT32) => {
            Box::new(UInt32ArrayReader::new(batch_size, false, column_desc_ptr, page_reader_iter, buffer_manager)?)
          }
          (&DataType::Int64, Type::INT64) => {
            Box::new(Int64ArrayReader::new(batch_size, false, column_desc_ptr, page_reader_iter, buffer_manager)?)
          }
          (&DataType::UInt64, Type::INT64) => {
            Box::new(UInt64ArrayReader::new(batch_size, false, column_desc_ptr, page_reader_iter, buffer_manager)?)
          }
          (&DataType::Float32, Type::FLOAT) => {
            Box::new(Float32ArrayReader::new(batch_size, false, column_desc_ptr, page_reader_iter, buffer_manager)?)
          }
          (&DataType::Float64, Type::DOUBLE) => {
            Box::new(Float64ArrayReader::new(batch_size, false, column_desc_ptr, page_reader_iter, buffer_manager)?)
          }
          (&DataType::Utf8, Type::BYTE_ARRAY) => Box::new(UTF8ArrayReader::new(
            batch_size,
            column_desc_ptr,
            DataType::Utf8,
            page_reader_iter,
            buffer_manager
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
      columns.insert(column.get_column_name().to_string(), column_info);
    } else {
      return Err(InvalidArgumentError(format!(
        "Column [{}] not found in schema.",
        column.get_column_name()
      )))?;
    }
  }

  Ok(ParquetReader { schema: Arc::new(schema), columns, meta, 
    buffers: HashMap::with_capacity(columns.len() * 3), root_buffer_manager: root_manager })
}
