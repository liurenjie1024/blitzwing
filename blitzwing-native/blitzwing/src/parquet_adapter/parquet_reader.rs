use arrow::ipc::convert::schema_from_bytes;
use std::rc::Rc;
use crate::error::BlitzwingErrorKind::ArrowError;
use arrow::array::ArrayRef;
use crate::error::BlitzwingErrorKind::InvalidArgumentError;
use crate::proto::parquet::RowGroupProto;
use crate::proto::parquet::ParquetReaderProto;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;
use crate::parquet_adapter::array_reader::ArrayReaderRef;
use crate::error::Result;
use std::collections::HashMap;
use failure::ResultExt;
use crate::util::concat_reader::create_page_reader;
use arrow::datatypes::DataType;
use parquet::basic::Type;
use std::convert::TryInto;
use crate::parquet_adapter::array_reader::*;
use std::sync::Arc;
use crate::util::shared_queue::SharedQueue;
use crate::types::ColumnDescProtoPtr;
use crate::util::concat_reader::{PageReaderRef, PageReaderIteratorRef};

struct ColumnInfo {
  array_reader: ArrayReaderRef,
  page_readers: SharedQueue<PageReaderRef>,
  column_desc: ColumnDescProtoPtr,
}

pub(crate) struct ParquetReader {
  schema: SchemaRef,
  columns: HashMap<String, ColumnInfo>,
  record_batch: Option<RecordBatch>,
  meta: ParquetReaderProto,
}

impl ParquetReader {
  pub(crate) fn set_data(&mut self, row_group_meta: RowGroupProto) -> Result<()> {
    for column_chunk in row_group_meta.get_columns() {
      if let Some(column) = self.columns.get_mut(column_chunk.get_column_name()) {
        let page_reader = Box::new(create_page_reader(column.column_desc.as_ref(), column_chunk)?);
        column.page_readers.push(page_reader)?;
      } else {
        return Err(InvalidArgumentError(format!("Column name not found {}", column_chunk.get_column_name())))?;
      }
    }

    Ok(())
  }

  pub(crate) fn next_batch(&mut self) -> Result<&RecordBatch> {
    match &mut self.record_batch {
      Some(r) => {
          for (i, column) in self.columns.values_mut().enumerate() {
              r.columns_mut()[i] = column.array_reader.next_batch()?;
          }
      },
      None => {
          let columns = self.columns.values_mut()
              .map(|r| r.array_reader.next_batch())
              .collect::<Result<Vec<ArrayRef>>>()
              .context(ArrowError)?;
          
          self.record_batch.replace(RecordBatch::try_new(self.schema.clone(), columns)
              .context(ArrowError)?);
      }
  }
  
    Ok(self.record_batch.as_ref().expect("Record batch should have been initialized!"))
  }

  pub(crate) fn reset_batch(&mut self) -> Result<()> {
    for column in &mut self.columns.values_mut() {
      column.array_reader.reset_batch()?;
    }

    Ok(())
  }
}

pub(crate) fn create_parquet_reader(meta: ParquetReaderProto) -> Result<ParquetReader> {
  let mut columns = HashMap::<String, ColumnInfo>::with_capacity(meta.get_column_desc().len());

  let schema = schema_from_bytes(meta.get_schema())
    .ok_or_else(|| InvalidArgumentError("Can't build arrow schema!".to_string()))?;

  for column in meta.get_column_desc() {
    if let Some((_, field)) = schema.column_with_name(column.get_column_name()) {
      let column_desc = Rc::new(column.clone());
      let batch_size = meta.get_batch_size() as usize;
      let page_readers = SharedQueue::new();

      let page_reader_iter: PageReaderIteratorRef = Box::new(page_readers.clone().into_iterator());
      let column_desc_ptr = column_desc.clone();

      let array_reader: ArrayReaderRef = match (field.data_type(), column.get_physical_type().try_into()?) {
        (&DataType::Int8, Type::INT32) => Box::new(Int8ArrayReader::new(
                    batch_size, true, column_desc_ptr, page_reader_iter)?),
        (&DataType::Int16, Type::INT32) => Box::new(Int16ArrayReader::new(
                    batch_size, true, column_desc_ptr, page_reader_iter)?),
        (&DataType::Int32, Type::INT32) => Box::new(Int32ArrayReader::new(
                    batch_size, false, column_desc_ptr, page_reader_iter)?),
        (&DataType::UInt8, Type::INT32) => Box::new(UInt8ArrayReader::new(
                    batch_size, true, column_desc_ptr, page_reader_iter)?),
        (&DataType::UInt16, Type::INT32) => Box::new(UInt16ArrayReader::new(
                    batch_size, true, column_desc_ptr, page_reader_iter)?),
        (&DataType::UInt32, Type::INT32) => Box::new(UInt32ArrayReader::new(
                    batch_size, false, column_desc_ptr, page_reader_iter)?),
        (&DataType::Int64, Type::INT64) => Box::new(Int64ArrayReader::new(
                    batch_size, false, column_desc_ptr, page_reader_iter)?),
        (&DataType::UInt64, Type::INT64) => Box::new(UInt64ArrayReader::new(
                    batch_size, false, column_desc_ptr, page_reader_iter)?),
        (&DataType::Float32, Type::FLOAT) => Box::new(Float32ArrayReader::new(
                    batch_size, false, column_desc_ptr, page_reader_iter)?),
        (&DataType::Float64, Type::DOUBLE) => Box::new(Float64ArrayReader::new(
                    batch_size, false, column_desc_ptr, page_reader_iter)?),
        (&DataType::Utf8, Type::BYTE_ARRAY) => Box::new(UTF8ArrayReader::new(
                    batch_size, column_desc_ptr, DataType::Utf8, page_reader_iter)?),
        (dt, parquet_type) => return Err(nyi!("Reading {:?} array from parquet type {:?} is not supported!", dt, parquet_type)),
      };

      let column_info = ColumnInfo {
        array_reader,
        page_readers,
        column_desc
      };
      columns.insert(column.get_column_name().to_string(), column_info);
    } else {
      return Err(InvalidArgumentError(format!("Column [{}] not found in schema.", column.get_column_name())))?;
    }
  }

  Ok(ParquetReader {
    schema: Arc::new(schema),
    columns,
    record_batch: None,
    meta
  })
}