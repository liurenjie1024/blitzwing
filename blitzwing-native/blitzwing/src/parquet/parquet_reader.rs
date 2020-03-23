use blitzwing_common::proto::parquet::RowGroupProto;
use blitzwing_common::proto::parquet::ParquetReaderProto;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;
use crate::array_reader::ArrayReaderRef;
use crate::error::Result;
use std::collections::HashMap;
use arrow::datatypes::Field;

pub(crate) struct ParquetReader {
  schema: SchemaRef,
  array_readers: HashMap<String, ArrayReaderRef>,
  record_batch: Option<RecordBatch>,
  meta: ParquetReaderProto,
}

impl ParquetReader {
  pub(crate) fn set_data(&mut self, row_group_meta: RowGroupProto) -> Result<()> {
    for column_chunk in row_group_meta.get_columns() {
      if let Some(array_reader) = self.array_readers.get_mut(column_chunk.get_column_name()) {
        array_reader.set_data(column_chunk);
      } else {
        return Err(InvalidArgumentError(format!("Column name not found {}", column_chunk.get_column_name())).into());
      }
    }

    Ok(())
  }

  pub(crate) fn next_batch(&mut self) -> Result<&RecordBatch> {
    match &mut self.record_batch {
      Some(r) => {
          for (i, array_reader) in &mut self.array_readers.values().enumerate() {
              r.columns_mut()[i] = array_reader.next_batch()?;
          }
      },
      None => {
          let columns: Result<Vec<ArrayRef>> = self.array_readers.values().iter_mut()
              .map(|r| r.next_batch())
              .collect();
          
          let columns = columns.context(ArrowError)?;
          self.record_batch.replace(RecordBatch::try_new(self.schema.clone(), columns)
              .context(ArrowError)?);
      }
  }
  
    Ok(self.record_batch.as_ref().expect("Record batch should have been initialized!"))
  }

  pub(crate) fn reset_batch(&mut self) -> Result<()> {
    for array_reader in &mut self.array_readers.values_mut() {
      array_reader.reset_batch()?;
    }

    Ok(())
  }
}

pub(crate) fn create_parquet_reader(meta: ParquetReaderProto) -> Result<ParquetReader> {
  // let mut column_readers = HashMap::<String, ArrayReaderRef>::with_capacity(meta.get_column_desc().len());

  // let mut schema_map = HashMap::<String, Field>::with_capacity(meta.get_schema().get_fields().len());

  // for field in meta.get_schema().get_fields() {

  // }

  // for column_desc in meta.get_column_desc() {
  //   if meta.get_schema()
  //           let array_reader: Box<dyn ArrayReader> = match (column_desc.logical_type(), column_desc
  //               .physical_type()) {
  //               (LogicalType::INT_8, PhysicalType::INT32) => Box::new(Int8ArrayReader::new(
  //                   self.batch_size, true, column_desc, pages)?),
  //               (LogicalType::INT_16, PhysicalType::INT32) => Box::new(Int16ArrayReader::new(
  //                   self.batch_size, true, column_desc, pages)?),
  //               (LogicalType::UINT_8, PhysicalType::INT32) => Box::new(UInt8ArrayReader::new(
  //                   self.batch_size, true, column_desc, pages)?),
  //               (LogicalType::UINT_16, PhysicalType::INT32) => Box::new(UInt16ArrayReader::new(
  //                   self.batch_size, true, column_desc, pages)?),
  //               (LogicalType::UINT_32, PhysicalType::INT32) => Box::new(UInt32ArrayReader::new(
  //                   self.batch_size, false, column_desc, pages)?),
  //               (LogicalType::UINT_64, PhysicalType::INT64) => Box::new(UInt64ArrayReader::new(
  //                   self.batch_size, false, column_desc, pages)?),
  //               (_, PhysicalType::FLOAT) => Box::new(Float32ArrayReader::new(
  //                   self.batch_size, true, column_desc, pages)?),
  //               (_, PhysicalType::DOUBLE) => Box::new(Float64ArrayReader::new(
  //                   self.batch_size, true, column_desc, pages)?),
  //               (_, PhysicalType::INT32) => Box::new(Int32ArrayReader::new(
  //                   self.batch_size, false, column_desc, pages)?),
  //               (_, PhysicalType::INT64) => Box::new(Int64ArrayReader::new(
  //                   self.batch_size, false, column_desc, pages)?),
  //               (LogicalType::UTF8, PhysicalType::BYTE_ARRAY) => Box::new(UTF8ArrayReader::new(
  //                   self.batch_size, column_desc, pages, DataType::Utf8)?),
  //               (logical_type, physical_type) => Err(ArrowExecutorErrorKind::PlanError(
  //                   format!("Reading {:?}:{:?} from parquet is not supported yet!",
  //                           logical_type, physical_type)))?
  //           };
            
  //           array_readers.push(array_reader);
  //       }
        
  //       let schema = Arc::new(parquet_to_arrow_schema_by_columns(self.schema_desc.clone(),
  //           column_indexes).context(ArrowExecutorErrorKind::ParquetError)?);
        
  //       Ok(ParquetRecordBatchReader::new(schema.clone(), array_readers))
  unimplemented!()
}