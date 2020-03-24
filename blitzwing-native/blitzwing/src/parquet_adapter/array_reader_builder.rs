use crate::array_reader::ArrayReader;
use crate::datasource::parquet::array_reader::{ArrayReader,
                                               Int8ArrayReader,
                                               Int16ArrayReader, Float32ArrayReader,
                                               Int32ArrayReader, Int64ArrayReader,
                                               UInt8ArrayReader, UInt16ArrayReader,
                                               UInt32ArrayReader, UInt64ArrayReader,
                                               Float64ArrayReader,
                                               UTF8ArrayReader};
use crate::error::{Result, ArrowExecutorErrorKind};
use parquet::schema::types::SchemaDescPtr;
use parquet::file::reader::{FileReader, FilePageIterator};
use std::rc::Rc;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{SchemaRef, DataType};
use parquet::basic::{LogicalType, Type as PhysicalType};
use parquet::arrow::parquet_to_arrow_schema_by_columns;
use std::sync::Arc;
use arrow::array::ArrayRef;
use failure::ResultExt;

pub struct RecordBatchReaderBuilder<'a, S: AsRef<str>>
{
    schema_desc: SchemaDescPtr,
    column_names: &'a [S],
    batch_size: usize,
    parquet_file_reader: Rc<dyn FileReader>,
    parquet_file_path: String,
}

pub struct ParquetRecordBatchReader {
    array_readers: Vec<Box<dyn ArrayReader>>,
    schema: SchemaRef,
    record_batch: Option<RecordBatch>,
}

impl ParquetRecordBatchReader {
    pub fn new(schema: SchemaRef, array_readers: Vec<Box<dyn ArrayReader>>) -> Self {
        Self {
            array_readers,
            schema,
            record_batch: None
        }
    }
    
    pub fn schema(&self) -> SchemaRef { self.schema.clone() }
    
    pub fn next_batch(&mut self) -> Result<&RecordBatch> {
        match &mut self.record_batch {
            Some(r) => {
                for i in 0..self.array_readers.len() {
                    r.columns_mut()[i] = (&mut self.array_readers[i]).next_batch()?;
                }
            },
            None => {
                let columns: Result<Vec<ArrayRef>> = self.array_readers.iter_mut()
                    .map(|r| r.next_batch())
                    .collect();
                
                let columns = columns.context(ArrowExecutorErrorKind::ArrowError)?;
                self.record_batch.replace(RecordBatch::try_new(self.schema.clone(), columns)
                    .context(ArrowExecutorErrorKind::ArrowError)?);
            }
        }
        
        Ok(self.record_batch.as_ref().expect("Record batch should have been initialized!"))
    }
}

impl<'a, S: AsRef<str>> RecordBatchReaderBuilder<'a, S>
{
    pub fn new(schema_desc: SchemaDescPtr, column_names: &'a [S], batch_size: usize,
               parquet_file_reader: Rc<dyn FileReader>, parquet_file_path: String) -> Self {
        Self {
            schema_desc,
            column_names,
            batch_size,
            parquet_file_reader,
            parquet_file_path,
        }
    }
    pub fn build(self) -> Result<ParquetRecordBatchReader> {
//        let mut names_to_idx = HashMap::with_capacity(self.column_names.len());
        let mut column_indexes = Vec::with_capacity(self.column_names.len());
        let mut array_readers = Vec::with_capacity(self.column_names.len());
        
        for column in self.column_names {
            let column_index = self.schema_desc.columns().iter()
                .position(|c| c.name() == column.as_ref())
                .ok_or_else(|| ArrowExecutorErrorKind::PlanError(format!("Unable to find column \
                {} in parquet file: {}", column.as_ref(), self.parquet_file_path).into()))?;
            
//            names_to_idx.insert(column.as_ref(), column_index);
            column_indexes.push(column_index);
            
            let column_desc = self.schema_desc.column(column_index);
            let pages = Box::new(FilePageIterator::new(
                column_index,
                self.parquet_file_reader.clone(),
            ).context(ArrowExecutorErrorKind::ArrowError)?);
            
            let array_reader: Box<dyn ArrayReader> = match (column_desc.logical_type(), column_desc
                .physical_type()) {
                (LogicalType::INT_8, PhysicalType::INT32) => Box::new(Int8ArrayReader::new(
                    self.batch_size, true, column_desc, pages)?),
                (LogicalType::INT_16, PhysicalType::INT32) => Box::new(Int16ArrayReader::new(
                    self.batch_size, true, column_desc, pages)?),
                (LogicalType::UINT_8, PhysicalType::INT32) => Box::new(UInt8ArrayReader::new(
                    self.batch_size, true, column_desc, pages)?),
                (LogicalType::UINT_16, PhysicalType::INT32) => Box::new(UInt16ArrayReader::new(
                    self.batch_size, true, column_desc, pages)?),
                (LogicalType::UINT_32, PhysicalType::INT32) => Box::new(UInt32ArrayReader::new(
                    self.batch_size, false, column_desc, pages)?),
                (LogicalType::UINT_64, PhysicalType::INT64) => Box::new(UInt64ArrayReader::new(
                    self.batch_size, false, column_desc, pages)?),
                (_, PhysicalType::FLOAT) => Box::new(Float32ArrayReader::new(
                    self.batch_size, true, column_desc, pages)?),
                (_, PhysicalType::DOUBLE) => Box::new(Float64ArrayReader::new(
                    self.batch_size, true, column_desc, pages)?),
                (_, PhysicalType::INT32) => Box::new(Int32ArrayReader::new(
                    self.batch_size, false, column_desc, pages)?),
                (_, PhysicalType::INT64) => Box::new(Int64ArrayReader::new(
                    self.batch_size, false, column_desc, pages)?),
                (LogicalType::UTF8, PhysicalType::BYTE_ARRAY) => Box::new(UTF8ArrayReader::new(
                    self.batch_size, column_desc, pages, DataType::Utf8)?),
                (logical_type, physical_type) => Err(ArrowExecutorErrorKind::PlanError(
                    format!("Reading {:?}:{:?} from parquet is not supported yet!",
                            logical_type, physical_type)))?
            };
            
            array_readers.push(array_reader);
        }
        
        let schema = Arc::new(parquet_to_arrow_schema_by_columns(self.schema_desc.clone(),
            column_indexes).context(ArrowExecutorErrorKind::ParquetError)?);
        
        Ok(ParquetRecordBatchReader::new(schema.clone(), array_readers))
    }
}

