use crate::error::{ArrowExecutorErrorKind, Result};
use crate::executor::{Context, Executor};
use crate::proto::plan::ParquetFileScanNode;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use failure::ResultExt;
use jni::JNIEnv;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::rc::Rc;
use crate::datasource::parquet::array_reader_builder::{ParquetRecordBatchReader,
                                                       RecordBatchReaderBuilder};

pub struct ParquetScanExecutor {
    reader: ParquetRecordBatchReader,
    schema: SchemaRef,
}

impl Executor for ParquetScanExecutor {
    fn output_schema(&self) -> &Schema {
        &self.schema
    }

    fn open(&mut self, _context: &Context) -> Result<()> {
        Ok(())
    }

    fn next(&mut self, _context: &Context) -> Result<Option<&RecordBatch>> {
        let record_batch = self.reader
            .next_batch()?;
        if record_batch.num_rows() > 0 {
            Ok(Some(record_batch))
        } else {
            Ok(None)
        }
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

impl ParquetScanExecutor {
    pub fn from_plan_node(
        parquet_scan_node: &ParquetFileScanNode,
        _jni_env: JNIEnv,
    ) -> Result<Box<dyn Executor>> {
        let file = File::open(parquet_scan_node.get_path())
            .context(ArrowExecutorErrorKind::ParquetError)?;
        let parquet_reader =
            SerializedFileReader::new(file).context(ArrowExecutorErrorKind::ParquetError)?;
        let parquet_schema = parquet_reader.metadata().file_metadata().schema_descr_ptr();
        
        let column_names: Vec<&str> = parquet_scan_node.get_schema().get_fields().iter()
            .map(|c| c.get_name())
            .collect();
        let parquet_batch_record_reader = RecordBatchReaderBuilder::new(parquet_schema,
                                                                        &column_names, 4096,
                                                                        Rc::new(parquet_reader),
                                                                        parquet_scan_node
                                                                            .get_path().to_string
                                                                        ()).build()?;

        let schema = parquet_batch_record_reader.schema();
        Ok(Box::new(Self {
            reader: parquet_batch_record_reader,
            schema,
        }) as Box<dyn Executor>)
    }
}
