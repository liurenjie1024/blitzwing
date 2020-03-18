use parquet::column::reader::{ColumnReaderImpl, PageReaderIterator};
use parquet::data_type::DataType as ParquetType;
use parquet::schema::types::ColumnDescPtr;
use std::cell::RefCell;
use std::cmp::max;

use crate::error::{BlitzwingParquetErrorKind, Result};
use arrow::array::{BooleanBufferBuilder, BufferBuilderTrait};
use failure::ResultExt;

pub struct ParquetRecordReader<T>
where
    T: ParquetType,
{
    column_desc: ColumnDescPtr,
    column_reader: ColumnReaderImpl<T>,
}

impl<T> ParquetRecordReader<T>
where
    T: ParquetType,
{
    pub fn new(desc: ColumnDescPtr, page_reader_iterator: Box<PageReaderIterator>) -> Result<Self> {
        Ok(Self {
            column_desc: desc.clone(),
            column_reader: ColumnReaderImpl::with_page_iterator(desc.clone(), page_reader_iterator)
                .context(ArrowExecutorErrorKind::ParquetError)?,
        })
    }
}

impl<T> ParquetRecordReader<T>
where
    T: ParquetType,
{
    pub fn next_batch(
        &mut self,
        batch_size: usize,
        values: &mut [T::T],
        def_levels: &RefCell<Option<Vec<i16>>>,
        rep_levels: &RefCell<Option<Vec<i16>>>,
        null_bitmap_builder: &mut BooleanBufferBuilder,
    ) -> Result<(usize, usize)> {
        let mut def_levels = def_levels.borrow_mut();
        let (values_read, levels_read) = self
            .column_reader
            .read_batch(
                batch_size,
                def_levels.as_mut().map(Vec::as_mut_slice),
                rep_levels.borrow_mut().as_mut().map(Vec::as_mut_slice),
                values,
            )
            .context(ArrowExecutorErrorKind::ParquetError)?;

        let mut null_count = 0;
        if values_read < levels_read {
            null_count = levels_read - values_read;
            null_bitmap_builder.reserve(levels_read).unwrap();
            null_bitmap_builder.set_len(levels_read);
//            let def_levels = def_levels
//                .as_mut()
//                .map(Vec::as_mut_slice)
//                .expect("Definition levels must exits for nullable column!");
//            // Fill in spaces
//            let (mut level_pos, mut data_pos) = (levels_read, values_read);
//            while level_pos > 0 && data_pos > 0 {
//                if def_levels[level_pos - 1] == self.column_desc.max_def_level() {
//                    values.swap(level_pos - 1, data_pos - 1);
//                    level_pos -= 1;
//                    data_pos -= 1;
//                    null_count += 1;
//                } else {
//                    level_pos -= 1;
//                }
//            }
//
//            // For null bitmap, we must go through all data since we need to clear states left by
//            // last batch
//            for i in 0..levels_read {
//                if def_levels[i] == self.column_desc.max_def_level() {
//                    null_bitmap_builder
//                        .append(true).unwrap();
//                } else {
//                    null_bitmap_builder
//                        .append(false).unwrap();
//                }
//            }
        }

        Ok((max(values_read, levels_read), null_count))
    }
}
