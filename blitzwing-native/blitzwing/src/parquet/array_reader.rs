use crate::datasource::parquet::parquet_record_reader::ParquetRecordReader;
use crate::error::{ArrowExecutorErrorKind, Result};
use crate::num::Cast;
use arrow::array::{
    Array, ArrayDataBuilder, ArrayDataRef, ArrayRef, BufferBuilder, BufferBuilderTrait,
    Int32BufferBuilder,
    StringArray, BinaryArray
};
use arrow::array::{BooleanBufferBuilder, PrimitiveArray};
use arrow::buffer::MutableBuffer;
use arrow::datatypes::{ArrowPrimitiveType, DataType};
use failure::ResultExt;
use parquet::column::reader::PageReaderIterator;
use parquet::data_type::{ByteArray, DataType as ParquetType};
use parquet::schema::types::ColumnDescPtr;
use std::cell::RefCell;
use std::convert::From;
use std::intrinsics::transmute;
use std::marker::PhantomData;
use std::slice::from_raw_parts_mut;
use std::sync::Arc;
use std::vec::Vec;
use arrow::datatypes::{
    Int8Type,
    Int16Type,
    Int32Type,
    Int64Type,
    UInt8Type,
    UInt16Type,
    UInt32Type,
    UInt64Type,
    Float32Type,
    Float64Type
};
use parquet::data_type::{
    Int32Type as ParquetInt32Type,
    Int64Type as ParquetInt64Type,
    FloatType as ParquetFloatType,
    DoubleType as ParquetDoubleType,
    ByteArrayType as ParquetByteArrayType
};

pub trait ArrayReader {
    fn data_type(&self) -> &DataType;
    fn next_batch(&mut self) -> Result<ArrayRef>;
    fn set_data(&mut self, column_chunk: &ColumnChunkProto);
    fn reset_batch(&mut self) -> Result<()>;
}

pub(crate) type ArrayReaderRef = Box<dyn ArrayReader>;

type ConcatPageReader = SerializedPageReader<ConcatReader<Buffer>>;

pub struct PrimitiveArrayReader<A, P>
where
    A: ArrowPrimitiveType,
    P: ParquetType,
{
    arrow_data_buffer: Option<BufferBuilder<A>>,
    data_type: DataType,
    record_reader: RecordReader<P, ConcatPageReader, MutableBuffer>,
}

impl<A, P> PrimitiveArrayReader<A, P>
where
    A: ArrowPrimitiveType,
    P: ParquetType,
{
    pub fn new(
        batch_size: usize,
        arrow_conversion_needed: bool,
        max_def_level: i16,
        page_reader: ConcatPageReader,
    ) -> Result<Self> {
        let parquet_data_buffer = MutableBuffer::new(batch_size * P::get_type_size());
        let arrow_data_buffer = if arrow_conversion_needed {
            Some(BufferBuilder::<A>::new(batch_size))
        } else {
            None
        };
        let record_reader = RecordReader::<P, ConcatPageReader, MutableBuffer>::new(batch_size, max_def_level, parquet_data_buffer, page_reader)?;

        Ok(Self {
            arrow_data_buffer,
            data_type: A::get_data_type(),
            record_reader,
        })
    }
}

impl<A, P> ArrayReader for PrimitiveArrayReader<A, P>
where
    A: ArrowPrimitiveType,
    P: ParquetType,
    P::T: Cast<A::Native>,
{
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn next_batch(&mut self) -> Result<ArrayRef> {
        self.record_reader.next_batch()?;

        let mut array_data = ArrayDataBuilder::new(A::get_data_type())
            .len(self.record_reader.get_num_values())
            .null_count(self.record_reader.get_num_nulls());

        if self.record_reader.get_num_nulls() > 0 {
            array_data = array_data.null_bit_buffer(unsafe { self.record_reader.get_null_bitmap().finish_shared() });
        }

        if let Some(arrow_builder) = &mut self.arrow_data_buffer {
            for i in 0..values_read {
                arrow_builder
                    .append(values[i].clone().cast())
                    .context(ArrowError)?;
            }

            array_data = array_data.add_buffer(unsafe { arrow_builder.finish_shared() });
        } else {
            array_data = array_data.add_buffer(unsafe { self.record_reader.get_parquet_data_buffer().freeze_shared() });
        }

        let array = PrimitiveArray::<A>::from(array_data.build());
        Ok(Arc::new(array))
    }
}

pub struct VarLenArrayReader<A, P>
where
    A: From<ArrayDataRef> + Array + 'static,
    P: ParquetType,
{
    arrow_data_buffer: MutableBuffer,
    arrow_offset_buffer: Int32BufferBuilder,
    data_type: DataType,
    record_reader: RecordReader<P, ConcatPageReader, Vec<P::T>>,
    _phantom_arrow: PhantomData<A>,
    _phantom_parquet: PhantomData<P>,
}

impl<A, P> VarLenArrayReader<A, P>
    where
        A: From<ArrayDataRef> + Array + 'static,
        P: ParquetType,
{
    pub fn new(batch_size: usize,
               max_def_level: i16,
               page_reader: ConcatPageReader,
               data_type: DataType) -> Result<Self> {
        let mut parquet_data_buffer = Vec::with_capacity(batch_size);
        parquet_data_buffer.resize_with(batch_size, P::T::default);
        
        let arrow_data_buffer = MutableBuffer::new(batch_size);
        let arrow_offset_buffer = Int32BufferBuilder::new(batch_size + 1);
        let record_reader = RecordReader::<P, ConcatPageReader, MutableBuffer>::new(batch_size, max_def_level, parquet_data_buffer, page_reader)?;
        
        Ok(Self {
            arrow_data_buffer,
            arrow_offset_buffer,
            data_type,
            record_reader,
            _phantom_arrow: PhantomData,
            _phantom_parquet: PhantomData
        })
    }
}

impl<A, P> ArrayReader for VarLenArrayReader<A, P>
where
    A: From<ArrayDataRef> + Array + 'static,
    P: ParquetType<T = ByteArray>,
{
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn next_batch(&mut self) -> Result<ArrayRef> {
        self.record_reader.next_batch()?;

        let values_read = self.record_reader.get_num_values();
        let null_count = self.record_reader.get_num_nulls();
        let parquet_data_buffer = self.record_reader.get_parquet_data_buffer();

        let mut array_data = ArrayDataBuilder::new(self.data_type.clone())
            .len(values_read)
            .null_count(null_count);

        let data_len = (&parquet_data_buffer[0..values_read])
            .iter()
            .map(|b| b.len())
            .sum();
        self.arrow_data_buffer
            .resize(data_len)
            .context(ArrowError)?;

        let mut start: usize = 0;
        let arrow_data = self.arrow_data_buffer.data_mut();
        self.arrow_offset_buffer
            .append(start as i32)
            .context(ArrowExecutorErrorKind::ArrowError)?;
        for b in &parquet_data_buffer[0..values_read] {
            let end = start + b.len() as usize;
            (&mut arrow_data[start..end]).copy_from_slice(b.data());
            start = end;
            self.arrow_offset_buffer
                .append(start as i32)
                .context(ArrowExecutorErrorKind::ArrowError)?;
        }

        array_data = array_data
            .add_buffer(unsafe { self.arrow_offset_buffer.finish_shared() })
            .add_buffer(unsafe { self.arrow_data_buffer.freeze_shared() });

        if null_count > 0 {
            array_data = array_data.null_bit_buffer(unsafe { self.record_reader.get_null_bitmap().finish_shared() });
        }

        Ok(Arc::new(A::from(array_data.build())))
    }
}

pub type Int8ArrayReader = PrimitiveArrayReader<Int8Type, ParquetInt32Type>;
pub type Int16ArrayReader = PrimitiveArrayReader<Int16Type, ParquetInt32Type>;
pub type Int32ArrayReader = PrimitiveArrayReader<Int32Type, ParquetInt32Type>;
pub type Int64ArrayReader = PrimitiveArrayReader<Int64Type, ParquetInt64Type>;
pub type UInt8ArrayReader = PrimitiveArrayReader<UInt8Type, ParquetInt32Type>;
pub type UInt16ArrayReader = PrimitiveArrayReader<UInt16Type, ParquetInt32Type>;
pub type UInt32ArrayReader = PrimitiveArrayReader<UInt32Type, ParquetInt32Type>;
pub type UInt64ArrayReader = PrimitiveArrayReader<UInt64Type, ParquetInt64Type>;
pub type Float32ArrayReader = PrimitiveArrayReader<Float32Type, ParquetFloatType>;
pub type Float64ArrayReader = PrimitiveArrayReader<Float64Type, ParquetDoubleType>;
pub type UTF8ArrayReader = VarLenArrayReader<StringArray, ParquetByteArrayType>;
pub type BinaryArrayReader = VarLenArrayReader<BinaryArray, ParquetByteArrayType>;
