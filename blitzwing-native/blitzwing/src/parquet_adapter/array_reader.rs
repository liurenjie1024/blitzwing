use crate::{
  error::{BlitzwingErrorKind::ArrowError, Result},
  parquet_adapter::record_reader::{RecordReader, RecordReaderBuffers},
  types::ColumnDescProtoPtr,
  util::{buffer::Buffer, num::Cast, reader::PageReaderIteratorRef},
};
use arrow::{
  array::{
    Array, ArrayDataBuilder, ArrayDataRef, ArrayRef, BinaryArray, BufferBuilder,
    BufferBuilderTrait, Int32BufferBuilder, PrimitiveArray, StringArray,
  },
  buffer::MutableBuffer,
  datatypes::{
    ArrowNativeType, ArrowPrimitiveType, DataType, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
  },
};
use failure::ResultExt;
use parquet::data_type::{
  ByteArray, ByteArrayType as ParquetByteArrayType, DataType as ParquetType,
  DoubleType as ParquetDoubleType, FloatType as ParquetFloatType, Int32Type as ParquetInt32Type,
  Int64Type as ParquetInt64Type,
};
use std::{convert::From, marker::PhantomData, sync::Arc, vec::Vec, mem::size_of};
use crate::util::buffer::{BufferManager, BooleanBufferBuilder, BufferBuilderTrait};


pub trait ArrayReader {
  fn data_type(&self) -> &DataType;
  fn next_batch(&mut self) -> Result<usize>;
  fn collect(&mut self) -> Result<ArrayRef>;
}

pub(crate) type ArrayReaderRef = Box<dyn ArrayReader>;

pub struct PrimitiveArrayReader<A, P>
where
  A: ArrowPrimitiveType,
  P: ParquetType,
  P::T: ArrowNativeType + num::Num,
{
  arrow_data_buffer: Option<BufferBuilder<A>>,
  data_type: DataType,
  record_reader: RecordReader<P, Buffer, Buffer>,
  buffer_manager: BufferManager,
}


impl<A, P> PrimitiveArrayReader<A, P>
where
  A: ArrowPrimitiveType,
  P: ParquetType,
  P::T: ArrowNativeType + num::Num,
{
  pub(crate) fn new(
    batch_size: usize,
    arrow_conversion_needed: bool,
    column_desc: ColumnDescProtoPtr,
    page_readers: PageReaderIteratorRef,
    buffer_manager: BufferManager,
  ) -> Result<Self> {

    let record_reader = RecordReader::<P, Buffer, Buffer>::new(
      batch_size,
      column_desc,
      create_record_reader_buffers(batch_size, &buffer_manager)?,
      page_readers,
    )?;

    let arrow_data_buffer =
      if arrow_conversion_needed { Some(BufferBuilder::<A>::new(batch_size)) } else { None };

    Ok(Self { arrow_data_buffer, 
      data_type: A::get_data_type(), record_reader, buffer_manager })
  }
}

impl<A, P> ArrayReader for PrimitiveArrayReader<A, P>
where
  A: ArrowPrimitiveType,
  P: ParquetType,
  P::T: Cast<A::Native>,
  P::T: ArrowNativeType + num::Num,
{
  fn data_type(&self) -> &DataType {
    &self.data_type
  }

  fn next_batch(&mut self) -> Result<usize> {
    self.record_reader.next_batch()?;
    Ok(self.record_reader.get_num_values())

  }

  fn collect(&mut self) -> Result<ArrayRef> {
    let num_values = self.record_reader.get_num_values();
    let null_count = self.record_reader.get_null_count();
    let buffers = self.record_reader.collect_buffers(create_record_reader_buffers(self.batch_size, &self.buffer_manager)?);

    let mut array_data = ArrayDataBuilder::new(A::get_data_type())
      .len(num_values)
      .null_count(null_count);

    if null_count > 0 {
      array_data =
        array_data.null_bit_buffer(unsafe { buffers.null_bitmap.finish() });
    }

    if let Some(arrow_builder) = &mut self.arrow_data_buffer {
      let values: &[P::T] = buffers.parquet_data_buffer.as_ref();
      for i in num_values {
        arrow_builder.append(values[i].clone().cast()).context(ArrowError)?;
      }

      array_data = array_data.add_buffer(unsafe { arrow_builder.finish() });
    } else {
      array_data = array_data
        .add_buffer(unsafe { buffers.parquet_data_buffer });
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
  record_reader: RecordReader<P, Vec<P::T>, Buffer>,
  _phantom_arrow: PhantomData<A>,
  _phantom_parquet: PhantomData<P>,
}

impl<A, P> VarLenArrayReader<A, P>
where
  A: From<ArrayDataRef> + Array + 'static,
  P: ParquetType,
{
  pub(crate) fn new(
    batch_size: usize,
    column_desc: ColumnDescProtoPtr,
    data_type: DataType,
    page_readers: PageReaderIteratorRef,
  ) -> Result<Self> {
    let mut parquet_data_buffer = Vec::with_capacity(batch_size);
    parquet_data_buffer.resize_with(batch_size, P::T::default);

    let arrow_data_buffer = MutableBuffer::new(batch_size);
    let arrow_offset_buffer = Int32BufferBuilder::new(batch_size + 1);
    let record_reader = RecordReader::<P, Vec<P::T>>::new(
      batch_size,
      column_desc,
      parquet_data_buffer,
      page_readers,
    )?;

    Ok(Self {
      arrow_data_buffer,
      arrow_offset_buffer,
      data_type,
      record_reader,
      _phantom_arrow: PhantomData,
      _phantom_parquet: PhantomData,
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
    let null_count = self.record_reader.get_null_count();
    let parquet_data_buffer = self.record_reader.parquet_data();

    let mut array_data =
      ArrayDataBuilder::new(self.data_type.clone()).len(values_read).null_count(null_count);

    let data_len = (&parquet_data_buffer[0..values_read]).iter().map(|b| b.len()).sum();
    self.arrow_data_buffer.resize(data_len).context(ArrowError)?;

    let mut start: usize = 0;
    let arrow_data = self.arrow_data_buffer.data_mut();
    self.arrow_offset_buffer.append(start as i32).context(ArrowError)?;
    for b in &parquet_data_buffer[0..values_read] {
      let end = start + b.len() as usize;
      (&mut arrow_data[start..end]).copy_from_slice(b.data());
      start = end;
      self.arrow_offset_buffer.append(start as i32).context(ArrowError)?;
    }

    array_data = array_data
      .add_buffer(unsafe { self.arrow_offset_buffer.finish_shared() })
      .add_buffer(unsafe { self.arrow_data_buffer.freeze_shared() });

    if null_count > 0 {
      array_data =
        array_data.null_bit_buffer(unsafe { self.record_reader.get_null_bitmap().finish_shared() });
    }

    Ok(Arc::new(A::from(array_data.build())))
  }

  fn reset_batch(&mut self) -> Result<()> {
    Ok(self.record_reader.reset_batch())
  }
}

fn create_record_reader_buffers(batch_size: usize, buffer_manager: &BufferManager) -> Result<RecordReaderBuffers<Buffer, Buffer>> {
    let parquet_data_buffer = buffer_manager.allocate_aligned(batch_size * P::get_type_size())?;
    let def_levels = buffer_manager.allocate_aligned(batch_size * size_of::<i16>())?;
    let null_bitmap = BooleanBufferBuilder::new(batch_size, buffer_manager.clone())?;

    Ok(RecordReaderBuffers {
      parquet_data_buffer,
      def_levels,
      null_bitmap
    })
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
