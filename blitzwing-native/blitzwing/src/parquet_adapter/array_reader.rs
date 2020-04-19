use crate::{
  error::{BlitzwingErrorKind::ArrowError, Result},
  parquet_adapter::record_reader::{RecordReader, RecordReaderBuffers},
  types::ColumnDescProtoPtr,
  util::{
    buffer::{
      BooleanBufferBuilder, Buffer, BufferBuilder, BufferBuilderTrait, BufferData, BufferManager,
      Int32BufferBuilder,
    },
    num::Cast,
    reader::PageReaderIteratorRef,
  },
};
use arrow::{
  array::{
    Array, ArrayDataBuilder, ArrayDataRef, ArrayRef, BinaryArray, PrimitiveArray, StringArray,
  },
  datatypes::{
    ArrowNativeType, ArrowNumericType, DataType, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
  },
};
use failure::ResultExt;
use parquet::data_type::{
  ByteArray, ByteArrayType as ParquetByteArrayType, DataType as ParquetType,
  DoubleType as ParquetDoubleType, FloatType as ParquetFloatType, Int32Type as ParquetInt32Type,
  Int64Type as ParquetInt64Type,
};
use std::{convert::From, marker::PhantomData, mem::size_of, sync::Arc, vec::Vec};

pub trait ArrayReader {
  fn data_type(&self) -> &DataType;
  fn next_batch(&mut self) -> Result<usize>;
  fn collect(&mut self) -> Result<ArrayRef>;
  fn free_buffer(&mut self, address: *mut u8) -> Result<bool>;
}

pub(crate) type ArrayReaderRef = Box<dyn ArrayReader>;

pub struct PrimitiveArrayReader<A, P>
// where
//   A: ArrowPrimitiveType,
//   P: ParquetType,
//   P::T: ArrowNativeType + num::Num,
{
  arrow_data_buffer: Option<BufferBuilder<A>>,
  data_type: DataType,
  record_reader: RecordReader<P, Buffer, Buffer>,
  buffer_manager: BufferManager,
  // A temporary workaround before new buffer design merged into arrow community
  collected_buffers: Vec<BufferData>,
  batch_size: usize,
}

impl<A, P> PrimitiveArrayReader<A, P>
where
  A: ArrowNumericType,
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
      create_record_reader_buffers::<P, _, Buffer>(batch_size, &buffer_manager, || {
        buffer_manager.allocate_aligned(batch_size, false)
      })?,
      page_readers,
    )?;

    let arrow_data_buffer = if arrow_conversion_needed {
      Some(BufferBuilder::<A>::new(batch_size, buffer_manager.clone())?)
    } else {
      None
    };

    Ok(Self {
      arrow_data_buffer,
      data_type: A::get_data_type(),
      record_reader,
      buffer_manager,
      collected_buffers: Vec::with_capacity(4),
      batch_size,
    })
  }
}

impl<A, P> ArrayReader for PrimitiveArrayReader<A, P>
where
  A: ArrowNumericType,
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
    let mut buffers =
      self.record_reader.collect_buffers(create_record_reader_buffers::<P, _, Buffer>(
        self.batch_size,
        &self.buffer_manager,
        || self.buffer_manager.allocate_aligned(self.batch_size, false),
      )?);

    let mut array_data =
      ArrayDataBuilder::new(A::get_data_type()).len(num_values).null_count(null_count);

    if null_count > 0 {
      let mut null_buffer = buffers.null_bitmap.finish();
      self.collected_buffers.push(null_buffer.buffer_data());
      array_data = array_data.null_bit_buffer(unsafe { null_buffer.to_arrow_buffer() });
    }

    if let Some(arrow_builder) = &mut self.arrow_data_buffer {
      let values: &[P::T] = buffers.parquet_data_buffer.as_ref();
      for i in 0..num_values {
        arrow_builder.append(values[i].clone().cast()).context(ArrowError)?;
      }

      let mut arrow_buffer = arrow_builder.finish();
      self.collected_buffers.push(arrow_buffer.buffer_data());

      array_data = array_data.add_buffer(unsafe { arrow_buffer.to_arrow_buffer() });
    } else {
      self.collected_buffers.push(buffers.parquet_data_buffer.buffer_data());
      array_data = array_data.add_buffer(unsafe { buffers.parquet_data_buffer.to_arrow_buffer() });
    }

    let array = PrimitiveArray::<A>::from(array_data.build());
    Ok(Arc::new(array))
  }

  fn free_buffer(&mut self, address: *mut u8) -> Result<bool> {
    if let Some(pos) = self.collected_buffers.iter().position(|b| b.as_ptr() == address) {
      self.buffer_manager.deallocate(self.collected_buffers.remove(pos))?;
      Ok(true)
    } else {
      Ok(false)
    }
  }
}

pub struct VarLenArrayReader<A, P>
where
  A: From<ArrayDataRef> + Array + 'static,
  P: ParquetType,
{
  // arrow_offset_buffer: Int32BufferBuilder,
  data_type: DataType,
  record_reader: RecordReader<P, Vec<P::T>, Buffer>,
  buffer_manager: BufferManager,
  batch_size: usize,
  collected_buffers: Vec<BufferData>,
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
    buffer_manager: BufferManager,
  ) -> Result<Self> {
    let record_reader_buffers =
      create_record_reader_buffers::<P, _, _>(batch_size, &buffer_manager, || {
        let mut buffer = Vec::<P::T>::with_capacity(batch_size);
        buffer.resize_with(batch_size, P::T::default);
        Ok(buffer)
      })?;

    let record_reader = RecordReader::<P, Vec<P::T>, Buffer>::new(
      batch_size,
      column_desc,
      record_reader_buffers,
      page_readers,
    )?;

    Ok(Self {
      // arrow_data_buffer,
      // arrow_offset_buffer,
      data_type,
      record_reader,
      buffer_manager,
      batch_size,
      collected_buffers: Vec::with_capacity(4),
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

  fn next_batch(&mut self) -> Result<usize> {
    self.record_reader.next_batch()?;
    Ok(self.record_reader.get_num_values())
  }

  fn collect(&mut self) -> Result<ArrayRef> {
    let values_read = self.record_reader.get_num_values();
    let null_count = self.record_reader.get_null_count();
    let mut buffers = self.record_reader.collect_buffers(create_record_reader_buffers::<P, _, _>(
      self.batch_size,
      &self.buffer_manager,
      || {
        let mut buffer = Vec::<P::T>::with_capacity(self.batch_size);
        buffer.resize_with(self.batch_size, P::T::default);
        Ok(buffer)
      },
    )?);

    let mut array_data =
      ArrayDataBuilder::new(self.data_type.clone()).len(values_read).null_count(null_count);

    let data_len = (&buffers.parquet_data_buffer[0..values_read]).iter().map(|b| b.len()).sum();
    let mut arrow_data_buffer = self.buffer_manager.allocate_aligned(data_len, true)?;
    let mut arrow_offset_buffer =
      Int32BufferBuilder::new(self.batch_size + 1, self.buffer_manager.clone())?;
    // self.arrow_data_buffer.resize(data_len).context(ArrowError)?;

    let mut start: usize = 0;
    let arrow_data = arrow_data_buffer.as_mut();
    arrow_offset_buffer.append(start as i32).context(ArrowError)?;
    for b in &buffers.parquet_data_buffer[0..values_read] {
      let end = start + b.len() as usize;
      (&mut arrow_data[start..end]).copy_from_slice(b.data());
      start = end;
      arrow_offset_buffer.append(start as i32).context(ArrowError)?;
    }

    self.collected_buffers.push(arrow_data_buffer.buffer_data());

    let mut arrow_offset_buffer = arrow_offset_buffer.finish();
    self.collected_buffers.push(arrow_offset_buffer.buffer_data());

    array_data = array_data
      .add_buffer(unsafe { arrow_offset_buffer.to_arrow_buffer() })
      .add_buffer(unsafe { arrow_data_buffer.to_arrow_buffer() });

    if null_count > 0 {
      let mut null_buffer = buffers.null_bitmap.finish();
      self.collected_buffers.push(null_buffer.buffer_data());
      array_data = array_data.null_bit_buffer(unsafe { null_buffer.to_arrow_buffer() });
    }

    Ok(Arc::new(A::from(array_data.build())))
  }

  fn free_buffer(&mut self, address: *mut u8) -> Result<bool> {
    if let Some(pos) = self.collected_buffers.iter().position(|b| b.as_ptr() == address) {
      self.buffer_manager.deallocate(self.collected_buffers.remove(pos))?;
      Ok(true)
    } else {
      Ok(false)
    }
  }
}

fn create_record_reader_buffers<P, F, B>(
  batch_size: usize,
  buffer_manager: &BufferManager,
  func: F,
) -> Result<RecordReaderBuffers<B, Buffer>>
where
  P: ParquetType,
  F: FnOnce() -> Result<B>,
{
  let parquet_data_buffer = func()?;
  let def_levels = buffer_manager.allocate_aligned(batch_size * size_of::<i16>(), false)?;
  let null_bitmap = BooleanBufferBuilder::new(batch_size, buffer_manager.clone())?;

  Ok(RecordReaderBuffers { parquet_data_buffer, def_levels, null_bitmap })
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
