use crate::{
  error::{BlitzwingError, BlitzwingErrorKind::ParquetError, Result},
  proto::parquet::{ColumnChunkProto, ColumnDescProto, SegmentProto},
  util::{buffer::ops::BufferOps, TryIterator},
};
use arrow::buffer::Buffer;
use failure::ResultExt;
use parquet::{column::page::PageReader, file::reader::SerializedPageReader};
use std::{
  convert::TryInto,
  io::{Cursor, Read},
  mem::transmute,
};

pub(crate) type PageReaderRef = Box<dyn PageReader>;
pub(crate) type PageReaderIteratorRef =
  Box<dyn TryIterator<Error = BlitzwingError, Item = PageReaderRef>>;

pub(crate) fn create_page_reader(
  column_desc: &ColumnDescProto,
  column_chunk: &ColumnChunkProto,
) -> Result<impl PageReader> {
  let reader = column_chunk_to_read(column_chunk)?;
  let num_values = column_chunk.get_num_values();
  let compression = column_desc.get_compression().try_into()?;
  let physical_type = column_desc.get_physical_type().try_into()?;

  Ok(
    SerializedPageReader::new(reader, num_values, compression, physical_type)
      .context(ParquetError)?,
  )
}

pub(crate) fn page_reader_from_read<R: Read>(
  column_desc: &ColumnDescProto,
  reader: R,
  num_values: i64,
) -> Result<SerializedPageReader<R>> {
  Ok(
    SerializedPageReader::new(
      reader,
      num_values,
      column_desc.get_compression().try_into()?,
      column_desc.get_physical_type().try_into()?,
    )
    .context(ParquetError)?,
  )
}

pub(crate) fn column_chunk_to_read(column_chunk: &ColumnChunkProto) -> Result<impl Read> {
  let empty_reader: Box<dyn Read> = Box::new(Cursor::new(Vec::<u8>::new()));
  column_chunk
    .get_segments()
    .iter()
    .map(segment_to_read)
    .try_fold(empty_reader, |b, r| r.map(|rr| Box::new(rr.chain(b)) as Box<dyn Read>))
}

pub(crate) fn segment_to_read(segment: &SegmentProto) -> Result<impl Read> {
  segment_to_buffer(segment).map(|b| Cursor::new(BufferOps::new(b)))
}

pub(crate) fn segment_to_buffer(segment: &SegmentProto) -> Result<Buffer> {
  unsafe {
    Ok(Buffer::from_unowned(
      transmute(segment.get_address()),
      segment.get_length() as usize,
      segment.get_length() as usize,
    ))
  }
}
