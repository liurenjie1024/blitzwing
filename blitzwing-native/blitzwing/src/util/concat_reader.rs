use crate::proto::parquet::SegmentProto;
use crate::error::BlitzwingErrorKind::ParquetError;
use crate::proto::parquet::ColumnChunkProto;
use crate::proto::parquet::ColumnDescProto;
use crate::util::buffer::BufferOps;
use std::io::Cursor;
use parquet::file::reader::SerializedPageReader;
use std::io::{Read, Result as IoResult};
use crate::error::Result;
use std::convert::TryInto;
use failure::ResultExt;
use arrow::buffer::Buffer;
use parquet::column::page::PageReader;
use std::mem::transmute;
use crate::error::BlitzwingError;
use crate::util::TryIterator;

pub(crate) struct ConcatReader<T: Read> {
  readers: Vec<T>,
  idx: usize
}

impl<T: Read> Read for ConcatReader<T> {
  fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
    let mut bytes_read: usize = 0;

    loop {
      if self.idx >= self.readers.len() || bytes_read >= buf.len() {
        break;
      }

      let current_buf = &mut buf[bytes_read..];

      let batch_read = (&mut self.readers[self.idx]).read(current_buf)?;

      bytes_read += batch_read;

      if batch_read == 0 {
        self.idx += 1;
      }
    }

    Ok(bytes_read)
  }
}

impl<T: Read> ConcatReader<T> {
  fn new(readers: Vec<T>) -> Self {
    Self {
      readers,
      idx: 0
    }
  }
}


pub(crate) type PageReaderRef = Box<dyn PageReader>;
pub(crate) type PageReaderIteratorRef = Box<dyn TryIterator<Error = BlitzwingError, Item = PageReaderRef>>;

pub(crate) fn create_page_reader(column_desc: &ColumnDescProto, column_chunk: &ColumnChunkProto) -> Result<impl PageReader> {
  let reader = column_chunk_to_read(column_chunk)?;
  let num_values = column_chunk.get_num_values();
  let compression = column_desc.get_compression().try_into()?;
  let physical_type = column_desc.get_physical_type().try_into()?;

  Ok(SerializedPageReader::new(reader, num_values, compression, physical_type)
    .context(ParquetError)?)
}

pub(crate) fn empty_page_reader(column_desc: &ColumnDescProto) -> Result<impl PageReader> {
  let reader = Cursor::new(Vec::<u8>::new()) ;
  let num_values = 0;
  let compression = column_desc.get_compression().try_into()?;
  let physical_type = column_desc.get_physical_type().try_into()?;

  Ok(SerializedPageReader::new(reader, num_values, compression, physical_type)
    .context(ParquetError)?)
}


pub(crate) fn page_reader_from_read<R: Read>(column_desc: &ColumnDescProto, reader: R, num_values:  i64) -> Result<SerializedPageReader<R>> {
  Ok(SerializedPageReader::new(reader, num_values, column_desc.get_compression().try_into()?, column_desc.get_physical_type().try_into()?).context(ParquetError)?)
}

pub(crate) fn column_chunk_to_read(column_chunk: &ColumnChunkProto) -> Result<impl Read> {
  let readers = column_chunk.get_segments()
  .iter()
  .map(segment_to_read)
  .collect::<Result<Vec<_>>>()?;

  Ok(ConcatReader::new(readers))
}

pub(crate) fn segment_to_read(segment: &SegmentProto) -> Result<impl Read> {
  segment_to_buffer(segment)
    .map(|b| Cursor::new(BufferOps::new(b)) )
}

pub(crate) fn segment_to_buffer(segment: &SegmentProto) -> Result<Buffer> {
  unsafe {
    Ok(Buffer::from_unowned(transmute(segment.get_address()), segment.get_length() as usize, segment.get_length() as usize))
  }
}