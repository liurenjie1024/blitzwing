use crate::{
  error::{BlitzwingError, BlitzwingErrorKind::ParquetError, Result},
  proto::parquet::{ColumnChunkProto, ColumnDescProto, SegmentProto},
  util::TryIterator,
};
// use arrow::buffer::Buffer;
use crate::util::buffer::Buffer;
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
  debug!("Column desc: {:?}, column chunk: {:?}", column_desc, column_chunk);
  let mut reader = column_chunk_to_read(column_chunk)?;

  // {
  //   let mut content = Vec::new();
  //   reader.read_to_end(&mut content).unwrap();
  //   debug!("Content is: {:?}", &content);
  // }

  let num_values = column_chunk.get_num_values();
  let compression = column_chunk.get_compression().try_into()?;
  let physical_type = column_desc.get_physical_type().try_into()?;

  Ok(
    SerializedPageReader::new(reader, num_values, compression, physical_type)
      .context(ParquetError)?,
  )
}

pub(crate) fn column_chunk_to_read(column_chunk: &ColumnChunkProto) -> Result<impl Read> {
  let empty_reader: Box<dyn Read> = Box::new(Cursor::new(Vec::<u8>::new()));
  column_chunk
    .get_segments()
    .iter()
    .map(segment_to_read)
    .try_fold(empty_reader, |b, r| r.map(|rr| Box::new(b.chain(rr)) as Box<dyn Read>))
}

pub(crate) fn segment_to_read(segment: &SegmentProto) -> Result<impl Read> {
  segment_to_buffer(segment).map(|b| Cursor::new(b))
}

pub(crate) fn segment_to_buffer(segment: &SegmentProto) -> Result<Buffer> {
  unsafe {
    Buffer::from_unowned(
      transmute(segment.get_address()),
      segment.get_length() as usize,
      segment.get_length() as usize,
    )
  }
}

#[cfg(test)]
mod tests {
  use crate::proto::parquet::{ColumnChunkProto, ParquetProto_Compression, SegmentProto};
  use std::mem::size_of;
  use super::column_chunk_to_read;
  use std::io::Read;

  fn vec_to_seg<T>(v: &mut Vec<T>) -> SegmentProto {
      let mut seg = SegmentProto::new();
      seg.set_address(v.as_mut_ptr() as i64);
      seg.set_length((v.len() * size_of::<T>()) as i32);
      seg
  }

  #[test]
  fn test_column_chunk_to_read() {
    let mut v1 = vec![1u8, 2u8, 3u8];
    let mut v2 = vec![3u8, 2u8, 1u8];

    let column_chunk_proto = {
      let mut column_chunk_proto = ColumnChunkProto::new();
      column_chunk_proto.set_column_name("a".to_string());
      column_chunk_proto.set_compression(ParquetProto_Compression::UNCOMPRESSED);

      column_chunk_proto.mut_segments().push(vec_to_seg(&mut v1));
      column_chunk_proto.mut_segments().push(vec_to_seg(&mut v2));
      column_chunk_proto
    };

    let mut reader = column_chunk_to_read(&column_chunk_proto).unwrap();
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).unwrap();
    
    let mut raw_data = Vec::new();
    raw_data.append(&mut v1.clone());
    raw_data.append(&mut v2.clone());

    assert_eq!(raw_data, buffer);
  }
}
