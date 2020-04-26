use crate::{
  error::{
    BlitzwingErrorKind::{FatalError, InvalidArgumentError, ParquetError},
    Result,
  },
  types::ColumnDescProtoPtr,
  util::{
    buffer::BooleanBufferBuilder,
    reader::{PageReaderIteratorRef, PageReaderRef},
  },
};
use failure::ResultExt;
use parquet::{
  basic::Encoding,
  column::page::Page,
  data_type::DataType as ParquetType,
  decoding::{get_decoder_simple, Decoder, DictDecoder, PlainDecoder},
  encodings::levels::LevelDecoder,
  memory::ByteBufferPtr,
};
use std::{cmp::min, collections::HashMap, marker::PhantomData, mem::replace};
use crate::util::buffer::BufferBuilderTrait;

pub(crate) struct RecordReaderBuffers<P, D> {
  pub(super) parquet_data_buffer: P,
  pub(super) def_levels: D,
  pub(super) null_bitmap: BooleanBufferBuilder,
}

pub struct RecordReader<T, B, D> {
  batch_size: usize,
  column_desc: ColumnDescProtoPtr,

  num_values: usize,
  num_nulls: usize,
  buffers: RecordReaderBuffers<B, D>,

  page_readers: PageReaderIteratorRef,
  cur_page_reader: Option<PageReaderRef>,

  def_level_decoder: Option<LevelDecoder>,
  current_encoding: Option<Encoding>,

  // The total number of values stored in the data page.
  num_buffered_values: usize,

  // The number of values from the current data page that has been decoded into memory
  // so far.
  num_decoded_values: usize,

  // Cache of decoders for existing encodings
  decoders: HashMap<Encoding, Box<dyn Decoder<T>>>,
  phantom_data: PhantomData<T>,
}

impl<T, B, D> RecordReader<T, B, D>
where
  T: ParquetType,
  B: AsMut<[T::T]>,
  D: AsMut<[i16]>,
{
  pub(crate) fn new(
    batch_size: usize,
    column_desc: ColumnDescProtoPtr,
    mut buffers: RecordReaderBuffers<B, D>,
    page_readers: PageReaderIteratorRef,
  ) -> Result<Self> {
    if column_desc.get_max_def_level() > 0 && buffers.def_levels.as_mut().len() != batch_size {
      return Err(InvalidArgumentError(format!(
        "Def level buffer size({}) not match batch size({})",
        buffers.def_levels.as_mut().len(),
        batch_size
      )))?;
    }

    Ok(Self {
      batch_size,
      column_desc,
      num_values: 0,
      num_nulls: 0,
      buffers,
      page_readers,
      cur_page_reader: None,
      def_level_decoder: None,
      current_encoding: None,
      num_buffered_values: 0,
      num_decoded_values: 0,
      decoders: HashMap::new(),
      phantom_data: PhantomData,
    })
  }

  pub(crate) fn next_batch(&mut self) -> Result<()> {
    while self.num_values < self.batch_size {
      if !self.has_next()? {
        break;
      }

      // Batch size for the current iteration
      let iter_batch_size =
        min(self.batch_size - self.num_values, self.num_buffered_values - self.num_decoded_values);

      let mut iter_num_nulls = 0;
      // If the field is required and non-repeated, there are no definition levels
      if self.column_desc.get_max_def_level() > 0 {
        self.read_def_levels(iter_batch_size)?;

        let mut cur_idx = self.num_values;
        let end_idx = self.num_values + iter_batch_size;

        loop {
          if cur_idx >= end_idx {
            break;
          }

          let is_empty = self.buffers.def_levels.as_mut()[cur_idx] < self.max_def_level();

          let mut end_idx_tmp = cur_idx + 1;
          while end_idx_tmp < end_idx
            && ((self.buffers.def_levels.as_mut()[end_idx_tmp] < self.max_def_level()) == is_empty)
          {
            end_idx_tmp += 1;
          }

          if !is_empty {
            self.read_values(cur_idx, end_idx_tmp)?;
          } else {
            iter_num_nulls += end_idx_tmp - cur_idx;
          }

          for _ in cur_idx..end_idx_tmp {
            self.buffers.null_bitmap.append(!is_empty)?;
          }

          cur_idx = end_idx_tmp;
        }
      } else {
        let cur_idx = self.num_values;
        let end_idx = self.num_values + iter_batch_size;
        self.read_values(cur_idx, end_idx)?;
      }

      self.num_decoded_values += iter_batch_size;
      self.num_nulls += iter_num_nulls;
      self.num_values += iter_batch_size;
    }

    Ok(())
  }

  pub(crate) fn get_null_count(&self) -> usize {
    self.num_nulls
  }

  pub(crate) fn get_num_values(&self) -> usize {
    self.num_values
  }

  pub(crate) fn collect_buffers(
    &mut self,
    new_buffer: RecordReaderBuffers<B, D>,
  ) -> RecordReaderBuffers<B, D> {
    self.num_values = 0;
    self.num_nulls = 0;

    replace(&mut self.buffers, new_buffer)
  }

  fn max_def_level(&self) -> i16 {
    self.column_desc.get_max_def_level() as i16
  }

  fn type_length(&self) -> i32 {
    self.column_desc.get_type_length() as i32
  }

  /// Reads a new page and set up the decoders for levels, values or dictionary.
  /// Returns false if there's no page left.
  fn read_new_page(&mut self) -> Result<bool> {
    loop {
      if self.cur_page_reader.is_none() {
        self.cur_page_reader = self.page_readers.try_next()?;
        if self.cur_page_reader.is_none() {
          return Ok(false);
        }
      }

      let cur_page_reader =
        self.cur_page_reader.as_mut().expect("Current page reader should have been initialized!");

      match cur_page_reader.get_next_page().context(ParquetError)? {
        // No more page to read
        None => self.cur_page_reader = None,
        Some(current_page) => {
          match current_page {
            // 1. Dictionary page: configure dictionary for this page.
            p @ Page::DictionaryPage { .. } => {
              self.configure_dictionary(p)?;
              continue;
            }
            // 2. Data page v1
            Page::DataPage {
              buf,
              num_values,
              encoding,
              def_level_encoding,
              rep_level_encoding: _,
              statistics: _,
            } => {
              self.num_buffered_values = num_values as usize;
              self.num_decoded_values = 0;

              let mut buffer_ptr = buf;

              if self.max_def_level() > 0 {
                let mut def_decoder = LevelDecoder::v1(def_level_encoding, self.max_def_level());
                let total_bytes =
                  def_decoder.set_data(self.num_buffered_values as usize, buffer_ptr.all());
                buffer_ptr = buffer_ptr.start_from(total_bytes);
                self.def_level_decoder = Some(def_decoder);
              }

              // Data page v1 does not have offset, all content of buffer
              // should be passed
              self.set_current_page_encoding(encoding, &buffer_ptr, 0, num_values as usize)?;
              return Ok(true);
            }
            // 3. Data page v2
            Page::DataPageV2 {
              buf,
              num_values,
              encoding,
              num_nulls: _,
              num_rows: _,
              def_levels_byte_len,
              rep_levels_byte_len: _,
              is_compressed: _,
              statistics: _,
            } => {
              self.num_buffered_values = num_values as usize;
              self.num_decoded_values = 0;

              let mut offset = 0;

              // DataPage v2 only supports RLE encoding for definition
              // levels
              if self.max_def_level() > 0 {
                let mut def_decoder = LevelDecoder::v2(self.max_def_level());
                let bytes_read = def_decoder.set_data_range(
                  self.num_buffered_values as usize,
                  &buf,
                  offset,
                  def_levels_byte_len as usize,
                );
                offset += bytes_read;
                self.def_level_decoder = Some(def_decoder);
              }

              self.set_current_page_encoding(encoding, &buf, offset, num_values as usize)?;
              return Ok(true);
            }
          };
        }
      }
    }
  }

  /// Resolves and updates encoding and set decoder for the current page
  fn set_current_page_encoding(
    &mut self,
    mut encoding: Encoding,
    buffer_ptr: &ByteBufferPtr,
    offset: usize,
    len: usize,
  ) -> Result<()> {
    if encoding == Encoding::PLAIN_DICTIONARY {
      encoding = Encoding::RLE_DICTIONARY;
    }

    let decoder = if encoding == Encoding::RLE_DICTIONARY {
      self.decoders.get_mut(&encoding).expect("Decoder for dict should have been set")
    } else {
      // Search cache for data page decoder
      if !self.decoders.contains_key(&encoding) {
        // Initialize decoder for this page
        let data_decoder =
          get_decoder_simple::<T>(self.type_length(), encoding).context(ParquetError)?;
        self.decoders.insert(encoding, data_decoder);
      }
      self.decoders.get_mut(&encoding).unwrap()
    };

    decoder.set_data(buffer_ptr.start_from(offset), len as usize).context(ParquetError)?;
    self.current_encoding = Some(encoding);
    Ok(())
  }

  #[inline]
  fn has_next(&mut self) -> Result<bool> {
    if self.num_buffered_values == 0 || self.num_buffered_values == self.num_decoded_values {
      // TODO: should we return false if read_new_page() = true and
      // num_buffered_values = 0?
      if !self.read_new_page()? {
        Ok(false)
      } else {
        Ok(self.num_buffered_values != 0)
      }
    } else {
      Ok(true)
    }
  }

  #[inline]
  fn read_def_levels(&mut self, num: usize) -> Result<usize> {
    let level_decoder = self.def_level_decoder.as_mut().expect("def_level_decoder be set");

    Ok(
      level_decoder
        .get(&mut self.buffers.def_levels.as_mut()[self.num_values..(self.num_values + num)])
        .context(ParquetError)?,
    )
  }

  #[inline]
  fn read_values(&mut self, start: usize, end: usize) -> Result<usize> {
    let encoding = self.current_encoding.expect("current_encoding should be set");
    let current_decoder = self
      .decoders
      .get_mut(&encoding)
      .expect(format!("decoder for encoding {} should be set", encoding).as_str());

    let buffer = &mut self.buffers.parquet_data_buffer.as_mut()[start..end];

    Ok(current_decoder.get(buffer).context(ParquetError)?)
  }

  #[inline]
  fn configure_dictionary(&mut self, page: Page) -> Result<bool> {
    let mut encoding = page.encoding();
    if encoding == Encoding::PLAIN || encoding == Encoding::PLAIN_DICTIONARY {
      encoding = Encoding::RLE_DICTIONARY
    }

    if self.decoders.contains_key(&encoding) {
      return Err(FatalError("Column cannot have more than one dictionary".to_string()))?;
    }

    if encoding == Encoding::RLE_DICTIONARY {
      let mut dictionary = PlainDecoder::<T>::new(self.type_length());
      let num_values = page.num_values();
      dictionary.set_data(page.buffer().clone(), num_values as usize).context(ParquetError)?;

      let mut decoder = DictDecoder::new();
      decoder.set_dict(Box::new(dictionary)).context(ParquetError)?;
      self.decoders.insert(encoding, Box::new(decoder));
      Ok(true)
    } else {
      Err(nyi!("Invalid/Unsupported encoding type for dictionary: {}", encoding))
    }
  }
}
