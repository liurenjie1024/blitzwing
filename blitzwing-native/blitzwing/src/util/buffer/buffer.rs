use super::manager::{BufferDataManagerRef, BufferManager, RootManager};
use crate::{
  error::{BlitzwingError, BlitzwingErrorKind::MemoryError, Result},
  util::buffer::manager::EmptyManager,
};
use arrow::{
  buffer::Buffer as ArrowBuffer, datatypes::ArrowNativeType, memory, memory::ALIGNMENT,
  util::bit_util,
};
use std::{
  alloc::Layout,
  cmp,
  convert::TryFrom,
  default::Default,
  fmt::{Debug, Formatter},
  io::{Result as IoResult, Write},
  mem,
  ptr::null_mut,
  slice::{from_raw_parts, from_raw_parts_mut},
  sync::Arc,
};

#[derive(new, Clone, Eq, PartialEq, Debug)]
pub struct BufferSpec {
  layout: Layout,
  // hint for allocator
  resizable: bool,
}

impl Default for BufferSpec {
  fn default() -> Self {
    Self { layout: Layout::new::<()>(), resizable: true }
  }
}

impl BufferSpec {
  pub fn with_capacity(size: usize, resizable: bool) -> Self {
    Self { layout: unsafe { Layout::from_size_align_unchecked(size, ALIGNMENT) }, resizable }
  }

  pub fn layout(&self) -> &Layout {
    &self.layout
  }

  pub fn resizable(&self) -> bool {
    self.resizable
  }
}

#[derive(Clone, Debug)]
pub struct BufferData {
  ptr: *mut u8,
  capacity: usize,
  spec: BufferSpec,
}

impl Default for BufferData {
  fn default() -> Self {
    Self { ptr: null_mut(), capacity: 0, spec: BufferSpec::default() }
  }
}

impl PartialEq for BufferData {
  fn eq(&self, other: &Self) -> bool {
    (self.ptr == other.ptr) && (self.capacity == other.capacity)
  }
}

impl Eq for BufferData {}

pub struct Buffer {
  inner: BufferData,
  len: usize,
  manager: BufferDataManagerRef,
}

impl Default for Buffer {
  fn default() -> Self {
    Self { inner: BufferData::default(), len: 0, manager: Arc::new(RootManager::default()) }
  }
}

impl Debug for Buffer {
  fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
    write!(
      f,
      "Buffer {{ ptr: {:?}, len: {}, capacity: {}, data: ",
      self.inner.ptr,
      self.len(),
      self.capacity()
    )?;

    if !self.is_empty() {
      unsafe {
        f.debug_list()
          .entries(std::slice::from_raw_parts(self.inner.ptr, self.len()).iter())
          .finish()?;
      }
    }

    write!(f, " }}")
  }
}

impl BufferData {
  pub(crate) fn new(ptr: *mut u8, size: usize, spec: BufferSpec) -> Self {
    Self { ptr, capacity: size, spec }
  }

  // pub(crate) fn from_arrow_buffer_without_len(arrow_buffer: &ArrowBuffer) -> Self {
  //   Self { ptr: arrow_buffer.raw_data() as *mut u8, len: 0, capacity: arrow_buffer.capacity() }
  // }

  pub(crate) fn as_ptr(&self) -> *mut u8 {
    self.ptr
  }

  pub(crate) fn capacity(&self) -> usize {
    self.capacity
  }

  pub(crate) fn spec(&self) -> &BufferSpec {
    &self.spec
  }
}

impl Buffer {
  pub fn with_capacity(capacity: usize, resizable: bool) -> Result<Self> {
    BufferManager::default().allocate_aligned(capacity, resizable)
  }

  pub(crate) fn from_unowned(address: *mut u8, len: usize, capacity: usize) -> Result<Self> {
    let buffer_data = BufferData::new(address, capacity, BufferSpec::default());
    Ok(Self { inner: buffer_data, len, manager: Arc::new(EmptyManager {}) })
  }

  pub(super) fn new(inner: BufferData, manager: BufferDataManagerRef) -> Self {
    Self { inner, len: 0, manager }
  }

  pub(crate) fn buffer_data(&self) -> BufferData {
    self.inner.clone()
  }

  pub(crate) fn len(&self) -> usize {
    self.len
  }

  pub(crate) fn raw_data(&self) -> *mut u8 {
    self.inner.ptr
  }

  pub(crate) fn capacity(&self) -> usize {
    self.inner.capacity
  }

  pub(crate) fn is_empty(&self) -> bool {
    self.len() == 0
  }

  pub fn to_bitmap(&self) -> Result<Buffer> {
    let mut buffer = self.create_bitmap_buffer()?;
    self.fill_bitmap_buffer(&mut buffer)?;
    debug!("Raw buffer: {:?}, bitmap buffer: {:?}", &self, &buffer);
    Ok(buffer)
  }

  /// Create this method so that we can test this method in arch without simd
  pub fn to_bitmap_basic(&self) -> Result<Buffer> {
    let mut buffer = self.create_bitmap_buffer()?;
    self.fill_bitmap_buffer_basic(&mut buffer)?;
    debug!("Raw buffer: {:?}, bitmap buffer: {:?}", &self, &buffer);
    Ok(buffer)
  }

  fn create_bitmap_buffer(&self) -> Result<Buffer> {
    let new_buffer_len = bit_util::ceil(self.len(), 8);
    let new_buffer_data = self.manager.allocate_aligned(new_buffer_len, false)?;
    let mut new_buffer = Buffer::new(new_buffer_data, self.manager.clone());
    new_buffer.resize(new_buffer_len)?;
    Ok(new_buffer)
  }

  fn fill_bitmap_buffer_basic(&self, new_buffer: &mut Buffer) -> Result<()> {
    let buf = AsRef::<[u8]>::as_ref(&self);
    // clear bits
    for v in AsMut::<[u8]>::as_mut(new_buffer) {
      *v = 0;
    }

    for i in 0..self.len {
      if buf[i] > 0 {
        unsafe {
          bit_util::set_bit_raw(new_buffer.raw_data() as *mut u8, i);
        }
      }
    }

    Ok(())
  }

  fn fill_bitmap_buffer(&self, new_buffer: &mut Buffer) -> Result<()> {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
      use std::arch::x86_64::*;
      if is_x86_feature_detected!("avx2") {
        unsafe {
          let out = new_buffer.raw_data();
          let mut j = 0isize;
          for i in (0..self.len).step_by(32) {
            let start = self.raw_data().offset(i as isize) as *const __m256i;
            let input = _mm256_load_si256(start);
            let output = _mm256_movemask_epi8(input);
            *out.offset(j) = (output & 0xFF) as u8;
            *out.offset(j + 1) = ((output >> 8) & 0xFF) as u8;
            *out.offset(j + 2) = ((output >> 16) & 0xFF) as u8;
            *out.offset(j + 3) = ((output >> 24) & 0xFF) as u8;
            j = j + 4;
          }
          return Ok(());
        }
      }
    }

    self.fill_bitmap_buffer_basic(new_buffer)
  }

  pub(crate) unsafe fn to_arrow_buffer(&mut self) -> ArrowBuffer {
    let arrow_buffer = ArrowBuffer::from_unowned(self.inner.ptr, self.len(), self.capacity());
    self.inner = BufferData::default();

    arrow_buffer
  }

  /// Set the bits in the range of `[0, end)` to 0 (if `val` is false), or 1 (if `val`
  /// is true). Also extend the length of this buffer to be `end`.
  ///
  /// This is useful when one wants to clear (or set) the bits and then manipulate
  /// the buffer directly (e.g., modifying the buffer by holding a mutable reference
  /// from `data_mut()`).
  pub fn with_bitset(&mut self, end: usize, val: bool) {
    assert!(end <= self.capacity());
    let v = if val { 255 } else { 0 };
    unsafe {
      std::ptr::write_bytes(self.inner.ptr, v, end);
      self.len = end;
    }
  }

  /// Ensure that `count` bytes from `start` contain zero bits
  ///
  /// This is used to initialize the bits in a buffer, however, it has no impact on the
  /// `len` of the buffer and so can be used to initialize the memory region from
  /// `len` to `capacity`.
  pub fn set_null_bits(&mut self, start: usize, count: usize) {
    assert!(start + count <= self.capacity());
    unsafe {
      std::ptr::write_bytes(self.inner.ptr.offset(start as isize), 0, count);
    }
  }

  /// Ensures that this buffer has at least `capacity` slots in this buffer. This will
  /// also ensure the new capacity will be a multiple of 64 bytes.
  ///
  /// Returns the new capacity for this buffer.
  pub fn reserve(&mut self, capacity: usize) -> Result<usize> {
    if capacity > self.capacity() {
      let new_capacity = bit_util::round_upto_multiple_of_64(capacity);
      let new_capacity = cmp::max(new_capacity, self.capacity() * 2);
      let new_data = memory::reallocate(self.inner.ptr, self.capacity(), new_capacity);
      if !new_data.is_null() {
        self.inner.ptr = new_data;
        self.inner.capacity = new_capacity;
      } else {
        unsafe {
          return Err(MemoryError(Layout::from_size_align_unchecked(new_capacity, ALIGNMENT)))?;
        }
      }
    }
    Ok(self.capacity())
  }

  /// Resizes the buffer so that the `len` will equal to the `new_len`.
  ///
  /// If `new_len` is greater than `len`, the buffer's length is simply adjusted to be
  /// the former, optionally extending the capacity. The data between `len` and
  /// `new_len` will remain unchanged.
  ///
  /// If `new_len` is less than `len`, only length will be changed..
  pub fn resize(&mut self, new_len: usize) -> Result<()> {
    if new_len > self.len() {
      self.reserve(new_len)?;
    }
    self.len = new_len;
    Ok(())
  }
}

impl Drop for Buffer {
  fn drop(&mut self) {
    self.manager.deallocate(&self.inner).expect("Failed to deallocate buffer!");
  }
}

impl<T: ArrowNativeType + num::Num> AsRef<[T]> for Buffer {
  fn as_ref(&self) -> &[T] {
    assert_eq!(self.len() % mem::size_of::<T>(), 0);
    assert!(memory::is_ptr_aligned::<T>(self.raw_data() as *const T));
    unsafe {
      from_raw_parts(
        mem::transmute::<*const u8, *const T>(self.raw_data()),
        self.len() / mem::size_of::<T>(),
      )
    }
  }
}

impl<T: ArrowNativeType + num::Num> AsMut<[T]> for Buffer {
  fn as_mut(&mut self) -> &mut [T] {
    assert_eq!(self.len() % mem::size_of::<T>(), 0);
    assert!(memory::is_ptr_aligned::<T>(self.raw_data() as *const T));
    unsafe {
      from_raw_parts_mut(
        mem::transmute::<*mut u8, *mut T>(self.raw_data()),
        self.len() / mem::size_of::<T>(),
      )
    }
  }
}

impl<'a> TryFrom<&'a [u8]> for Buffer {
  type Error = BlitzwingError;

  fn try_from(slice: &'a [u8]) -> Result<Self> {
    // allocate aligned memory buffer
    let len = slice.len() * mem::size_of::<u8>();
    let buffer_manager = BufferManager::default();
    let mut buffer = buffer_manager.allocate_aligned(len, false)?;
    unsafe {
      memory::memcpy(buffer.inner.as_ptr(), slice.as_ptr(), len);
    }
    buffer.resize(len)?;
    Ok(buffer)
  }
}

impl PartialEq for Buffer {
  fn eq(&self, other: &Self) -> bool {
    if self.len() != other.len() {
      return false;
    }

    unsafe { memory::memcmp(self.inner.as_ptr(), other.inner.as_ptr(), self.len()) == 0 }
  }
}

impl Write for Buffer {
  fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
    self.reserve(self.len() + buf.len()).map_err(|e| e.into_std_io_error())?;

    unsafe {
      memory::memcpy(self.inner.ptr.offset(self.len() as isize), buf.as_ptr(), buf.len());
      self.len += buf.len();
      Ok(buf.len())
    }
  }

  fn flush(&mut self) -> IoResult<()> {
    Ok(())
  }
}

unsafe impl Sync for Buffer {}
unsafe impl Send for Buffer {}

#[cfg(test)]
mod tests {
  use arrow::util::bit_util;
  use std::thread;

  use super::*;
  use arrow::datatypes::ToByteSlice;
  use rand::prelude::*;
  use std::{convert::TryFrom, sync::Arc};

  #[test]
  fn test_buffer_data_equality() {
    let buf1 =
      Buffer::try_from(&[0u8, 1, 2, 3, 4] as &[u8]).expect("Failed to build buffer from array!");
    let mut buf2 =
      Buffer::try_from(&[0u8, 1u8, 2, 3, 4] as &[u8]).expect("Failed to create buffer from array!");
    assert_eq!(buf1, buf2);

    // unequal because of different elements
    buf2 =
      Buffer::try_from(&[0u8, 0, 2, 3, 4] as &[u8]).expect("Failed to create buffer from array");
    assert_ne!(buf1, buf2);

    // unequal because of different length
    buf2 = Buffer::try_from(&[0u8, 1, 2, 3] as &[u8]).expect("Failed to create buffer from array!");
    assert_ne!(buf1, buf2);
  }

  #[test]
  fn test_from_vec() {
    let buf =
      Buffer::try_from(&[0u8, 1, 2, 3, 4] as &[u8]).expect("Failed to create buffer from array");
    assert_eq!(5, buf.len());
    assert!(!buf.raw_data().is_null());
    assert_eq!([0, 1, 2, 3, 4], <Buffer as AsRef<[u8]>>::as_ref(&buf));
  }

  #[test]
  fn test_with_bitset() {
    let mut buf = Buffer::with_capacity(64, false).expect("Failed to create buffer");
    buf.with_bitset(64, false);
    assert_eq!(0, bit_util::count_set_bits(buf.as_ref()));

    let mut buf = Buffer::with_capacity(64, false).expect("Failed to create buffer");
    buf.with_bitset(64, true);
    assert_eq!(512, bit_util::count_set_bits(buf.as_ref()));
  }

  // #[test]
  // fn test_bitwise_and() {
  //     let buf1 = Buffer::try_from(&[0b01101010_u8] as &[u8]).expect("Failed to build buf1");
  //     let buf2 = Buffer::try_from(&[0b01001110_u8] as &[u8]).expect("Failed to build buf2");

  //     let result_buf = Buffer::try_from(&[0b01001010_u8] as &[u8]).expect("Failed to build result buf");
  //     assert_eq!(result_buf, (&buf1 & &buf2).unwrap());
  // }

  // #[test]
  // fn test_bitwise_or() {
  //     let buf1 = Buffer::from([0b01101010]);
  //     let buf2 = Buffer::from([0b01001110]);
  //     assert_eq!(Buffer::from([0b01101110]), (&buf1 | &buf2).unwrap());
  // }

  // #[test]
  // fn test_bitwise_not() {
  //     let buf = Buffer::from([0b01101010]);
  //     assert_eq!(Buffer::from([0b10010101]), !&buf);
  // }

  // #[test]
  // #[should_panic(expected = "Buffers must be the same size to apply Bitwise OR.")]
  // fn test_buffer_bitand_different_sizes() {
  //     let buf1 = Buffer::from([1_u8, 1_u8]);
  //     let buf2 = Buffer::from([0b01001110]);
  //     let _buf3 = (&buf1 | &buf2).unwrap();
  // }

  #[test]
  fn test_with_capacity() {
    let buf = Buffer::with_capacity(63, false).unwrap();
    assert_eq!(64, buf.capacity());
    assert_eq!(0, buf.len());
    assert!(buf.is_empty());
  }

  // #[test]
  // fn test_mutable_write() {
  //     let mut buf = MutableBuffer::new(100);
  //     buf.write("hello".as_bytes()).expect("Ok");
  //     assert_eq!(5, buf.len());
  //     assert_eq!("hello".as_bytes(), buf.data());

  //     buf.write(" world".as_bytes()).expect("Ok");
  //     assert_eq!(11, buf.len());
  //     assert_eq!("hello world".as_bytes(), buf.data());

  //     buf.clear();
  //     assert_eq!(0, buf.len());
  //     buf.write("hello arrow".as_bytes()).expect("Ok");
  //     assert_eq!(11, buf.len());
  //     assert_eq!("hello arrow".as_bytes(), buf.data());
  // }

  // #[test]
  // #[should_panic(expected = "Buffer not big enough")]
  // fn test_mutable_write_overflow() {
  //     let mut buf = MutableBuffer::new(1);
  //     assert_eq!(64, buf.capacity());
  //     for _ in 0..10 {
  //         buf.write(&[0, 0, 0, 0, 0, 0, 0, 0]).unwrap();
  //     }
  // }

  #[test]
  fn test_reserve() {
    let mut buf = Buffer::with_capacity(1, true).unwrap();
    assert_eq!(64, buf.capacity());

    // Reserving a smaller capacity should have no effect.
    let mut new_cap = buf.reserve(10).expect("reserve should be OK");
    assert_eq!(64, new_cap);
    assert_eq!(64, buf.capacity());

    new_cap = buf.reserve(100).expect("reserve should be OK");
    assert_eq!(128, new_cap);
    assert_eq!(128, buf.capacity());
  }

  #[test]
  fn test_mutable_resize() {
    let mut buf = Buffer::with_capacity(1, true).unwrap();
    assert_eq!(64, buf.capacity());
    assert_eq!(0, buf.len());

    buf.resize(20).expect("resize should be OK");
    assert_eq!(64, buf.capacity());
    assert_eq!(20, buf.len());

    buf.resize(10).expect("resize should be OK");
    assert_eq!(64, buf.capacity());
    assert_eq!(10, buf.len());

    buf.resize(100).expect("resize should be OK");
    assert_eq!(128, buf.capacity());
    assert_eq!(100, buf.len());

    buf.resize(30).expect("resize should be OK");
    assert_eq!(128, buf.capacity());
    assert_eq!(30, buf.len());

    buf.resize(0).expect("resize should be OK");
    assert_eq!(128, buf.capacity());
    assert_eq!(0, buf.len());
  }

  // #[test]
  // fn test_equal() -> Result<()> {
  //     let mut buf = MutableBuffer::new(1);
  //     let mut buf2 = MutableBuffer::new(1);

  //     buf.write(&[0xaa])?;
  //     buf2.write(&[0xaa, 0xbb])?;
  //     assert!(buf != buf2);

  //     buf.write(&[0xbb])?;
  //     assert_eq!(buf, buf2);

  //     buf2.reserve(65)?;
  //     assert!(buf != buf2);

  //     Ok(())
  // }

  #[test]
  fn test_access_concurrently() {
    let buffer = Arc::new(Buffer::try_from(vec![1u8, 2, 3, 4, 5].as_slice()).unwrap());
    let buffer2 = buffer.clone();
    assert_eq!(&[1u8, 2, 3, 4, 5] as &[u8], <Buffer as AsRef<[u8]>>::as_ref(buffer.as_ref()));

    let buffer_copy = thread::spawn(move || {
      // access buffer in another thread.
      buffer.clone()
    })
    .join();

    assert!(buffer_copy.is_ok());
    assert_eq!(buffer2, buffer_copy.ok().unwrap());
  }

  macro_rules! check_as_typed_data {
    ($input: expr, $native_t: ty) => {{
      let buffer = Buffer::try_from($input.to_byte_slice()).unwrap();
      let slice: &[$native_t] = buffer.as_ref();
      assert_eq!($input, slice);
    }};
  }

  #[test]
  fn test_as_typed_data() {
    check_as_typed_data!(&[1i8, 3i8, 6i8], i8);
    check_as_typed_data!(&[1u8, 3u8, 6u8], u8);
    check_as_typed_data!(&[1i16, 3i16, 6i16], i16);
    check_as_typed_data!(&[1i32, 3i32, 6i32], i32);
    check_as_typed_data!(&[1i64, 3i64, 6i64], i64);
    check_as_typed_data!(&[1u16, 3u16, 6u16], u16);
    check_as_typed_data!(&[1u32, 3u32, 6u32], u32);
    check_as_typed_data!(&[1u64, 3u64, 6u64], u64);
    check_as_typed_data!(&[1f32, 3f32, 6f32], f32);
    check_as_typed_data!(&[1f64, 3f64, 6f64], f64);
  }

  #[test]
  fn test_to_bitmap() {
    let bits: Vec<bool> = (0..1000).map(|_i| random()).collect();
    let bytes: Vec<u8> = bits.iter().map(|v| if *v { 128u8 } else { 0u8 }).collect();

    let buffer = Buffer::try_from(bytes.as_slice()).expect("Failed to build buffer");

    let bitmap = buffer.to_bitmap().expect("Failed to create bitmap");
    check_bitmap_with_bools(&bitmap, bits.as_slice());

    let bitmap = buffer.to_bitmap_basic().expect("Failed to create bitmap basic");
    check_bitmap_with_bools(&bitmap, bits.as_slice());
  }

  fn check_bitmap_with_bools(bitmap: &Buffer, bits: &[bool]) {
    assert_eq!(bit_util::ceil(bits.len(), 8), bitmap.len());

    let data: &[u8] = bitmap.as_ref();

    for i in 0..bits.len() {
      assert_eq!(bits[i], bit_util::get_bit(data, i));
    }
  }
}
