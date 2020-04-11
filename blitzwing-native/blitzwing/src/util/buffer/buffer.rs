use super::manager::BufferDataManagerRef;
use arrow::datatypes::ArrowNativeType;
use arrow::{memory, memory::ALIGNMENT};
use arrow::util::bit_util;
use std::cmp;
use std::{mem, ptr::null_mut, slice::{from_raw_parts, from_raw_parts_mut}};
use crate::error::{BlitzwingErrorKind::MemoryError, BlitzwingError};
use std::alloc::Layout;
use crate::error::Result;
use std::sync::Arc;
use super::manager::{RootManager, BufferManager};
use std::default::Default;
use std::convert::TryFrom;
use std::fmt::{Debug, Formatter};
use std::io::{Write, Result as IoResult, Error as IoError, ErrorKind};
use arrow::buffer::{Buffer as ArrowBuffer};

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct BufferData {
  ptr: *mut u8,
  len: usize,
  capacity: usize
}

impl Default for BufferData {
  fn default() -> Self {
    Self {
      ptr: null_mut(),
      len: 0,
      capacity: 0
    }
  }
}

pub struct Buffer {
  inner: BufferData,
  manager: BufferDataManagerRef,
}

impl Default for Buffer {
  fn default() -> Self {
    Self {
      inner: BufferData::default(),
      manager: Arc::new(RootManager::default())
    }
  }
}

impl Debug for Buffer {
  fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
      write!(
          f,
          "Buffer {{ ptr: {:?}, len: {}, capacity: {}, data: ",
          self.inner.ptr, self.len(), self.capacity()
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
  pub(crate) fn new(ptr: *mut u8, size: usize) -> Self {
    Self {
      ptr,
      len: 0,
      capacity: size
    }
  }

  pub(crate) fn from_arrow_buffer_without_len(arrow_buffer: &ArrowBuffer) -> Self {
    Self {
      ptr: arrow_buffer.raw_data() as *mut u8,
      len: 0,
      capacity: arrow_buffer.capacity()
    }
  }

  pub(in super) fn as_ptr(&self) -> *mut u8 {
    self.ptr
  }

  pub(in super) fn capacity(&self) -> usize {
    self.capacity
  }
}

impl Buffer {
  pub(in super) fn with_capacity(capacity: usize) -> Result<Self> {
    BufferManager::default().allocate_aligned(capacity)
  }

  pub(in super) fn new(inner: BufferData, manager: BufferDataManagerRef) -> Self {
    Self {
      inner,
      manager,
    }
  }

  pub(crate) fn len(&self) -> usize {
    self.inner.len
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

  pub(crate) fn to_arrow_buffer(self) -> ArrowBuffer {
    ArrowBuffer::from_unowned(self.inner.ptr, self.len(), self.capacity())
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
          self.inner.len = end;
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
      self.inner.len = new_len;
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
    let buffer = buffer_manager.allocate_aligned(len)?;
    unsafe {
        memory::memcpy(buffer.inner.as_ptr(), slice.as_ptr(), len);
    }
    Ok(buffer)
  }
}

impl PartialEq for Buffer {
  fn eq(&self, other: &Self) -> bool {
    if self.len() != other.len() {
      return false;
    }

    unsafe {
      memory::memcmp(self.inner.as_ptr(), other.inner.as_ptr(), self.len()) == 0
    }
  }
}

impl Write for Buffer {
  fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
      let remaining_capacity = self.capacity() - self.len();
      if buf.len() > remaining_capacity {
          return Err(IoError::new(ErrorKind::Other, "Buffer not big enough"));
      }
      unsafe {
          memory::memcpy(self.inner.ptr.offset(self.len() as isize), buf.as_ptr(), buf.len());
          self.inner.len += buf.len();
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
    use std::convert::TryFrom;
    use std::sync::Arc;

    #[test]
    fn test_buffer_data_equality() {
        let buf1 = Buffer::try_from(&[0u8, 1, 2, 3, 4] as &[u8]).expect("Failed to build buffer from array!");
        let mut buf2 = Buffer::try_from(&[0u8, 1u8, 2, 3, 4] as &[u8]).expect("Failed to create buffer from array!");
        assert_eq!(buf1, buf2);

        // unequal because of different elements
        buf2 = Buffer::try_from(&[0u8, 0, 2, 3, 4] as &[u8]).expect("Failed to create buffer from array");
        assert_ne!(buf1, buf2);

        // unequal because of different length
        buf2 = Buffer::try_from(&[0u8, 1, 2, 3] as &[u8]).expect("Failed to create buffer from array!");
        assert_ne!(buf1, buf2);
    }

    #[test]
    fn test_from_vec() {
        let buf = Buffer::try_from(&[0u8, 1, 2, 3, 4] as &[u8]).expect("Failed to create buffer from array") ;
        assert_eq!(5, buf.len());
        assert!(!buf.raw_data().is_null());
        assert_eq!([0, 1, 2, 3, 4], <Buffer as AsRef<[u8]>>::as_ref(&buf));
    }

    #[test]
    fn test_with_bitset() {
        let mut buf = Buffer::with_capacity(64).expect("Failed to create buffer");
        buf.with_bitset(64, false);
        assert_eq!(0, bit_util::count_set_bits(buf.as_ref()));

        let mut buf = Buffer::with_capacity(64).expect("Failed to create buffer");
        buf.with_bitset(64, true);
        assert_eq!(512, bit_util::count_set_bits(buf.as_ref()));

        let mut buf = Buffer::with_capacity(64).expect("Failed to create buffer");
        buf.set_null_bits(32, 32);
        assert_eq!(256, bit_util::count_set_bits(buf.as_ref()));
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
        let buf = Buffer::with_capacity(63).unwrap();
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
        let mut buf = Buffer::with_capacity(1).unwrap();
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
        let mut buf = Buffer::with_capacity(1).unwrap();
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
        assert_eq!(64, buf.capacity());
        assert_eq!(30, buf.len());

        buf.resize(0).expect("resize should be OK");
        assert_eq!(0, buf.capacity());
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
}

