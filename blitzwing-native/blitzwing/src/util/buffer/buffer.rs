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

#[derive(Clone)]
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
  pub(in super) fn new(ptr: *mut u8, size: usize) -> Self {
    Self {
      ptr,
      len: 0,
      capacity: size
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

