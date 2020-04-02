use std::ptr::NonNull;
use super::manage::BufferManagerRef;
use arrow::datatypes::ArrowNativeType;
use arrow::memory;
use std::{mem, slice::{from_raw_parts, from_raw_parts_mut}};

#[derive(Clone)]
pub(super) struct BufferData {
  ptr: NonNull<u8>,
  len: usize,
  capacity: usize
}
pub(crate) struct Buffer {
  inner: BufferData,
  manager: BufferManagerRef,
}

impl Buffer {
  fn len(&self) -> usize {
    self.inner.len
  }

  fn raw_data(&self) -> *mut u8 {
    self.inner.ptr.as_ptr()
  }

  fn capacity(&self) -> usize {
    self.inner.capacity
  }
}

impl Drop for Buffer {
  fn drop(&mut self) {
    self.manager.deallocate(self.inner.clone()).expect("Failed to deallocate buffer!");
  }
}

impl<T: ArrowNativeType + num::Num> AsRef<[T]> for Buffer {
  fn as_ref(&self) -> &[T] {
    assert_eq!(self.len() % mem::size_of::<T>(), 0);
    assert!(memory::is_ptr_aligned::<T>(self.raw_data() as *const T));
        from_raw_parts(
            mem::transmute::<*const u8, *const T>(self.raw_data()),
            self.len() / mem::size_of::<T>(),
        )
  }
}

impl<T: ArrowNativeType + num::Num> AsMut<[T]> for Buffer {
  fn as_mut(&mut self) -> &mut [T] {
    assert_eq!(self.len() % mem::size_of::<T>(), 0);
    assert!(memory::is_ptr_aligned::<T>(self.raw_data() as *const T));
        from_raw_parts_mut(
            mem::transmute::<*mut u8, *mut T>(self.raw_data()),
            self.len() / mem::size_of::<T>(),
        )
  }
}