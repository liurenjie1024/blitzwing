use super::buffer::{Buffer, BufferData};
use crate::error::{
  BlitzwingErrorKind::{InvalidArgumentError, LayoutError, MemoryError},
  Result,
};
use arrow::{memory, memory::ALIGNMENT, util::bit_util};
use failure::ResultExt;
use std::{alloc::Layout, sync::Arc};

pub type BufferDataManagerRef = Arc<dyn Manager>;
#[derive(Clone)]
pub struct BufferManager {
  inner: BufferDataManagerRef,
}

impl BufferManager {
  pub fn allocate(&self, layout: Layout) -> Result<Buffer> {
    let buffer_data = self.inner.allocate(layout)?;
    Ok(Buffer::new(buffer_data, self.inner.clone()))
  }

  pub fn allocate_aligned(&self, capacity: usize) -> Result<Buffer> {
    unsafe { self.allocate(Layout::from_size_align_unchecked(capacity, ALIGNMENT)) }
  }
}

impl Default for BufferManager {
  fn default() -> Self {
    Self { inner: Arc::new(RootManager::default()) }
  }
}

impl BufferManager {
  pub(crate) fn new(inner: BufferDataManagerRef) -> Self {
    Self { inner }
  }
}

pub trait Manager {
  fn allocate(&self, layout: Layout) -> Result<BufferData> {
    if layout.align() != ALIGNMENT {
      return Err(InvalidArgumentError(format!("Buffer alignment must be {}", ALIGNMENT)))?;
    }

    let new_capacity = bit_util::round_upto_multiple_of_64(layout.size());
    let ptr = memory::allocate_aligned(new_capacity);

    if ptr.is_null() {
      return Err(MemoryError(
        Layout::from_size_align(new_capacity, ALIGNMENT).context(LayoutError)?,
      ))?;
    }

    Ok(BufferData::new(ptr, new_capacity))
  }

  fn allocate_aligned(&self, capacity: usize) -> Result<BufferData> {
    unsafe { self.allocate(Layout::from_size_align_unchecked(capacity, ALIGNMENT)) }
  }

  fn deallocate(&self, buffer: &BufferData) -> Result<()> {
    let ptr = buffer.as_ptr();
    if !ptr.is_null() {
      memory::free_aligned(ptr, buffer.capacity());
    }

    Ok(())
  }
}

#[derive(Default)]
pub(crate) struct RootManager {}
impl Manager for RootManager {}

pub(crate) struct CachedManager {
  root: BufferDataManagerRef,
}

impl CachedManager {
  pub(crate) fn new(root: BufferDataManagerRef) -> Self {
    Self { root }
  }
}
impl Manager for CachedManager {}
