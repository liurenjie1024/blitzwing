use super::buffer::{Buffer, BufferData, BufferSpec};
use crate::error::{
  BlitzwingErrorKind::{FatalError, InvalidArgumentError, LayoutError, MemoryError},
  Result,
};
use arrow::{memory, memory::ALIGNMENT, util::bit_util};
use failure::ResultExt;
use std::{
  alloc::Layout,
  cmp,
  sync::{Arc, Mutex},
};

pub type BufferDataManagerRef = Arc<dyn Manager>;
#[derive(Clone)]
pub struct BufferManager {
  inner: BufferDataManagerRef,
}

impl BufferManager {
  pub(crate) fn allocate(&self, spec: BufferSpec) -> Result<Buffer> {
    let buffer_data = self.inner.allocate(spec)?;
    Ok(Buffer::new(buffer_data, self.inner.clone()))
  }

  pub fn allocate_aligned(&self, capacity: usize, resizable: bool) -> Result<Buffer> {
    self.allocate(BufferSpec::with_capacity(capacity, resizable))
  }

  pub(crate) fn deallocate(&self, buffer_data: BufferData) -> Result<()> {
    self.inner.deallocate(&buffer_data)
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
  fn allocate(&self, spec: BufferSpec) -> Result<BufferData> {
    if spec.layout().align() != ALIGNMENT {
      return Err(InvalidArgumentError(format!("Buffer alignment must be {}", ALIGNMENT)))?;
    }

    let new_capacity = bit_util::round_upto_multiple_of_64(spec.layout().size());
    let ptr = memory::allocate_aligned(new_capacity);

    if ptr.is_null() {
      return Err(MemoryError(
        Layout::from_size_align(new_capacity, ALIGNMENT).context(LayoutError)?,
      ))?;
    }

    Ok(BufferData::new(ptr, new_capacity, spec))
  }

  fn allocate_aligned(&self, capacity: usize, resizable: bool) -> Result<BufferData> {
    let spec =
      unsafe { BufferSpec::new(Layout::from_size_align_unchecked(capacity, ALIGNMENT), resizable) };
    self.allocate(spec)
  }

  fn reallocate(&self, buffer: &BufferData, capacity: usize) -> Result<BufferData> {
    let new_capacity = bit_util::round_upto_multiple_of_64(capacity);
    let new_capacity = cmp::max(new_capacity, buffer.capacity() * 2);
    let new_data = memory::reallocate(buffer.as_ptr(), buffer.capacity(), new_capacity);
    if !new_data.is_null() {
      let new_spec = unsafe {
        BufferSpec::new(
          Layout::from_size_align_unchecked(new_capacity, ALIGNMENT),
          buffer.spec().resizable(),
        )
      };
      Ok(BufferData::new(new_data, new_capacity, new_spec))
    } else {
      unsafe {
        return Err(MemoryError(Layout::from_size_align_unchecked(new_capacity, ALIGNMENT)))?;
      }
    }
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
  max_cached_num: usize,
  cached_fix_buffers: Mutex<Vec<BufferData>>,
  cached_var_len_buffers: Mutex<Vec<BufferData>>,
}

impl CachedManager {
  pub(crate) fn new(root: BufferDataManagerRef) -> Self {
    let max_cached_num = 4;
    Self {
      root,
      max_cached_num,
      cached_fix_buffers: Mutex::new(Vec::with_capacity(max_cached_num)),
      cached_var_len_buffers: Mutex::new(Vec::with_capacity(max_cached_num)),
    }
  }
}

impl Manager for CachedManager {
  fn allocate(&self, spec: BufferSpec) -> Result<BufferData> {
    if spec.layout().align() != ALIGNMENT {
      return Err(InvalidArgumentError(format!("Buffer alignment must be {}", ALIGNMENT)))?;
    }

    if spec.resizable() {
      match self.cached_fix_buffers.lock() {
        Ok(mut buffers) => {
          if let Some(idx) = buffers.iter().position(|b| b.capacity() == spec.layout().size()) {
            Ok(buffers.remove(idx))
          } else {
            Manager::allocate(self, spec.clone())
          }
        }
        Err(_) => Err(FatalError("Mutex of fixed size buffer poisoned".to_string()))?,
      }
    } else {
      match self.cached_var_len_buffers.lock() {
        Ok(mut buffers) => {
          if let Some(idx) = buffers.iter().position(|b| b.capacity() >= spec.layout().size()) {
            Ok(buffers.remove(idx))
          } else {
            self.root.allocate(spec)
          }
        }
        Err(_) => Err(FatalError("Mutex of var size buffer poisoned".to_string()))?,
      }
    }
  }

  fn deallocate(&self, buffer: &BufferData) -> Result<()> {
    let ptr = buffer.as_ptr();
    if !ptr.is_null() {
      if buffer.spec().resizable() {
        match self.cached_var_len_buffers.lock() {
          Ok(mut buffers) => {
            if buffers.len() < self.max_cached_num {
              buffers.push(buffer.clone());
            } else {
              memory::free_aligned(ptr, buffer.capacity());
            }
          }
          Err(_) => Err(FatalError("Mutex of var size buffer poisoned".to_string()))?,
        }
      } else {
        match self.cached_fix_buffers.lock() {
          Ok(mut buffers) => {
            if buffers.len() < self.max_cached_num {
              buffers.push(buffer.clone());
            } else {
              self.root.deallocate(buffer)?;
            }
          }
          Err(_) => Err(FatalError("Mutex of fixed size buffer poisoned".to_string()))?,
        }
      }
    }

    Ok(())
  }
}

pub(crate) struct EmptyManager {}

impl Manager for EmptyManager {
  fn allocate(&self, _: BufferSpec) -> Result<BufferData> {
    Err(nyi!("Allocated not supported by empty manager!"))
  }

  fn deallocate(&self, _: &BufferData) -> Result<()> {
    Ok(())
  }
}
