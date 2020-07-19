use super::buffer::{Buffer, BufferData, BufferSpec};
use crate::error::{
  BlitzwingErrorKind::{FatalError, InvalidArgumentError, LayoutError, MemoryError},
  Result,
};
use arraydeque::{ArrayDeque, Wrapping};
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
    let new_data = unsafe { memory::reallocate(buffer.as_ptr(), buffer.capacity(), new_capacity) };
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
      unsafe { memory::free_aligned(ptr, buffer.capacity()) };
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
  cached_fix_buffers: Mutex<ArrayDeque<[BufferData; 4], Wrapping>>,
  cached_var_len_buffers: Mutex<ArrayDeque<[BufferData; 4], Wrapping>>,
}

impl CachedManager {
  pub(crate) fn new(root: BufferDataManagerRef) -> Self {
    let max_cached_num = 4;
    Self {
      root,
      max_cached_num,
      cached_fix_buffers: Mutex::new(ArrayDeque::new()),
      cached_var_len_buffers: Mutex::new(ArrayDeque::new()),
    }
  }
}

impl Manager for CachedManager {
  fn allocate(&self, spec: BufferSpec) -> Result<BufferData> {
    if spec.layout().align() != ALIGNMENT {
      return Err(InvalidArgumentError(format!("Buffer alignment must be {}", ALIGNMENT)))?;
    }

    if !spec.resizable() {
      match self.cached_fix_buffers.lock() {
        Ok(mut buffers) => {
          if let Some(idx) = buffers.iter().position(|b| b.capacity() == spec.layout().size()) {
            buffers
              .remove(idx)
              .ok_or_else(|| FatalError(format!("{} element should exist!", idx)).into())
          } else {
            self.root.allocate(spec.clone())
          }
        }
        Err(_) => Err(FatalError("Mutex of fixed size buffer poisoned".to_string()))?,
      }
    } else {
      match self.cached_var_len_buffers.lock() {
        Ok(mut buffers) => {
          if let Some(idx) = buffers.iter().position(|b| b.capacity() >= spec.layout().size()) {
            buffers
              .remove(idx)
              .ok_or_else(|| FatalError(format!("{} element should exist!", idx)).into())
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
      let locked_buffers = if buffer.spec().resizable() {
        &self.cached_var_len_buffers
      } else {
        &self.cached_fix_buffers
      };

      match locked_buffers.lock() {
        Ok(mut buffers) => {
          if buffers.iter().position(|b| b == buffer).is_none() {
            if let Some(buffer_data) = buffers.push_back(buffer.clone()) {
              self.root.deallocate(&buffer_data)?
            }
          }
        }
        Err(_) => Err(FatalError("Mutex of var size buffer poisoned".to_string()))?,
      }
    }
    Ok(())
  }
}

impl CachedManager {
  fn do_free_buffer(&self, buffer_data: &BufferData) -> Result<()> {
    self.root.deallocate(buffer_data)
  }
}

impl Drop for CachedManager {
  fn drop(&mut self) {
    if let Ok(mut buffers) = self.cached_fix_buffers.lock() {
      for b in buffers.iter() {
        self.do_free_buffer(b).unwrap();
      }
      buffers.clear();
    }

    if let Ok(mut buffers) = self.cached_var_len_buffers.lock() {
      for b in buffers.iter() {
        self.do_free_buffer(b).unwrap();
      }
      buffers.clear();
    }
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

#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::Arc;
  #[test]
  fn test_cached_manager_fixed() {
    let root = Arc::new(RootManager {});

    let cached_manager = CachedManager::new(root);

    // Frist allocate fixed size buffer
    let buffer1 = cached_manager.allocate_aligned(64, false).unwrap();
    let buffer2 = cached_manager.allocate_aligned(128, false).unwrap();
    let buffer3 = cached_manager.allocate_aligned(256, false).unwrap();
    let buffer4 = cached_manager.allocate_aligned(512, false).unwrap();

    cached_manager.deallocate(&buffer1).unwrap();
    cached_manager.deallocate(&buffer2).unwrap();
    cached_manager.deallocate(&buffer3).unwrap();
    cached_manager.deallocate(&buffer4).unwrap();

    assert_eq!(buffer1, cached_manager.allocate_aligned(64, false).unwrap());
    assert_eq!(buffer2, cached_manager.allocate_aligned(128, false).unwrap());
    assert_eq!(buffer3, cached_manager.allocate_aligned(256, false).unwrap());
    assert_eq!(buffer4, cached_manager.allocate_aligned(512, false).unwrap());

    assert!(cached_manager.allocate_aligned(1024, false).is_ok());
  }

  #[test]
  fn test_cached_manager_resizable() {
    let root = Arc::new(RootManager {});

    let cached_manager = CachedManager::new(root);

    // Frist allocate fixed size buffer
    let buffer1 = cached_manager.allocate_aligned(63, true).unwrap();
    let buffer2 = cached_manager.allocate_aligned(127, true).unwrap();
    let buffer3 = cached_manager.allocate_aligned(255, true).unwrap();
    let buffer4 = cached_manager.allocate_aligned(511, true).unwrap();

    cached_manager.deallocate(&buffer1).unwrap();
    cached_manager.deallocate(&buffer2).unwrap();
    cached_manager.deallocate(&buffer3).unwrap();
    cached_manager.deallocate(&buffer4).unwrap();

    assert_eq!(buffer1, cached_manager.allocate_aligned(63, true).unwrap());
    assert_eq!(buffer2, cached_manager.allocate_aligned(127, true).unwrap());
    assert_eq!(buffer3, cached_manager.allocate_aligned(255, true).unwrap());
    assert_eq!(buffer4, cached_manager.allocate_aligned(511, true).unwrap());

    assert!(cached_manager.allocate_aligned(1023, true).is_ok());
  }

  #[test]
  fn test_cached_manager_drop() {
    let root = Arc::new(RootManager {});

    let cached_manager = CachedManager::new(root);

    let buffer1 = cached_manager.allocate_aligned(63, true).unwrap();
    let buffer2 = cached_manager.allocate_aligned(128, false).unwrap();

    cached_manager.deallocate(&buffer1).unwrap();
    cached_manager.deallocate(&buffer2).unwrap();
    cached_manager.deallocate(&buffer1).unwrap();
    cached_manager.deallocate(&buffer2).unwrap();

    // Should not panic when droppped
  }
}
