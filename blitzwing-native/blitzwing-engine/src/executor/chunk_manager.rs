use std::alloc::Layout;

use crate::error::{ArrowExecutorErrorKind, Result};

pub struct ChunkManager {}

impl ChunkManager {
    pub fn new() -> Self {
        Self {}
    }

    pub fn allocate(&self, layout: Layout) -> Result<*mut u8> {
        let ret = unsafe { std::alloc::alloc(layout.clone()) };

        if ret.is_null() {
            Err(ArrowExecutorErrorKind::MemoryError(layout.clone()))?
        } else {
            Ok(ret)
        }
    }

    pub fn deallocate(&self, ptr: *mut u8, layout: Layout) -> Result<()> {
        Ok(unsafe { std::alloc::dealloc(ptr, layout) })
    }
}
