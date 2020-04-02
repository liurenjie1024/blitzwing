use std::sync::Arc;
use std::alloc::Layout;
use crate::error::Result;
use super::buffer::{Buffer, BufferData};

pub(crate) type BufferManagerRef = Arc<dyn Manager>;
pub(crate) trait Manager {
  fn allocate(&self, layout: Layout) -> Result<Buffer>;
  fn deallocate(&self, buffer_data: BufferData) -> Result<()>;
}
