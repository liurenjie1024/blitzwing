use arrow::{
  buffer::{Buffer, MutableBuffer},
  datatypes::ArrowNativeType,
};

pub(crate) struct BufferOps {
  inner: Buffer,
}

impl BufferOps {
  pub(crate) fn new(inner: Buffer) -> Self {
    Self { inner }
  }
}

impl AsRef<[u8]> for BufferOps {
  fn as_ref(&self) -> &[u8] {
    self.inner.data()
  }
}

pub(crate) struct MutableBufferOps {
  inner: MutableBuffer,
}

impl<T: ArrowNativeType + num::Num> AsMut<[T]> for MutableBufferOps {
  fn as_mut(&mut self) -> &mut [T] {
    unsafe { self.inner.typed_data_mut() }
  }
}

impl<T: ArrowNativeType + num::Num> AsRef<[T]> for MutableBufferOps {
  fn as_ref(&self) -> &[T] {
    unsafe { self.inner.typed_data() }
  }
}

impl MutableBufferOps {
  pub(crate) fn new(inner: MutableBuffer) -> Self {
    Self { inner }
  }

  pub(crate) fn inner(&self) -> &MutableBuffer {
    &self.inner
  }

  pub(crate) fn inner_mut(&mut self) -> &mut MutableBuffer {
    &mut self.inner
  }
}
