use std::fmt::{Display, Formatter};
use failure::{Backtrace, Context, Fail};

pub struct BlitzwingParquetError {
  inner: Context<BlitzwingParquetErrorKind>
}

pub enum BlitzwingParquetErrorKind {
}

impl BlitzwingParquetError {
  pub fn kind(&self) -> &BlitzwingParquetErrorKind {
    self.inner.get_context()
  }
}

impl Display for BlitzwingParquetError {
  fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
    Display::fmt(&self.inner, f)
  }
}

impl Fail for BlitzwingParquetError {
  fn cause(&self) -> Option<&dyn Fail> {
    self.inner.cause()
  }

  fn backtrace(&self) -> Option<&Backtrace> {
    self.inner.backtrace()
  }
}

impl From<BlitzwingParquetErrorKind> for BlitzwingParquetError {
  fn from(kind: BlitzwingParquetErrorKind) -> Self {
    Self { inner: Context::new(kind) }
  }
}

impl From<Context<BlitzwingParquetErrorKind>> for BlitzwingParquetError {
  fn from(inner: Context<BlitzwingParquetErrorKind>) -> Self {
    Self { inner }
  }
}

pub type Result<T> = std::result::Result<T, BlitzwingParquetError>;