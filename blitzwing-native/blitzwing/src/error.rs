use failure::{Backtrace, Context, Fail};
use std::{
  alloc::Layout,
  fmt::{Display, Formatter},
};

#[derive(Debug)]
pub struct BlitzwingError {
  inner: Context<BlitzwingErrorKind>,
}

#[derive(Clone, Eq, PartialEq, Debug, Fail)]
pub enum BlitzwingErrorKind {
  #[fail(display = "Failed to convert to/from protobuf")]
  ProtobufError,
  #[fail(display = "Failed to allocate memory: {:?}", _0)]
  MemoryError(Layout),
  #[fail(display = "Planner error happened: {}", _0)]
  PlanError(String),
  #[fail(display = "Jni error happened")]
  JniError,
  #[fail(display = "Fatal error happened: {}", _0)]
  FatalError(String),
  #[fail(display = "Parquet error happened")]
  ParquetError,
  #[fail(display = "Arrow error happened")]
  ArrowError,
  #[fail(display = "Not yet implemented: {}", _0)]
  NotYetImplementedError(String),
  #[fail(display = "Invalid argument: {}", _0)]
  InvalidArgumentError(String),
  #[fail(display = "Null pointer")]
  NullPointerError,
  #[fail(display = "Layout error")]
  LayoutError,
  #[fail(display = "IO error")]
  IoError,
}

impl BlitzwingError {
  pub fn kind(&self) -> &BlitzwingErrorKind {
    self.inner.get_context()
  }
}

impl Display for BlitzwingError {
  fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
    Display::fmt(&self.inner, f)
  }
}

impl Fail for BlitzwingError {
  fn cause(&self) -> Option<&dyn Fail> {
    self.inner.cause()
  }

  fn backtrace(&self) -> Option<&Backtrace> {
    self.inner.backtrace()
  }
}

impl From<BlitzwingErrorKind> for BlitzwingError {
  fn from(kind: BlitzwingErrorKind) -> Self {
    Self { inner: Context::new(kind) }
  }
}

impl From<Context<BlitzwingErrorKind>> for BlitzwingError {
  fn from(inner: Context<BlitzwingErrorKind>) -> Self {
    Self { inner }
  }
}

pub type Result<T> = std::result::Result<T, BlitzwingError>;

macro_rules! nyi {
  ($fmt:expr) => {
    crate::error::BlitzwingError::from(crate::error::BlitzwingErrorKind::NotYetImplementedError($fmt.to_owned()))
  };
  ($fmt:expr, $($args:tt)*) => {
    crate::error::BlitzwingError::from(crate::error::BlitzwingErrorKind::NotYetImplementedError(format!($fmt, $($args)*)))
  };
}
