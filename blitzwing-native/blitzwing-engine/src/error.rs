use failure::{Backtrace, Context, Fail};
use std::alloc::Layout;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub struct ArrowExecutorError {
    inner: Context<ArrowExecutorErrorKind>,
}

#[derive(Clone, Eq, PartialEq, Debug, Fail)]
pub enum ArrowExecutorErrorKind {
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
}

impl ArrowExecutorError {
    pub fn kind(&self) -> &ArrowExecutorErrorKind {
        self.inner.get_context()
    }
}

impl Display for ArrowExecutorError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl Fail for ArrowExecutorError {
    fn cause(&self) -> Option<&dyn Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl From<ArrowExecutorErrorKind> for ArrowExecutorError {
    fn from(kind: ArrowExecutorErrorKind) -> Self {
        Self {
            inner: Context::new(kind),
        }
    }
}

impl From<Context<ArrowExecutorErrorKind>> for ArrowExecutorError {
    fn from(inner: Context<ArrowExecutorErrorKind>) -> Self {
        Self { inner }
    }
}

pub type Result<T> = std::result::Result<T, ArrowExecutorError>;
