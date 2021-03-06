use crate::{
  hadoop_proto::RpcHeader::{RpcResponseHeaderProto, RpcResponseHeaderProto_RpcErrorCodeProto},
  hdfs::block::ExtendedBlock,
};
use failure::{Backtrace, Context, Fail};
use std::{
  ffi::OsString,
  fmt::{Display, Formatter},
  time::Duration,
};

use crate::{
  hadoop_proto::datatransfer::{BlockOpResponseProto, Status as BlockOpStatus},
  hdfs::transfer::data_transfer_protocol::BaseBlockOpInfo,
};

#[derive(Debug)]
pub struct HdfsLibError {
  inner: Context<HdfsLibErrorKind>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct RpcRemoteErrorInfo {
  exception_class_name: String,
  message: String,
  error_code: RpcResponseHeaderProto_RpcErrorCodeProto,
}

impl<'a> From<&'a RpcResponseHeaderProto> for RpcRemoteErrorInfo {
  fn from(header: &'a RpcResponseHeaderProto) -> Self {
    Self {
      exception_class_name: header.get_exceptionClassName().to_string(),
      message: header.get_errorMsg().to_string(),
      error_code: header.get_errorDetail(),
    }
  }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct BlockOpErrorInfo {
  // TODO: Add local and remote add info
  base_info: BaseBlockOpInfo,
  status: BlockOpStatus,
  message: String,
}

impl BlockOpErrorInfo {
  pub fn new(base_info: BaseBlockOpInfo, response: &BlockOpResponseProto) -> Self {
    Self { base_info, status: response.get_status(), message: response.get_message().to_string() }
  }

  pub fn to_err(self) -> HdfsLibError {
    let error_kind = match self.status {
      BlockOpStatus::ERROR_ACCESS_TOKEN => HdfsLibErrorKind::InvalidBlockTokenError(self),
      _ => HdfsLibErrorKind::BlockOperationError(self),
    };

    HdfsLibError::from(error_kind)
  }
}

#[derive(Debug, new, Clone, Eq, PartialEq)]
pub struct MissingBlockErrorInfo {
  block: Option<ExtendedBlock>,
  filename: String,
  offset: usize,
}

#[derive(Clone, Eq, PartialEq, Debug, Fail)]
pub enum HdfsLibErrorKind {
  #[fail(display = "Invalid argument: {}", _0)]
  InvalidArgumentError(String),
  #[fail(display = "Illegal state: {}", _0)]
  IllegalStateError(String),
  #[fail(display = "Protobuf error happened")]
  ProtobufError,
  #[fail(display = "System error happened: {}", _0)]
  SystemError(String),
  #[fail(display = "Io error happened")]
  IoError,
  #[fail(display = "Illegal utf8 string")]
  FromUtf8Error,
  #[fail(display = "Can't convert from OsString[{:?}] to String", _0)]
  FromOsStringError(OsString),
  #[fail(display = "Failed to get environment")]
  GetEnvError,
  #[fail(display = "Illegal path string")]
  PathError,
  #[fail(display = "Illegal socket address string: {}", _0)]
  SocketAddressParseError(String),
  #[fail(display = "Lock status is incorrect")]
  LockError,
  #[fail(display = "Timeout after: {:?}", _0)]
  TimeOutError(Duration),
  #[fail(display = "Rpc remote error happened: {:?}", _0)]
  RpcRemoteError(RpcRemoteErrorInfo),
  #[fail(display = "Failed to join tokio task")]
  TaskJoinError,
  #[fail(display = "Failed to pass messages")]
  SyncError,
  #[fail(display = "Failed to parse configuration: {}", _0)]
  ConfigError(String),
  #[fail(display = "Error happened in hdfs client protocol: {}", _0)]
  ProtocolError(String),
  #[fail(display = "Error in sasl client")]
  SaslError,

  // Block operation related error
  #[fail(display = "Block operation failed: {:?}", _0)]
  BlockOperationError(BlockOpErrorInfo),
  #[fail(display = "Invalid block token: {:?}", _0)]
  InvalidBlockTokenError(BlockOpErrorInfo),

  // Hdfs Client related errors
  #[fail(display = "Missing block error: {:?}", _0)]
  MissingBlockError(MissingBlockErrorInfo),
}

impl HdfsLibError {
  pub fn kind(&self) -> &HdfsLibErrorKind {
    self.inner.get_context()
  }

  pub fn into_std_io_error(self) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, failure::Error::from(self))
  }
}

impl Display for HdfsLibError {
  fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
    Display::fmt(&self.inner, f)
  }
}

impl Fail for HdfsLibError {
  fn cause(&self) -> Option<&dyn Fail> {
    self.inner.cause()
  }

  fn backtrace(&self) -> Option<&Backtrace> {
    self.inner.backtrace()
  }
}

impl From<HdfsLibErrorKind> for HdfsLibError {
  fn from(kind: HdfsLibErrorKind) -> Self {
    Self { inner: Context::new(kind) }
  }
}

impl From<Context<HdfsLibErrorKind>> for HdfsLibError {
  fn from(inner: Context<HdfsLibErrorKind>) -> Self {
    Self { inner }
  }
}

pub type Result<T> = std::result::Result<T, HdfsLibError>;

// macros for helping to generate error
macro_rules! invalid_argument {
    ($fmt:expr, $($arg:tt)*) => {
        return Err(crate::error::HdfsLibError::from
        (crate::error::HdfsLibErrorKind::InvalidArgumentError(format!
        ($fmt, $
        ($arg)*))));
    };
}

macro_rules! check_args {
    ($cond:expr) => {
        if !($cond) {
            invalid_argument!("{}", stringify!($cond));
        }
    };
    ($cond:expr, $fmt:expr, $($arg:tt)*) => {
        if !($cond) {
            invalid_argument!($fmt, $($arg)*);
        }
    };
}

macro_rules! invalid_state {
    ($fmt:expr, $($arg:tt)*) => {
        return Err(crate::error::HdfsLibError::from
        (crate::error::HdfsLibErrorKind::IllegalStateError(format!
        ($fmt, $
        ($arg)*))));
    };
}

macro_rules! check_state {
    ($cond:expr) => {
        if !($cond) {
          invalid_state!("Illegal state: [{}]", stringify!($cond));
        }
    };
    ($cond:expr, $fmt:expr, $($arg:tt)*) => {
        if !($cond) {
          invalid_state!("Illegal state: [{}], {}", stringify!($cond), format!($fmt, $
                        ($arg)*));
        }
    };
}

macro_rules! check_protocol_content {
    ($cond:expr) => {
        if !($cond) {
            return Err(crate::error::HdfsLibError::from(
                crate::error::HdfsLibErrorKind::ProtocolError(
                    format!("Condition check [{}] failed", stringify!($cond)))));
        }
    };
    ($cond:expr, $fmt:expr, $($arg:tt)*) => {
        if !($cond) {
            return Err(crate::error::HdfsLibError::from(
                crate::error::HdfsLibErrorKind::ProtocolError(
                    format!("Condition check [{}] failed: {}", stringify!($cond), format!($fmt, $
                    ($arg)*)))));
        }
    };
}

#[cfg(test)]
mod tests {

  #[test]
  fn test_check_args() {
    let e = || {
      let (a, b) = (1, 2);
      check_args!(a == b);

      Ok(())
    };

    let r = e();
    assert!(r.is_err());
    println!("{}", r.err().unwrap());
  }
}
