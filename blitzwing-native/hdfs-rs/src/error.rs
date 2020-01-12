use failure::{Backtrace, Context, Fail};
use std::fmt::{Display, Formatter};
use std::time::Duration;
use crate::hadoop_proto::RpcHeader::{RpcResponseHeaderProto,
                                     RpcResponseHeaderProto_RpcErrorCodeProto};

#[derive(Debug)]
pub struct HdfsLibError {
    inner: Context<HdfsLibErrorKind>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct RpcRemoteErrorInfo {
    exception_class_name: String,
    message: String,
    error_code: RpcResponseHeaderProto_RpcErrorCodeProto
}

impl<'a> From<&'a RpcResponseHeaderProto> for RpcRemoteErrorInfo {
    fn from(header: &'a RpcResponseHeaderProto) -> Self {
        Self {
            exception_class_name: header.get_exceptionClassName().to_string(),
            message: header.get_errorMsg().to_string(),
            error_code: header.get_errorDetail()
        }
    }
}

#[derive(Clone, Eq, PartialEq, Debug, Fail)]
pub enum HdfsLibErrorKind {
    #[fail(display = "Invalid argument: {}", _0)]
    InvalidArgumentError(String),
    #[fail(display = "Protobuf error happened")]
    ProtobufError,
    #[fail(display = "System error happened: {}", _0)]
    SystemError(String),
    #[fail(display = "Io error happened")]
    IoError,
    #[fail(display = "Illegal utf8 string")]
    FromUtf8Error,
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
    
}


impl HdfsLibError {
    pub fn kind(&self) -> &HdfsLibErrorKind {
        self.inner.get_context()
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
        Self {
            inner: Context::new(kind),
        }
    }
}

impl From<Context<HdfsLibErrorKind>> for HdfsLibError {
    fn from(inner: Context<HdfsLibErrorKind>) -> Self {
        Self { inner }
    }
}

pub type Result<T> = std::result::Result<T, HdfsLibError>;
