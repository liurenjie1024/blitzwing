
use crate::error::SaslErrorKind::GsaslError;
use crate::bindings::gsasl_strerror;
use crate::bindings::gsasl_strerror_name;
use core::fmt::Debug;
use failure::Fail;
use failure::Backtrace;
use std::fmt::Formatter;
use std::fmt::Display;
use failure::Context;
use std::ffi::CStr;

#[derive(Debug)]
pub struct SaslError {
  inner: Context<SaslErrorKind>,
}

#[derive(Clone, Eq, PartialEq, Debug, Fail)]
pub enum SaslErrorKind {
  #[fail(display = "Can't find current sasl client's mechanism name.")]
  MechanismNameNotFound,
  #[fail(display = "Gsasl error: {:?}.", _0)]
  GsaslError(GsaslErrorInfo),
}

#[derive(Clone, Eq, PartialEq, new)]
pub struct GsaslErrorInfo {
  rc: i32
}

impl Debug for GsaslErrorInfo {
  
fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
  let (error_name, error_reaseon) = unsafe {
    let error_name = CStr::from_ptr(gsasl_strerror_name(self.rc));
    let error_desc = CStr::from_ptr(gsasl_strerror(self.rc));
    (error_name, error_desc)
  };
  write!(f, "Gsasl error [{}], error name: [{:?}], error reason: [{:?}]", self.rc, error_name, error_reaseon)
}
}

impl SaslError {
  pub fn kind(&self) -> &SaslErrorKind {
    self.inner.get_context()
  }

  pub fn from_gsasl_rc(rc: i32) -> Self {
    SaslError::from(GsaslError(GsaslErrorInfo::new(rc)))
  }
}

impl Display for SaslError {
  fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
    Display::fmt(&self.inner, f)
  }
}

impl Fail for SaslError {
  fn cause(&self) -> Option<&dyn Fail> {
    self.inner.cause()
  }

  fn backtrace(&self) -> Option<&Backtrace> {
    self.inner.backtrace()
  }
}

impl From<SaslErrorKind> for SaslError {
  fn from(kind: SaslErrorKind) -> Self {
    Self { inner: Context::new(kind) }
  }
}

impl From<Context<SaslErrorKind>> for SaslError {
  fn from(inner: Context<SaslErrorKind>) -> Self {
    Self { inner }
  }
}

pub type Result<T> = std::result::Result<T, SaslError>;