use crate::error::{
  HdfsLibError,
  HdfsLibErrorKind::{FromUtf8Error, PathError},
  Result,
};
use failure::ResultExt;
use std::{convert::TryFrom, str::from_utf8, sync::Arc};
use url::Url;

#[derive(Debug, Clone)]
pub struct FsPath {
  url: Url,
}

pub struct FsPathBuilder {
  url: Url,
}

impl FsPathBuilder {
  pub fn new(base: &str) -> Result<Self> {
    Ok(Self { url: Url::parse(base).context(PathError)? })
  }

  pub fn append<B: AsRef<[u8]>>(self, bytes: B) -> Result<Self> {
    let new_url =
      self.url.join(from_utf8(bytes.as_ref()).context(FromUtf8Error)?).context(PathError)?;
    Ok(Self { url: new_url })
  }

  pub fn build(self) -> Result<FsPath> {
    Ok(FsPath { url: self.url })
  }
}

pub type FsPathRef = Arc<FsPath>;

impl FsPath {
  pub fn to_string(&self) -> String {
    self.url.as_str().to_string()
  }

  pub fn scheme(&self) -> &str {
    self.url.scheme()
  }

  pub fn authority(&self) -> String {
    let mut authority = String::with_capacity(32);
    authority.push_str(self.url.username());

    if let Some(passwd) = self.url.password() {
      authority.push(':');
      authority.push_str(passwd);
    }

    if authority.len() > 0 {
      authority.push('@');
    }

    if let Some(host_str) = self.url.host_str() {
      authority.push_str(host_str);
    }

    if let Some(port) = self.url.port() {
      authority.push(':');
      authority.push_str(port.to_string().as_str());
    }

    authority
  }

  pub fn file_path(&self) -> &str {
    self.url.path()
  }

  pub fn base(&self) -> Result<Self> {
    Self::try_from(format!("{}://{}", self.url.scheme(), self.authority()).as_str())
  }

  pub fn as_str(&self) -> &str {
    self.url.as_str()
  }
}

impl AsRef<[u8]> for FsPath {
  fn as_ref(&self) -> &[u8] {
    self.as_str().as_bytes()
  }
}

impl<'a> TryFrom<&'a [u8]> for FsPath {
  type Error = HdfsLibError;

  fn try_from(value: &'a [u8]) -> Result<Self> {
    let s = from_utf8(value).context(FromUtf8Error)?;
    FsPath::try_from(s)
  }
}

impl<'a> TryFrom<&'a str> for FsPath {
  type Error = HdfsLibError;

  fn try_from(value: &'a str) -> Result<Self> {
    let url = Url::parse(value).context(PathError)?;
    Ok(FsPath { url })
  }
}

#[cfg(test)]
mod tests {
  use crate::fs::path::FsPath;
  use std::convert::TryInto;

  #[test]
  fn test_scheme() {
    let path: FsPath = "unix:home/folder".try_into().unwrap();
    assert_eq!("home/folder", path.file_path());
  }
}
