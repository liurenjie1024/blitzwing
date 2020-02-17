use crate::error::Result;
use std::io::{Read, Seek};

pub type FsInputStreamRef = Box<dyn FsInputStream>;

pub trait FsInputStream: Read + Seek {
  fn skip(&mut self, len: usize) -> Result<usize>;
}
