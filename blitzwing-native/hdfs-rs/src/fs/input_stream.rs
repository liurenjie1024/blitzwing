use crate::error::Result;
use std::io::{Read, Seek};

pub trait FsInputStream: Read + Seek {
  fn skip(&mut self, len: usize) -> Result<usize>;
}
