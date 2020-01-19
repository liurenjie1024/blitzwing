use crate::error::Result;
use std::io::Read;

pub(super) trait BlockReader: Read {
    fn skip(&mut self, n: usize) -> Result<()>;
}