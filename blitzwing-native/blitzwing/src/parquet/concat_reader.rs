use std::io::{Read, Result as IoResult};

#[derive(new)]
struct ConcatReader<T: Read> {
  readers: Vec<T>,
  idx: usize
}

impl<T: Read> Read for ConcatReader<T> {
  fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
    let mut bytes_read: usize = 0;

    loop {
      if self.idx >= self.readers.len() || bytes_read >= buf.len() {
        break;
      }

      let current_buf = &mut buf[bytes_read..];

      let batch_read = (&mut self.readers[self.idx]).read()?;

      bytes_read += batch_read;

      if batch_read == 0 {
        self.idx += 1;
      }
    }

    Ok(bytes_read)
  }
}