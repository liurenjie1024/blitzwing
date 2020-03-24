pub trait Cast<T> {
  fn cast(self) -> T;
}

impl<T> Cast<T> for T {
  fn cast(self) -> T {
      self
  }
}

impl Cast<u8> for i32 {
  fn cast(self) -> u8 {
      self as u8
  }
}

impl Cast<u16> for i32 {
  fn cast(self) -> u16 {
      self as u16
  }
}

impl Cast<u32> for i32 {
  fn cast(self) -> u32 {
      self as u32
  }
}

impl Cast<i8> for i32 {
  fn cast(self) -> i8 {
      self as i8
  }
}

impl Cast<i16> for i32 {
  fn cast(self) -> i16 {
      self as i16
  }
}

impl Cast<u64> for i64 {
  fn cast(self) -> u64 {
      self as u64
  }
}
