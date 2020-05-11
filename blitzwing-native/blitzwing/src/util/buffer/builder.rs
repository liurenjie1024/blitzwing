// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Defines a `BufferBuilder` capable of creating a `Buffer` which can be used as an
//! internal buffer in an `ArrayData` object.

use std::marker::PhantomData;

use crate::{
  error::{BlitzwingErrorKind::IoError, Result},
  util::buffer::{manager::BufferManager, Buffer},
};
use arrow::{
  datatypes::{ArrowNumericType, ToByteSlice},
};
use failure::ResultExt;
use std::{io::Write, mem};
/// Buffer builder with zero-copy build method
pub struct BufferBuilder<T> {
  buffer: Buffer,
  len: usize,
  buffer_manager: BufferManager,
  _marker: PhantomData<T>,
}

// Trait for buffer builder. This is used mainly to offer separate implementations for
// numeric types and boolean types, while still be able to call methods on buffer builder
// with generic primitive type.
pub trait BufferBuilderTrait: Sized {
  type Native;
  fn new(capacity: usize, buffer_manager: BufferManager) -> Result<Self>;
  fn with_default_buffer_manager(capacity: usize) -> Result<Self> {
    Self::new(capacity, BufferManager::default())
  }
  fn len(&self) -> usize;
  fn capacity(&self) -> usize;
  fn advance(&mut self, i: usize) -> Result<()>;
  fn reserve(&mut self, n: usize) -> Result<()>;
  fn append(&mut self, v: Self::Native) -> Result<()>;
  fn append_slice(&mut self, slice: &[Self::Native]) -> Result<()>;
  fn finish(&mut self) -> Buffer;
}

impl<T: ArrowNumericType> BufferBuilderTrait for BufferBuilder<T> {
  type Native = T::Native;
  /// Creates a builder with a fixed initial capacity
  fn new(capacity: usize, buffer_manager: BufferManager) -> Result<Self> {
    Ok(Self {
      buffer: buffer_manager.allocate_aligned(capacity, false)?,
      len: 0,
      buffer_manager,
      _marker: PhantomData,
    })
  }

  /// Returns the number of array elements (slots) in the builder
  fn len(&self) -> usize {
    self.len
  }

  /// Returns the current capacity of the builder (number of elements)
  fn capacity(&self) -> usize {
    let bit_capacity = self.buffer.capacity() * 8;
    bit_capacity / T::get_bit_width()
  }

  // Advances the `len` of the underlying `Buffer` by `i` slots of type T
  fn advance(&mut self, i: usize) -> Result<()> {
    let new_buffer_len = (self.len + i) * mem::size_of::<T::Native>();
    self.buffer.resize(new_buffer_len)?;
    self.len += i;
    Ok(())
  }

  /// Reserves memory for `n` elements of type `T`.
  fn reserve(&mut self, n: usize) -> Result<()> {
    let new_capacity = self.len + n;
    let byte_capacity = mem::size_of::<T::Native>() * new_capacity;
    self.buffer.reserve(byte_capacity)?;
    Ok(())
  }

  /// Appends a value into the builder, growing the internal buffer as needed.
  fn append(&mut self, v: T::Native) -> Result<()> {
    self.reserve(1)?;
    self.write_bytes(v.to_byte_slice(), 1)
  }

  /// Appends a slice of type `T`, growing the internal buffer as needed.
  fn append_slice(&mut self, slice: &[T::Native]) -> Result<()> {
    let array_slots = slice.len();
    self.reserve(array_slots)?;
    self.write_bytes(slice.to_byte_slice(), array_slots)
  }

  /// Reset this builder and returns an immutable `Buffer`.
  fn finish(&mut self) -> Buffer {
    let buf = std::mem::replace(&mut self.buffer, Buffer::default());
    self.len = 0;
    buf
  }
}

impl<T: ArrowNumericType> BufferBuilder<T> {
  /// Writes a byte slice to the underlying buffer and updates the `len`, i.e. the
  /// number array elements in the builder.  Also, converts the `io::Result`
  /// required by the `Write` trait to the Arrow `Result` type.
  fn write_bytes(&mut self, bytes: &[u8], len_added: usize) -> Result<()> {
    self.buffer.write(bytes).context(IoError)?;
    self.len += len_added;
    Ok(())
  }
}

pub struct BooleanBufferBuilder {
  buffer: Buffer,
  len: usize,
  buffer_manager: BufferManager,
}

impl BufferBuilderTrait for BooleanBufferBuilder {
  type Native = bool;
  /// Creates a builder with a fixed initial capacity.
  fn new(capacity: usize, buffer_manager: BufferManager) -> Result<Self> {
    Ok(Self { buffer: buffer_manager.allocate_aligned(capacity, false)?, len: 0, buffer_manager })
  }

  /// Returns the number of array elements (slots) in the builder
  fn len(&self) -> usize {
    self.len
  }

  /// Returns the current capacity of the builder (number of elements)
  fn capacity(&self) -> usize {
    self.buffer.capacity()
  }

  // Advances the `len` of the underlying `Buffer` by `i` slots of type T
  fn advance(&mut self, i: usize) -> Result<()> {
    let new_buffer_len = self.len + i;
    self.buffer.resize(new_buffer_len)?;
    self.len += i;
    Ok(())
  }

  /// Appends a value into the builder, growing the internal buffer as needed.
  fn append(&mut self, v: bool) -> Result<()> {
    self.reserve(1)?;
    let buf = if v {
      &[1u8]
    } else {
      &[0u8]
    };

    self.buffer.write(buf).expect("Should not happen!");
    self.len += 1;
    Ok(())
  }

  /// Appends a slice of type `T`, growing the internal buffer as needed.
  fn append_slice(&mut self, slice: &[bool]) -> Result<()> {
    self.reserve(slice.len())?;
    for v in slice {
      if *v {
        self.buffer.write(&[1u8]).context(IoError)?;
      } else {
        self.buffer.write(&[0u8]).context(IoError)?;
      }
    }
    self.len += slice.len();
    Ok(())
  }

  /// Reserves memory for `n` elements of type `T`.
  fn reserve(&mut self, n: usize) -> Result<()> {
    let new_capacity = self.len + n;
    if new_capacity > self.capacity() {
      self.buffer.reserve(new_capacity)?;
    }
    Ok(())
  }

  /// Reset this builder and returns an immutable `Buffer`.
  fn finish(&mut self) -> Buffer {
    // `append` does not update the buffer's `len` so do it before `freeze` is called.
    std::mem::replace(&mut self.buffer, Buffer::default())
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::util::buffer::*;
  use std::convert::TryFrom;

  #[test]
  fn test_builder_i32_empty() {
    let mut b = Int32BufferBuilder::with_default_buffer_manager(5).unwrap();
    assert_eq!(0, b.len());
    assert_eq!(16, b.capacity());
    let a = b.finish();
    assert_eq!(0, a.len());
  }

  #[test]
  fn test_builder_i32_alloc_zero_bytes() {
    let mut b = Int32BufferBuilder::with_default_buffer_manager(0).unwrap();
    b.append(123).unwrap();
    let a = b.finish();
    assert_eq!(4, a.len());
  }

  #[test]
  fn test_builder_i32() {
    let mut b = Int32BufferBuilder::with_default_buffer_manager(5).unwrap();
    for i in 0..5 {
      b.append(i).unwrap();
    }
    assert_eq!(16, b.capacity());
    let a = b.finish();
    assert_eq!(20, a.len());
  }

  #[test]
  fn test_builder_i32_grow_buffer() {
    let mut b = Int32BufferBuilder::with_default_buffer_manager(2).unwrap();
    assert_eq!(16, b.capacity());
    for i in 0..20 {
      b.append(i).unwrap();
    }
    assert_eq!(32, b.capacity());
    let a = b.finish();
    assert_eq!(80, a.len());
  }

  #[test]
  fn test_builder_finish() {
    let mut b = Int32BufferBuilder::with_default_buffer_manager(5).unwrap();
    assert_eq!(16, b.capacity());
    for i in 0..10 {
      b.append(i).unwrap();
    }
    let mut a = b.finish();
    assert_eq!(40, a.len());
    assert_eq!(0, b.len());
    assert_eq!(0, b.capacity());

    // Try build another buffer after cleaning up.
    for i in 0..20 {
      b.append(i).unwrap()
    }
    assert_eq!(32, b.capacity());
    a = b.finish();
    assert_eq!(80, a.len());
  }

  #[test]
  fn test_reserve() {
    let mut b = UInt8BufferBuilder::with_default_buffer_manager(2).unwrap();
    assert_eq!(64, b.capacity());
    b.reserve(64).unwrap();
    assert_eq!(64, b.capacity());
    b.reserve(65).unwrap();
    assert_eq!(128, b.capacity());

    let mut b = Int32BufferBuilder::with_default_buffer_manager(2).unwrap();
    assert_eq!(16, b.capacity());
    b.reserve(16).unwrap();
    assert_eq!(16, b.capacity());
    b.reserve(17).unwrap();
    assert_eq!(32, b.capacity());
  }

  #[test]
  fn test_append_slice() {
    let mut b = UInt8BufferBuilder::with_default_buffer_manager(0).unwrap();
    b.append_slice("Hello, ".as_bytes()).unwrap();
    b.append_slice("World!".as_bytes()).unwrap();
    let buffer = b.finish();
    assert_eq!(13, buffer.len());

    let mut b = Int32BufferBuilder::with_default_buffer_manager(0).unwrap();
    b.append_slice(&[32, 54]).unwrap();
    let buffer = b.finish();
    assert_eq!(8, buffer.len());
  }

  #[test]
  fn test_write_bytes_i32() {
    let mut b = Int32BufferBuilder::with_default_buffer_manager(4).unwrap();
    let bytes = [8, 16, 32, 64].to_byte_slice();
    b.write_bytes(bytes, 4).unwrap();
    assert_eq!(4, b.len());
    assert_eq!(16, b.capacity());
    let buffer = b.finish();
    assert_eq!(16, buffer.len());
  }

  // #[test]
  fn test_write_bytes() {
    let mut b = BooleanBufferBuilder::with_default_buffer_manager(4)
      .expect("Failed to allocate boolean buffer builder!");
    b.append(false).unwrap();
    b.append(true).unwrap();
    b.append(false).unwrap();
    b.append(true).unwrap();
    assert_eq!(4, b.len());
    assert_eq!(512, b.capacity());
    let buffer = b.finish();
    assert_eq!(1, buffer.len());

    let mut b = BooleanBufferBuilder::with_default_buffer_manager(4)
      .expect("Failed to allocate boolean buffer builder!");
    b.append_slice(&[false, true, false, true]).unwrap();
    assert_eq!(4, b.len());
    assert_eq!(512, b.capacity());
    let buffer = b.finish();
    assert_eq!(1, buffer.len());
  }

  // #[test]
  fn test_boolean_builder_increases_buffer_len() {
    // 00000010 01001000
    let buf =
      Buffer::try_from(&[72_u8, 2_u8] as &[u8]).expect("Failed to create buffer from slice!");
    let mut builder = BooleanBufferBuilder::with_default_buffer_manager(8)
      .expect("Failed to allocate boolean buffer builder!");

    for i in 0..10 {
      if i == 3 || i == 6 || i == 9 {
        builder.append(true).unwrap();
      } else {
        builder.append(false).unwrap();
      }
    }
    let buf2 = builder.finish();

    assert_eq!(buf, buf2);
  }
}
