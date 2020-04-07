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

use crate::util::buffer::Buffer;
use crate::error::Result;
use arrow::util::bit_util;
use arrow::datatypes::{ArrowPrimitiveType, BooleanType};
use crate::util::buffer::manager::BufferManager;

/// Buffer builder with zero-copy build method
pub struct BufferBuilder<T: ArrowPrimitiveType> {
    buffer: Buffer,
    len: usize,
    buffer_manager: BufferManager,
    _marker: PhantomData<T>,
}

// Trait for buffer builder. This is used mainly to offer separate implementations for
// numeric types and boolean types, while still be able to call methods on buffer builder
// with generic primitive type.
pub trait BufferBuilderTrait<T: ArrowPrimitiveType>: Sized {
    fn new(capacity: usize, buffer_manager: BufferManager) -> Result<Self>;
    fn with_default_buffer_manager(capacity: usize) -> Result<Self> {
        Self::new(capacity, BufferManager::default())
    }
    fn len(&self) -> usize;
    fn capacity(&self) -> usize;
    fn advance(&mut self, i: usize) -> Result<()>;
    fn reserve(&mut self, n: usize) -> Result<()>;
    fn append(&mut self, v: T::Native) -> Result<()>;
    fn append_slice(&mut self, slice: &[T::Native]) -> Result<()>;
    fn finish(&mut self) -> Buffer;
}

impl BufferBuilderTrait<BooleanType> for BufferBuilder<BooleanType> {
    /// Creates a builder with a fixed initial capacity.
    fn new(capacity: usize, buffer_manager: BufferManager) -> Result<Self> {
        Ok(Self {
            buffer: buffer_manager.allocate_aligned(capacity)?,
            len: 0,
            buffer_manager,
            _marker: PhantomData 
        })
    }

    /// Returns the number of array elements (slots) in the builder
    fn len(&self) -> usize {
        self.len
    }

    /// Returns the current capacity of the builder (number of elements)
    fn capacity(&self) -> usize {
        self.buffer.capacity() * 8
    }

    // Advances the `len` of the underlying `Buffer` by `i` slots of type T
    fn advance(&mut self, i: usize) -> Result<()> {
        let new_buffer_len = bit_util::ceil(self.len + i, 8);
        self.buffer.resize(new_buffer_len)?;
        self.len += i;
        Ok(())
    }

    /// Appends a value into the builder, growing the internal buffer as needed.
    fn append(&mut self, v: bool) -> Result<()> {
        self.reserve(1)?;
        if v {
            // For performance the `len` of the buffer is not updated on each append but
            // is updated in the `freeze` method instead.
            unsafe {
                bit_util::set_bit_raw(self.buffer.raw_data() as *mut u8, self.len);
            }
        }
        self.len += 1;
        Ok(())
    }

    /// Appends a slice of type `T`, growing the internal buffer as needed.
    fn append_slice(&mut self, slice: &[bool]) -> Result<()> {
        self.reserve(slice.len())?;
        for v in slice {
            if *v {
                // For performance the `len` of the buffer is not
                // updated on each append but is updated in the
                // `freeze` method instead.
                unsafe {
                    bit_util::set_bit_raw(self.buffer.raw_data() as *mut u8, self.len);
                }
            }
            self.len += 1;
        }
        Ok(())
    }

    /// Reserves memory for `n` elements of type `T`.
    fn reserve(&mut self, n: usize) -> Result<()> {
        let new_capacity = self.len + n;
        if new_capacity > self.capacity() {
            let new_byte_capacity = bit_util::ceil(new_capacity, 8);
            let existing_capacity = self.buffer.capacity();
            let new_capacity = self.buffer.reserve(new_byte_capacity)?;
            self.buffer
                .set_null_bits(existing_capacity, new_capacity - existing_capacity);
        }
        Ok(())
    }

    /// Reset this builder and returns an immutable `Buffer`.
    fn finish(&mut self) -> Buffer {
        // `append` does not update the buffer's `len` so do it before `freeze` is called.
        let new_buffer_len = bit_util::ceil(self.len, 8);
        debug_assert!(new_buffer_len >= self.buffer.len());
        let mut buf = std::mem::replace(&mut self.buffer, Buffer::default());
        self.len = 0;
        buf.resize(new_buffer_len).unwrap();
        buf
    }
}

pub(crate) type BooleanBufferBuilder = BufferBuilder<BooleanType>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryFrom;

    #[test]
    fn test_write_bytes() {
        let mut b = BooleanBufferBuilder::with_default_buffer_manager(4).expect("Failed to allocate boolean buffer builder!");
        b.append(false).unwrap();
        b.append(true).unwrap();
        b.append(false).unwrap();
        b.append(true).unwrap();
        assert_eq!(4, b.len());
        assert_eq!(512, b.capacity());
        let buffer = b.finish();
        assert_eq!(1, buffer.len());

        let mut b = BooleanBufferBuilder::with_default_buffer_manager(4).expect("Failed to allocate boolean buffer builder!");
        b.append_slice(&[false, true, false, true]).unwrap();
        assert_eq!(4, b.len());
        assert_eq!(512, b.capacity());
        let buffer = b.finish();
        assert_eq!(1, buffer.len());
    }

    #[test]
    fn test_boolean_builder_increases_buffer_len() {
        // 00000010 01001000
        let buf = Buffer::try_from(&[72_u8, 2_u8] as &[u8]).expect("Failed to create buffer from slice!");
        let mut builder = BooleanBufferBuilder::with_default_buffer_manager(8).expect("Failed to allocate boolean buffer builder!");

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
