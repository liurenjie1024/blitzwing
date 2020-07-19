use blitzwing_rs::util::buffer::Buffer;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::prelude::*;
use std::{convert::TryFrom, vec::Vec};

pub fn to_bitmap_basic(c: &mut Criterion) {
  let values: Vec<u8> = (0..4096).map(|_i| random::<u8>()).collect();

  let buffer = Buffer::try_from(values.as_slice()).expect("Failed to build buffer");

  c.bench_function("To bitmap", |b| {
    b.iter(|| buffer.to_bitmap_basic().expect("Failed to create bitmap buffer!"))
  });
}

pub fn to_bitmap_simd(c: &mut Criterion) {
  let values: Vec<u8> = (0..4096).map(|_i| random::<u8>()).collect();

  let buffer = Buffer::try_from(values.as_slice()).expect("Failed to build buffer");

  c.bench_function("To bitmap simd", |b| {
    b.iter(|| buffer.to_bitmap().expect("Failed to create bitmap buffer!"))
  });
}

criterion_group!(benches, to_bitmap_simd, to_bitmap_basic);
criterion_main!(benches);
