#[macro_use]
extern crate failure;
extern crate failure_derive;
extern crate parquet;
extern crate arrow;
#[macro_use]
extern crate derive_new;

pub mod array_reader;
pub mod array_reader_builder;
pub mod parquet_reader;
pub mod error;
mod concat_reader;
