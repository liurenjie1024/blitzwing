#![allow(dead_code)]
#[macro_use]
extern crate failure;
extern crate arrow;
extern crate derive_new;
extern crate failure_derive;
extern crate num;
extern crate parquet;

#[macro_use]
pub mod error;
pub(crate) mod parquet_adapter;
pub(crate) mod proto;
pub(crate) mod types;
pub(crate) mod util;
