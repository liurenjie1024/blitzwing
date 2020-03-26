#![allow(dead_code)]
#[macro_use]
extern crate failure;
extern crate arrow;
extern crate derive_new;
extern crate failure_derive;
extern crate num;
extern crate parquet;
extern crate jni;
#[macro_use]
extern crate lazy_static;

#[macro_use]
pub mod error;
pub(crate) mod parquet_adapter;
pub(crate) mod proto;
pub(crate) mod types;
pub(crate) mod util;
