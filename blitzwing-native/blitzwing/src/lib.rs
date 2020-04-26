#![allow(dead_code)]
#[macro_use]
extern crate failure;
extern crate arrow;
#[macro_use]
extern crate derive_new;
extern crate failure_derive;
extern crate jni;
extern crate num;
extern crate parquet;
#[macro_use]
extern crate log;
extern crate arraydeque;
extern crate log4rs;
// extern crate flatbuffers;

#[macro_use]
pub mod error;
pub(crate) mod parquet_adapter;
pub(crate) mod proto;
pub(crate) mod types;
pub(crate) mod util;
