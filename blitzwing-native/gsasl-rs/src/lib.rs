#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]

#[macro_use]
extern crate failure;
extern crate failure_derive;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate derive_new;

pub mod client;
pub mod error;
pub mod bindings;
pub(crate) mod utils;