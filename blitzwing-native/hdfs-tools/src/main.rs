#![allow(dead_code)]
extern crate hdfs_rs;
extern crate log4rs;
#[macro_use]
extern crate log;
extern crate failure;

use failure::Fail;
use hdfs_rs::{config::Configuration, error::Result, fs::make_file_system};
use std::sync::Arc;

fn main() {
  let path = "/Users/renliu/Workspace/blitzwing/blitzwing-native/hdfs-tools/log4rs/hdfs.yaml";
  //    let path = "/root/liu/hdfs-rs/hdfs-tools/log4rs/hdfs.yaml";
  log4rs::init_file(path, Default::default()).expect("Log4rs initialization failed!");

  if let Err(e) = do_main() {
    error!("error happened: {:?}, {:?}", e, e.backtrace());
  }
}

fn do_main() -> Result<()> {
  unimplemented!()
}

fn list_file_status() -> Result<()> {
  let path = "hdfs://hadoop-docker-build-3648195.lvs02.dev.ebayc3.com:9000";
  let config = Arc::new(Configuration::new());
  let fs = make_file_system(&path, config)?;

  let file_path = "/user";
  let file_status = fs.get_file_status(file_path)?;
  println!("File status of {} is {:?}", file_path, file_status);

  let file_path = "/";
  let file_status = fs.get_file_status(file_path)?;
  println!("File status of {} is {:?}", file_path, file_status);
  Ok(())
}

fn show_block_locations() -> Result<()> {
  //    let config = Arc::new(Configuration::new());
  //    let fs = make_file_system(&path, config)?;
  //
  //    let file_path = "/user/root/input/slaves";
  //    let file_status = fs (file_path)?;
  //    println!("File status of {} is {:?}", file_path, file_status);
  //
  //    let file_path = "/";
  //    let file_status = fs.get_file_status(file_path)?;
  //    println!("File status of {} is {:?}", file_path, file_status);
  //    Ok(())
  unimplemented!()
}
