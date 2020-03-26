#![allow(dead_code)]
extern crate hdfs_rs;
extern crate log4rs;
#[macro_use]
extern crate log;
extern crate failure;

use failure::{Fail, ResultExt};
use hdfs_rs::{
  config::Configuration,
  error::{HdfsLibErrorKind::IoError, Result},
  fs::make_file_system,
};
use std::{fs::File, io::copy, sync::Arc};

fn main() {
  let path = "/Users/renliu/Workspace/blitzwing/blitzwing-native/hdfs-tools/log4rs/hdfs.yaml";
  //    let path = "/root/liu/hdfs-rs/hdfs-tools/log4rs/hdfs.yaml";
  log4rs::init_file(path, Default::default()).expect("Log4rs initialization failed!");

  if let Err(e) = do_main() {
    error!("error happened: {:?}, {:?}", e, e.backtrace());
  }
}

fn do_main() -> Result<()> {
  // list_file_status()
  read_file_content()
}

fn list_file_status() -> Result<()> {
  let path = "hdfs://hadoop-docker-build-3648195.lvs02.dev.ebayc3.com:9000";
  let config = Arc::new(Configuration::new());
  let fs = make_file_system(&path, config)?;

  let file_path = "/user";
  let file_status = fs.get_file_status(file_path)?;
  println!("File status of {} is {:?}", file_path, file_status);

  let file_path = "/user/root/input/log4j.properties";
  let file_status = fs.get_file_status(file_path)?;
  println!("File status of {} is {:?}", file_path, file_status);

  Ok(())
}

fn read_file_content() -> Result<()> {
  let path = "hdfs://hadoop-docker-build-3648195.lvs02.dev.ebayc3.com:9000";
  let config = Arc::new(Configuration::new());
  let fs = make_file_system(&path, config)?;

  let file_path = "/user/root/input/log4j.properties";
  let mut input = fs.open(file_path)?;
  let mut output = File::create("/tmp/out").context(IoError)?;
  copy(&mut input, &mut output).context(IoError)?;
  Ok(())
}
