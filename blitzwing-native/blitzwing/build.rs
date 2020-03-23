extern crate protoc_rust;

use protoc_rust::Customize;
use std::path::Path;

fn main() {
  let proto_dir_path = Path::new("../../proto")
    .canonicalize()
    .expect("Unable to canonicalize proto dir");
  
  let proto_dir = proto_dir_path
    .to_str()
    .expect("Unable to convert proto dir path to string!");

  let files: Vec<String> = vec![
    "expr.proto",
    "parquet.proto",
    "plan.proto",
    "record_batch.proto",
    "types.proto",
  ].iter()
  .map(|f| proto_dir_path.join(f))
  .map(|p| p.to_str().expect(format!("Unable to convert {:?} to string!", p).as_str()).to_string())
  .collect();

  
  protoc_rust::run(protoc_rust::Args {
    out_dir: "src/proto",
    input: files.iter().map(AsRef::as_ref).collect::<Vec<&str>>().as_slice(),
    includes: &[proto_dir],
    customize: Customize::default(),
  })
  .expect("protoc");
}
