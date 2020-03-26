extern crate bindgen;

fn main() {
  println!("cargo:rustc-link-lib=gsasl");

  println!("cargo:rerun-if-changed=wrapper.h");

  let bindings = bindgen::Builder::default()
    .header("wrapper.h")
    .whitelist_function("gsasl_.*")
    .whitelist_type("Gsasl_.*")
    .parse_callbacks(Box::new(bindgen::CargoCallbacks))
    .generate()
    .expect("Unable to generate bindings");

  // Write the bindings to the $OUT_DIR/bindings.rs file.
  bindings.write_to_file("src/bindings.rs").expect("Couldn't write bindings!");
}
