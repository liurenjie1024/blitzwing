//extern crate protobuf;
//extern crate jni;
//
//use std::env;
//use std::fs;
//use jni::{InitArgsBuilder, JNIVersion, JavaVM, Executor};
//use arrow_executor_rs::executor::{ArrowExecutorBuilder, JniContext};
//use std::sync::Arc;
//
//pub fn main() {
//    let args: Vec<String> = env::args().collect();
//
//    let serialized_plan = fs::read(&args[1])
//        .expect(&format!("Failed to read serialized plan from {}", &args[0]));
//
//    // Build the VM properties
//    let jvm_args = InitArgsBuilder::new()
//        .version(JNIVersion::V8)
//        .option("-Xcheck:jni")
//        .build()
//        .unwrap();
//
//    let jvm = Arc::new(JavaVM::new(jvm_args).unwrap());
//
//    let exec = Executor::new(jvm);
//
//    exec.with_attached(|env| {
//        let mut arrow_executor = ArrowExecutorBuilder::new(&serialized_plan, env.clone())
//            .unwrap()
//            .build().unwrap();
//        let class = env.find_class("java/lang/String").unwrap();
//
//        let jni_context = || JniContext::new(env.clone(), class.clone());
//
//        arrow_executor.open(jni_context()).unwrap();
//
//        loop {
//            let ret = arrow_executor.next(jni_context()).unwrap();
//            if ret.is_null() {
//                break;
//            }
//        }
//
//        arrow_executor.close(jni_context()).unwrap();
//        Ok(())
//    }).unwrap();
//}

fn main() {
    println!("Hello world!");
}