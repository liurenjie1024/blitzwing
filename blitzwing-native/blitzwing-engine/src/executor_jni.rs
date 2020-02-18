use crate::error::Result;
use crate::executor::{ArrowExecutor, ArrowExecutorBuilder, JniContext};
use jni::objects::JClass;
use jni::sys::jbyteArray;
use jni::sys::jlong;
use jni::JNIEnv;
use std::mem::transmute;
use std::slice::from_raw_parts;

#[no_mangle]
pub extern "system" fn Java_com_ebay_hadoop_arrow_executor_JniExecutor_buildExecutor(
    env: JNIEnv,
    _class: JClass,
    serialized_plan_address: jlong,
    serialized_plan_len: jlong,
) -> jlong {
    let plan_buf = unsafe {
        from_raw_parts(
            transmute::<jlong, *const u8>(serialized_plan_address),
            transmute(serialized_plan_len),
        )
    };

    let build_result = ArrowExecutorBuilder::new(plan_buf, env.clone())
        .and_then(|builder| builder.build())
        .map(|e| Box::into_raw(e) as jlong);

    process_result(env, build_result)
}

#[no_mangle]
pub extern "system" fn Java_com_ebay_hadoop_arrow_executor_JniExecutor_open<'a>(
    env: JNIEnv<'a>,
    class: JClass<'a>,
    executor_id: jlong,
) {
    let jni_context = JniContext::new(env.clone(), class);
    let mut executor = ArrowExecutor::from_raw(executor_id);
    let open_result = executor.open(jni_context);
    Box::into_raw(executor);
    process_result(env, open_result)
}

#[no_mangle]
pub extern "system" fn Java_com_ebay_hadoop_arrow_executor_JniExecutor_next(
    env: JNIEnv,
    class: JClass,
    executor_id: jlong,
) -> jbyteArray {
    let jni_context = JniContext::new(env.clone(), class);
    let mut executor = ArrowExecutor::from_raw(executor_id);
    let result = executor.next(jni_context);
    Box::into_raw(executor);
    process_result_with(env, result, std::ptr::null_mut)
}

#[no_mangle]
pub extern "system" fn Java_com_ebay_hadoop_arrow_executor_JniExecutor_close<'a>(
    env: JNIEnv<'a>,
    class: JClass<'a>,
    executor_id: jlong,
) {
    let jni_context = JniContext::new(env.clone(), class);
    let mut executor = ArrowExecutor::from_raw(executor_id);
    let result = executor.close(jni_context);
    process_result(env, result)
}

#[no_mangle]
pub extern "system" fn Java_com_ebay_hadoop_arrow_executor_JniExecutor_freeBuffer(
    env: JNIEnv,
    _class: JClass,
    executor_id: jlong,
    memory_address: jlong,
    _length: jlong,
) {
    let mut executor = ArrowExecutor::from_raw(executor_id);
    let result = executor.free_buffer(memory_address);
    Box::into_raw(executor);
    process_result(env, result)
}

fn process_result<T>(env: JNIEnv, result: Result<T>) -> T
where
    T: Default,
{
    process_result_with(env, result, T::default)
}

fn process_result_with<T, F>(env: JNIEnv, result: Result<T>, f: F) -> T
where
    F: FnOnce() -> T,
{
    match result {
        Ok(t) => t,
        Err(e) => {
            match env.throw(format!("Failed to build executor: {:?}", e)) {
                Err(e) => eprintln!("Failed to throw exception: {}", e),
                _ => {}
            }
            f()
        }
    }
}
