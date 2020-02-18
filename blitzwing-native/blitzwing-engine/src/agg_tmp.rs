use jni::{JNIEnv, objects::JObject, sys::{jlong, jint}};
use std::mem::transmute;
use std::slice::from_raw_parts;
use std::collections::HashMap;
use std::collections::hash_map::Drain;
use std::cell::RefCell;
use jni::objects::JByteBuffer;


thread_local! {
  static AGG_MAP: RefCell<HashMap<i32, i32>> = RefCell::new(HashMap::with_capacity(1024));
  static AGG_ITER: RefCell<Option<Drain<'static, i32, i32>>> = RefCell::new(None);
}

#[no_mangle]
pub extern "system" fn Java_com_ebay_hadoop_arrow_executor_agg_JniWrapper_update (
  _env: JNIEnv, _object: JObject, keys_address: jlong, values_address: jlong, len: jint) {
  
  AGG_MAP.with(|agg_map_cell |{
//    println!("update len: {}", len);
    let mut agg_map = agg_map_cell.borrow_mut();
    unsafe {
      let keys_slice = from_raw_parts(
        transmute::<jlong, *const i32>(keys_address),
        len as usize);
      let values_slice = from_raw_parts(
        transmute::<jlong, *const i32>(values_address),
        len as usize);
      
      for i in 0..(len as usize) {
        if let Some(sum) = agg_map.get_mut(&keys_slice[i]) {
          *sum += values_slice[i];
        } else {
          agg_map.insert(keys_slice[i], values_slice[i]);
        }
      }
    }
  });
}

#[no_mangle]
pub extern "system" fn Java_com_ebay_hadoop_arrow_executor_agg_JniWrapper_getSize(
  _env: JNIEnv, _object: JObject
) -> jint {
  AGG_MAP.with(|agg_map_cell| agg_map_cell.borrow().len() as i32)
}

#[no_mangle]
pub extern "system" fn Java_com_ebay_hadoop_arrow_executor_agg_JniWrapper_toIterator(
  _env: JNIEnv, _object: JObject
) {
  AGG_MAP.with(|agg_map_cell| {
    AGG_ITER.with(|agg_iter_cell| {
      agg_iter_cell.replace(Some(unsafe {
        transmute(agg_map_cell.borrow_mut().drain())
      }))
    })
  });
}

#[no_mangle]
pub extern "system" fn Java_com_ebay_hadoop_arrow_executor_agg_JniWrapper_next(
  env: JNIEnv, _object: JObject, buf: JByteBuffer) {
  AGG_ITER.with(|agg_iter_cell| {
    let addr = env.get_direct_buffer_address(buf).unwrap();
    let data = agg_iter_cell.borrow_mut().as_mut().unwrap().next().unwrap();
    addr[0..4].copy_from_slice(&data.0.to_be_bytes());
    addr[4..8].copy_from_slice(&data.1.to_be_bytes());
  })
}

#[no_mangle]
pub extern "system" fn Java_com_ebay_hadoop_arrow_executor_agg_JniWrapper_clean(
  _env: JNIEnv, _object: JObject) {
  AGG_MAP.with(|agg_map_cell| {
    agg_map_cell.borrow_mut().clear()
  });
  AGG_ITER.with(|agg_iter_cell| {
    agg_iter_cell.replace(None)
  });
}
