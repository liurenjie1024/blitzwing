use crate::{
  error::Result,
  parquet_adapter::parquet_reader::{create_parquet_reader, ParquetReader},
  proto::{
    parquet::{ParquetReaderProto, RowGroupProto},
    record_batch::JniRecordBatchProto,
  },
  util::jni_util::{deserialize, serialize, throw_exception, JniWrapper},
};
use jni::{
  objects::JClass,
  sys::{jbyteArray, jlong, jint},
  JNIEnv,
};
use std::{
  convert::{identity, TryFrom},
  mem::transmute,
  ptr::null_mut,
};

#[no_mangle]
pub extern "system" fn Java_com_ebay_hadoop_blitzwing_arrow_adaptor_parquet_JniWrapper_newInstance(
  env: JNIEnv,
  _klass: JClass,
  parquet_reader_proto: jbyteArray,
) -> jlong {
  deserialize::<ParquetReaderProto>(env.clone(), parquet_reader_proto)
    .and_then(create_parquet_reader)
    .map(|reader| unsafe { transmute(Box::into_raw(Box::new(reader))) })
    .map_err(|e| throw_exception(env, e))
    .map_or(jlong::default(), identity)
}

#[no_mangle]
pub extern "system" fn Java_com_ebay_hadoop_blitzwing_arrow_adaptor_parquet_JniWrapper_setRowGroupData<
  'a,
>(
  env: JNIEnv<'a>,
  _klass: JClass,
  parquet_reader_id: jlong,
  row_group_proto: jbyteArray,
) {
  let call_inner = || -> Result<()> {
    let row_group = deserialize::<RowGroupProto>(env.clone(), row_group_proto)?;
    JniWrapper::<'a, ParquetReader>::new(env.clone(), parquet_reader_id)
      .and_then(move |mut w| w.inner_mut().set_data(row_group))
  };

  call_inner().map_err(|e| throw_exception(env, e)).map_or_else(identity, identity)
}

#[no_mangle]
pub extern "system" fn Java_com_ebay_hadoop_blitzwing_arrow_adaptor_parquet_JniWrapper_next<'a>(
  env: JNIEnv<'a>,
  _klass: JClass,
  parquet_reader_id: jlong,
) -> jlong {
  let call_inner = || -> Result<jlong> {
    let mut wrapper = JniWrapper::<'a, ParquetReader>::new(env.clone(), parquet_reader_id)?;
    wrapper.inner_mut().next_batch()
      .map(|len| len as jlong)
  };

  // TODO: Remember to free buffer
  call_inner().map_err(|e| throw_exception(env.clone(), e)).map_or(0, identity)
}

#[no_mangle]
pub extern "system" fn Java_com_ebay_hadoop_blitzwing_arrow_adaptor_parquet_JniWrapper_collect<'a>(
  env: JNIEnv<'a>,
  _klass: JClass,
  parquet_reader_id: jlong
) -> jbyteArray {
  let call_inner = || -> Result<jbyteArray> {
    let mut wrapper = JniWrapper::<'a, ParquetReader>::new(env.clone(), parquet_reader_id)?;
    serialize(env.clone(), JniRecordBatchProto::try_from(wrapper.inner_mut().collect()?)?)
  };

  // TODO: Remember to free buffer
  call_inner().map_err(|e| throw_exception(env.clone(), e)).map_or(null_mut(), identity)
}

#[no_mangle]
pub extern "system" fn Java_com_ebay_hadoop_blitzwing_arrow_adaptor_parquet_JniWrapper_freeBuffer<
  'a,
>(
  env: JNIEnv<'a>,
  _klass: JClass,
  parquet_reader_id: jlong,
  buffer_id: jlong,
  len: jint
) {
  let call_inner = || -> Result<()> {
    let mut wrapper = JniWrapper::<'a, ParquetReader>::new(env.clone(), parquet_reader_id)?;
    unsafe { wrapper.inner_mut().free_buffer(transmute(buffer_id), len as usize) }
  };

  call_inner().map_err(|e| throw_exception(env, e)).map_or_else(identity, identity)
}

#[no_mangle]
pub extern "system" fn Java_com_ebay_hadoop_blitzwing_arrow_adaptor_parquet_JniWrapper_close<'a>(
  env: JNIEnv<'a>,
  _klass: JClass,
  parquet_reader_id: jlong,
) {
  let call_inner = || -> Result<()> {
    let mut wrapper = JniWrapper::<'a, ParquetReader>::new(env.clone(), parquet_reader_id)?;
    wrapper.inner_mut().close()?;
    // Convert to Box for destruction
    wrapper.into_box();
    Ok(())
  };

  call_inner().map_err(|e| throw_exception(env, e)).map_or_else(identity, identity)
}
