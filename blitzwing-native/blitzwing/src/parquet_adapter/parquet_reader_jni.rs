use jni::sys::{jlong, jbyteArray};
use jni::JNIEnv;
use jni::objects::JClass;
use crate::util::jni_util::{deserialize_from_byte_array, into_raw, from_raw, process_result};
use crate::parquet_adapter::parquet_reader::create_parquet_reader;
use crate::error::Result;
use crate::proto::parquet::{ParquetReaderProto, RowGroupProto};
use crate::parquet_adapter::parquet_reader::ParquetReader;
use crate::util::jni_util::JniWrapper;

#[no_mangle]
pub extern "system" fn Java_com_ebay_hadoop_blitzwing_arrow_adaptor_parquet_JniWrapper_newInstance
 (env: JNIEnv, _klass: JClass , parquet_reader_proto: jbyteArray) -> jlong {
   let reader = deserialize_from_byte_array::<ParquetReaderProto>(env.clone(), parquet_reader_proto)
    .and_then(create_parquet_reader)
    .map(|reader| into_raw(Box::new(reader)));

    process_result(env, reader)
}

#[no_mangle]
pub extern "system" fn Java_com_ebay_hadoop_blitzwing_arrow_adaptor_parquet_JniWrapper_setRowGroupData<'a>
 (env: JNIEnv<'a>, _klass: JClass, parquet_reader_id: jlong, row_group_proto: jbyteArray) {
   let set_data = |reader: &mut ParquetReader| -> Result<()> { 
     let row_group = deserialize_from_byte_array::<RowGroupProto>(env.clone(), row_group_proto)?;
     reader.set_data(row_group)
   };

  let ret = JniWrapper::<'a, ParquetReader>::new(env.clone(), parquet_reader_id)
   .map(move |mut wrapper| wrapper.with_inner_mut(set_data, || ()));

   process_result(env, ret)
 }

#[no_mangle]
pub extern "system" fn Java_com_ebay_hadoop_blitzwing_arrow_adaptor_parquet_JniWrapper_next
 (env: JNIEnv, _klass: JClass, parquet_reader_id: jlong) -> jbyteArray {
   let call_next = |reader: &mut ParquetReader| -> Result<jbyteArray> {
     
   }
 }

fn Java_com_ebay_hadoop_blitzwing_arrow_adaptor_parquet_JniWrapper_freeBuffer
 (env: JNIEnv, klass: JClass, parquet_reader_id: jlong, buffer_id: jlong) {
   unimplemented!()
 }

fn Java_com_ebay_hadoop_blitzwing_arrow_adaptor_parquet_JniWrapper_close
 (env: JNIEnv, klass: JClass, parquet_reader_id: jlong) {
   unimplemented!()
}