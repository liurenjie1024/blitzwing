use crate::error::{
  BlitzwingError,
  BlitzwingErrorKind::{JniError, NullPointerError, ProtobufError},
  Result,
};
use failure::ResultExt;
use jni::{
  sys::{jbyteArray, jlong},
  JNIEnv,
};
use protobuf::{parse_from_bytes, Message};
use std::{mem::transmute, ptr::NonNull};

pub(crate) fn throw_exception(env: JNIEnv, e: BlitzwingError) -> () {
  match env.throw_new(
    "com/ebay/hadoop/blitzwing/exception/BlitzwingException",
    format!("Failed to build executor: {:?}", e),
  ) {
    Err(e) => eprintln!("Failed to throw exception: {}", e),
    _ => (),
  }
}

pub(crate) fn deserialize<M: Message>(env: JNIEnv, bytes: jbyteArray) -> Result<M> {
  let bytes = env.convert_byte_array(bytes).context(JniError)?;
  Ok(parse_from_bytes(&bytes).context(ProtobufError)?)
}

pub(crate) fn serialize<M: Message>(env: JNIEnv, message: M) -> Result<jbyteArray> {
  let mut buffer = Vec::with_capacity(message.compute_size() as usize);
  message.write_to_vec(&mut buffer).context(ProtobufError)?;

  Ok(env.byte_array_from_slice(&buffer).context(JniError)?)
}

pub(crate) struct JniWrapper<'a, T> {
  env: JNIEnv<'a>,
  inner: NonNull<T>,
}

impl<'a, T> JniWrapper<'a, T> {
  pub(crate) fn new(env: JNIEnv<'a>, address: jlong) -> Result<Self> {
    unsafe {
      if let Some(inner) = NonNull::new(transmute(address)) {
        Ok(Self { env, inner })
      } else {
        Err(NullPointerError)?
      }
    }
  }

  pub(crate) fn inner(&self) -> &T {
    unsafe { self.inner.as_ref() }
  }

  pub(crate) fn inner_mut(&mut self) -> &mut T {
    unsafe { self.inner.as_mut() }
  }

  pub(crate) fn into_box(self) -> Box<T> {
    unsafe { Box::from_raw(self.inner.as_ptr()) }
  }
}
