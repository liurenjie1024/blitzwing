use crate::error::{Result, BlitzwingErrorKind::{JniError, ProtobufError, NullPointerError}};
use jni::JNIEnv;
use jni::sys::{jbyteArray, jlong};
use protobuf::{Message, parse_from_bytes};
use failure::ResultExt;
use std::mem::transmute;
use std::ptr::NonNull;

pub(crate) fn process_result<T>(env: JNIEnv, result: Result<T>) -> T
where
    T: Default,
{
    process_result_with(env, result, T::default)
}

pub(crate) fn process_result_with<T, F>(env: JNIEnv, result: Result<T>, f: F) -> T
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

pub(crate) fn deserialize_from_byte_array<M: Message>(env: JNIEnv, bytes: jbyteArray) -> Result<M> {
  let bytes = env.convert_byte_array(bytes).context(JniError)?;
  Ok(parse_from_bytes(&bytes).context(ProtobufError)?)
}

pub(crate) fn into_raw<T>(t: Box<T>) -> jlong {
  unsafe { transmute(Box::into_raw(t)) }
}

pub(crate) fn from_raw<T>(address: jlong) -> Box<T> {
  unsafe {
    Box::from_raw(transmute::<jlong, *mut T>(address)) 
  }
}

pub(crate) struct JniWrapper<'a, T> {
  env: JNIEnv<'a>,
  inner: NonNull<T>,
}

impl<'a, T> JniWrapper<'a, T> {
  pub(crate) fn new(env: JNIEnv<'a>, address: jlong) -> Result<Self> {
    unsafe {
    if let Some(inner) = NonNull::new(transmute(address)) {
      Ok(Self {
        env,
        inner
      })
    } else {
      Err(NullPointerError)?
    }
    }
  }

  pub(crate) fn with_inner<F, D, R>(&self, f: F, defaults: D) -> R 
    where F: FnOnce(&T) -> Result<R>,
          D: FnOnce() -> R,
  {
    unsafe {
    process_result_with(self.env.clone(), f(self.inner.as_ref()), defaults)
    }
  }

  pub(crate) fn with_inner_mut<F, D, R>(&mut self, f: F, defaults: D) -> R 
    where F: FnOnce(&mut T) -> Result<R>,
          D: FnOnce() -> R,
  {
    unsafe {
    process_result_with(self.env.clone(), f(self.inner.as_mut()), defaults)
    }
  }
}