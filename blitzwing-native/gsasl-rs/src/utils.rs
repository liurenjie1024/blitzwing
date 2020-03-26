use crate::bindings::{Gsasl_rc, Gsasl_rc_GSASL_OK};
use std::mem::transmute;

pub(crate) fn to_raw<T>(address: usize) -> *const T {
  unsafe { transmute(address) }
}

pub(crate) fn to_raw_mut<T>(address: usize) -> *mut T {
  unsafe { transmute(address) }
}

pub(crate) fn from_raw<T>(address: *const T) -> usize {
  unsafe { transmute(address) }
}

pub(crate) fn from_raw_mut<T>(address: *mut T) -> usize {
  unsafe { transmute(address) }
}

pub(crate) fn is_ok(rc: Gsasl_rc) -> bool {
  rc == Gsasl_rc_GSASL_OK
}
