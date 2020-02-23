use std::ffi::c_void;
use crate::bindings::gsasl_free;
use crate::bindings::Gsasl_rc_GSASL_NEEDS_MORE;
use crate::bindings::gsasl_step;
use crate::utils::to_raw_mut;
use crate::bindings::gsasl_client_start;
use crate::utils::to_raw;
use std::ffi::CStr;
use std::ffi::CString;
use crate::utils::is_ok;
use crate::bindings::Gsasl_rc;
use crate::utils::from_raw_mut;
use crate::bindings::Gsasl_rc_GSASL_OK;
use std::ptr::null_mut;
use crate::bindings::gsasl_init;
use crate::bindings::Gsasl;
use crate::bindings::Gsasl_session;
use crate::error::Result;
use crate::error::GsaslErrorInfo;
use crate::error::SaslError;

/// We use `usize` instead of `NonNull`, ptr types here becase we want to skip sync and send check of compiler
type GsaslPtr = usize;
type GsaslSessionPtr = usize;

const MECHANISM_NAME_GSS_API: &'static str = "GSSAPI";

lazy_static! {
  static ref CLIENT_CONTEXT: GsaslPtr = {
    let mut ctx: *mut Gsasl = null_mut();
    let rc = unsafe { gsasl_init(&mut ctx) };

    if !is_ok(rc as Gsasl_rc) {
      let error = GsaslErrorInfo::new(rc);
      panic!("Failed to init gsasl client context: [{:?}]", error);
    }

    assert!(!ctx.is_null());

    from_raw_mut(ctx)
  };

  static ref C_MA_GSS_API: CString = {
    CString::new(MECHANISM_NAME_GSS_API).expect(format!("Failed to c style mechanism name for {}", MECHANISM_NAME_GSS_API).as_str())
  };
}

pub struct GssApiInfo {
}

enum Kind {
  GssApi(GssApiInfo)
}

impl Kind {
  fn mechanism_name(&self) -> &str {
    match self {
      Kind::GssApi(_) => MECHANISM_NAME_GSS_API
    }
  }

  fn mechanism_name_c(&self) -> &CStr {
    match self {
      Kind::GssApi(_) => C_MA_GSS_API.as_c_str()
    }
  }

  fn client_sends_data_first(&self) -> bool {
    match self {
      Kind::GssApi(_) => true
    }
  }
}

pub struct SaslClient {
  gsasl_session: GsaslSessionPtr,
  inner: Kind,
}

fn new_session(kind: &Kind) -> Result<GsaslSessionPtr> {
  unsafe {
    let ctx_ptr = to_raw_mut::<Gsasl>(*CLIENT_CONTEXT); 
    let mut session_ptr = null_mut();
    let rc = gsasl_client_start(ctx_ptr, kind.mechanism_name_c().as_ptr(), &mut session_ptr);

    if !is_ok(rc as Gsasl_rc) {
      return Err(SaslError::from_gsasl_rc(rc));
    }

    assert!(!session_ptr.is_null());

    Ok(from_raw_mut(session_ptr))
  }
}

fn new_sasl_client(kind: Kind) -> Result<SaslClient> {
  let session = new_session(&kind)?;

  Ok(SaslClient {
    gsasl_session: session,
    inner: kind
  })
}

impl SaslClient {
  pub fn use_gss_api(info: GssApiInfo) -> Result<Self> {
    new_sasl_client(Kind::GssApi(info))

    //TODO: Init properties
  }

  pub fn mechanism_name(&self) -> &str {
    self.inner.mechanism_name()
  }

  pub fn sends_data_first(&self) -> bool {
    self.inner.client_sends_data_first()
  }

  pub fn step(&mut self, input: &[i8], output: &mut Vec<i8>) -> Result<bool> {
    unsafe {
      let (mut output_ptr, mut output_len) = (null_mut(), 0u64);
      let rc = gsasl_step(self.gsasl_session_ptr_mut(), input.as_ptr(), input.len() as u64, &mut output_ptr, &mut output_len);

      let gsasl_rc = rc as Gsasl_rc;

      if gsasl_rc != Gsasl_rc_GSASL_OK && gsasl_rc != Gsasl_rc_GSASL_NEEDS_MORE {
        return Err(SaslError::from_gsasl_rc(rc));
      }

      let output_slice = std::slice::from_raw_parts_mut(output_ptr, output_len as usize);

      output.extend_from_slice(output_slice);

      gsasl_free(output_ptr as *mut c_void);

      Ok(gsasl_rc == Gsasl_rc_GSASL_NEEDS_MORE)
    }
  }

  fn gsasl_session_ptr(&self) -> *const Gsasl_session {
    to_raw(self.gsasl_session)
  }

  fn gsasl_session_ptr_mut(&self) -> *mut Gsasl_session {
    to_raw_mut(self.gsasl_session)
  }
}

impl Drop for SaslClient {
fn drop(&mut self) { unimplemented!() }
}