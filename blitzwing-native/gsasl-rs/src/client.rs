use crate::bindings::Gsasl_property_GSASL_QOP;
use crate::bindings::Gsasl_property_GSASL_HOSTNAME;
use crate::bindings::Gsasl_property_GSASL_SERVICE;
use crate::bindings::gsasl_property_set;
use crate::bindings::Gsasl_property_GSASL_AUTHID;
use crate::error::SaslErrorKind::NotValidCString;
use crate::bindings::gsasl_finish;
use std::ffi::c_void;
use crate::bindings::gsasl_free;
use crate::bindings::Gsasl_rc_GSASL_NEEDS_MORE;
use crate::bindings::gsasl_step;
use crate::utils::to_raw_mut;
use crate::bindings::gsasl_client_start;
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
use failure::ResultExt;

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

#[derive(new, Debug)]
pub struct GssApiInfo {
  client_principal: String,
  service: String,
  server_host: String
}

impl GssApiInfo {
  fn init_gsasl_properties(&self, gsasl_session: GsaslSessionPtr) -> Result<()> {
    unsafe {
      let gsasl_session = to_raw_mut(gsasl_session);

      let c_client_principal = CString::new(self.client_principal.as_bytes())
        .context(NotValidCString(self.client_principal.clone()))?;
      let c_service = CString::new(self.service.as_bytes())
        .context(NotValidCString(self.service.clone()))?;
      let c_server_host = CString::new(self.server_host.as_bytes())
        .context(NotValidCString(self.server_host.clone()))?;

      gsasl_property_set(gsasl_session, Gsasl_property_GSASL_AUTHID, c_client_principal.as_ptr());
      gsasl_property_set(gsasl_session, Gsasl_property_GSASL_SERVICE, c_service.as_ptr());
      gsasl_property_set(gsasl_session, Gsasl_property_GSASL_HOSTNAME, c_server_host.as_ptr());

      Ok(())
    }
  }
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

  fn init_gsasl_properties(&self, gsasl_session: GsaslSessionPtr) -> Result<()> {
    // TODO: Make this configurable
    unsafe {
      let gsasl_session = to_raw_mut(gsasl_session);
      let c_qop = CString::new("qop-auth").context(NotValidCString("qop-auth".to_string()))?;
      gsasl_property_set(gsasl_session, Gsasl_property_GSASL_QOP, c_qop.as_ptr());
    }

    match self {
      Kind::GssApi(info) => info.init_gsasl_properties(gsasl_session)
    }
  }
}

pub struct SaslClient {
  gsasl_session: GsaslSessionPtr,
  inner: Kind,
  is_complete: bool,
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

  kind.init_gsasl_properties(session)?;

  Ok(SaslClient {
    gsasl_session: session,
    inner: kind,
    is_complete: false,
  })
}

impl SaslClient {
  pub fn use_gss_api(info: GssApiInfo) -> Result<Self> {
    new_sasl_client(Kind::GssApi(info))
  }

  pub fn mechanism_name(&self) -> &str {
    self.inner.mechanism_name()
  }

  pub fn sends_data_first(&self) -> bool {
    self.inner.client_sends_data_first()
  }

  pub fn evaluate(&mut self, input: &[u8]) -> Result<Vec<u8>> {
    unsafe {
      let (mut output_ptr, mut output_len) = (null_mut(), 0u64);
      let rc = gsasl_step(self.gsasl_session_ptr_mut(), input.as_ptr() as *const i8, input.len() as u64, &mut output_ptr, &mut output_len);

      let gsasl_rc = rc as Gsasl_rc;

      if gsasl_rc != Gsasl_rc_GSASL_OK && gsasl_rc != Gsasl_rc_GSASL_NEEDS_MORE {
        return Err(SaslError::from_gsasl_rc(rc));
      }

      let output_slice = std::slice::from_raw_parts_mut(output_ptr as *mut u8, output_len as usize);
      let mut output = Vec::new();

      output.extend_from_slice(output_slice);

      self.is_complete = gsasl_rc == Gsasl_rc_GSASL_NEEDS_MORE;
      gsasl_free(output_ptr as *mut c_void);

      Ok(output)
    }
  }

  pub fn is_complete(&self) -> bool {
    self.is_complete
  }

  fn gsasl_session_ptr_mut(&self) -> *mut Gsasl_session {
    to_raw_mut(self.gsasl_session)
  }
}

impl Drop for SaslClient {
  fn drop(&mut self) {
    unsafe {
      gsasl_finish(self.gsasl_session_ptr_mut())
    }
  }
}