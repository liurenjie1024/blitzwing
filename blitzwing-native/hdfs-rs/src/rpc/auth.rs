pub struct AuthMethod {
  _code: i8,
  _mechanism_name: &'static str,
}

pub const AUTH_METHOD_SIMPLE: AuthMethod = AuthMethod { _code: 80i8, _mechanism_name: "" };

//pub const AUTH_METHOD_KERBEROS: AuthMethod = AuthMethod {
//    code: 81i8,
//    mechanism_name: "GSSAPI",
//};
//pub const AUTH_METHOD_DIGEST: AuthMethod = AuthMethod {
//    code: 82i8,
//    mechanism_name: "DIGEST-MD5",
//};
//pub const AUTH_METHOD_TOKEN: AuthMethod = AuthMethod {
//    code: 82i8,
//    mechanism_name: "DIGEST-MD5",
//};
//pub const AUTH_METHOD_PLAIN: AuthMethod = AuthMethod {
//    code: 83i8,
//    mechanism_name: "PLAIN",
//};

pub struct AuthProtocol {
  call_id: i32,
}

pub const AUTH_PROTOCOL_NONE: AuthProtocol = AuthProtocol { call_id: 0 };
//pub const AUTH_PROTOCOL_SASL: AuthProtocol = AuthProtocol { call_id: -33 };

impl AuthProtocol {
  pub fn call_id(&self) -> i32 {
    self.call_id
  }
}
