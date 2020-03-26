use crate::{
  error::Result,
  rpc::auth::AuthMethod::{Digest, Kerberos, Plain, Simple, Token},
};
use std::ops::Deref;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum AuthMethod {
  Simple,
  Kerberos,
  Digest,
  Token,
  Plain,
}

pub(crate) struct AuthMethodValue {
  code: i8,
  mechanism_name: &'static str,
}

impl AuthMethodValue {
  pub(crate) fn code(&self) -> i8 {
    self.code
  }

  pub(crate) fn mechanism_name(&self) -> &str {
    self.mechanism_name
  }
}

const AUTH_METHOD_SIMPLE: AuthMethodValue = AuthMethodValue { code: 80i8, mechanism_name: "" };

const AUTH_METHOD_KERBEROS: AuthMethodValue =
  AuthMethodValue { code: 81i8, mechanism_name: "GSSAPI" };
const AUTH_METHOD_DIGEST: AuthMethodValue =
  AuthMethodValue { code: 82i8, mechanism_name: "DIGEST-MD5" };
const AUTH_METHOD_TOKEN: AuthMethodValue =
  AuthMethodValue { code: 82i8, mechanism_name: "DIGEST-MD5" };
const AUTH_METHOD_PLAIN: AuthMethodValue = AuthMethodValue { code: 83i8, mechanism_name: "PLAIN" };

impl Deref for AuthMethod {
  type Target = AuthMethodValue;
  fn deref(&self) -> &Self::Target {
    match self {
      AuthMethod::Simple => &AUTH_METHOD_SIMPLE,
      AuthMethod::Kerberos => &AUTH_METHOD_KERBEROS,
      AuthMethod::Digest => &AUTH_METHOD_DIGEST,
      AuthMethod::Token => &AUTH_METHOD_DIGEST,
      AuthMethod::Plain => &AUTH_METHOD_PLAIN,
    }
  }
}

impl AuthMethod {
  pub(crate) fn value_of<T: AsRef<str>>(t: T) -> Result<Self> {
    match t.as_ref() {
      "SIMPLE" => Ok(Simple),
      "KERBEROS" => Ok(Kerberos),
      "DIGEST" => Ok(Digest),
      "TOKEN" => Ok(Token),
      "PLAIN" => Ok(Plain),
      s => invalid_argument!("Unrecognized auth method name: {}", s),
    }
  }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum AuthProtocol {
  None,
  Sasl,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct AuthProtocolValue {
  call_id: i32,
}

const AUTH_PROTOCOL_NONE: AuthProtocolValue = AuthProtocolValue { call_id: 0 };
const AUTH_PROTOCOL_SASL: AuthProtocolValue = AuthProtocolValue { call_id: -33 };

impl Deref for AuthProtocol {
  type Target = AuthProtocolValue;

  fn deref(&self) -> &Self::Target {
    match self {
      AuthProtocol::None => &AUTH_PROTOCOL_NONE,
      AuthProtocol::Sasl => &AUTH_PROTOCOL_SASL,
    }
  }
}

impl AuthProtocol {
  pub(crate) fn call_id(&self) -> i32 {
    self.call_id
  }

  pub(crate) fn is_secure(&self) -> bool {
    match self {
      AuthProtocol::None => false,
      AuthProtocol::Sasl => true,
    }
  }
}

#[cfg(test)]
mod tests {
  use super::{AuthProtocol::*, *};

  #[test]
  fn test_auth_method() {
    assert_eq!(80i8, Simple.code());
    assert_eq!("", Simple.mechanism_name());

    assert_eq!(81i8, Kerberos.code());
    assert_eq!("GSSAPI", Kerberos.mechanism_name());

    assert_eq!(82i8, Digest.code());
    assert_eq!("DIGEST-MD5", Digest.mechanism_name());

    assert_eq!(82i8, Token.code());
    assert_eq!("DIGEST-MD5", Token.mechanism_name());

    assert_eq!(83i8, Plain.code());
    assert_eq!("PLAIN", Plain.mechanism_name());
  }

  #[test]
  fn test_auth_protocol() {
    assert_eq!(0, AuthProtocol::None.call_id());
    assert_eq!(-33, Sasl.call_id());
  }
}
