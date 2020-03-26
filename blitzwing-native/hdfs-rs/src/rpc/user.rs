use crate::{
  error::{HdfsLibError, HdfsLibErrorKind::FromOsStringError, Result},
  rpc::auth::AuthMethod,
};
use regex::Regex;
use std::sync::Arc;
use users::get_current_username;

lazy_static! {
  static ref KERBEROS_USER_NAME: Regex =
    Regex::new(r"(?P<short>\w+)(/(?P<host>[\w\-\.]+))?@(?P<domain>[\w\.]+)")
      .expect("Failed to parse regular expression pattern for kerberos username!");
}

#[derive(Debug, PartialEq, Eq, Clone, Getters)]
#[get = "pub"]
struct KerberosName {
  user: String,
  host: Option<String>,
  domain: String,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum UserKind {
  Simple,
  Kerberos,
  Token,
}
#[derive(Debug, PartialEq, Eq, Clone, Getters, CopyGetters)]
pub(crate) struct User {
  #[get = "pub"]
  shortname: String,
  #[get = "pub"]
  fullname: String,
  #[get_copy = "pub"]
  kind: UserKind,
}

pub(crate) struct Credentials {}

pub struct Subject {
  user: User,
  credential: Credentials,
}

pub type SubjectRef = Arc<Subject>;

/// Public interfaces
impl Subject {
  pub fn from_os_user() -> Result<Self> {
    Ok(Subject { user: User::from_os_user()?, credential: Credentials {} })
  }

  pub fn from_ticket_cache<S: AsRef<str>>(username: S) -> Result<Self> {
    Ok(Subject { user: User::from_kerberos_name(username)?, credential: Credentials {} })
  }

  pub fn username(&self) -> &str {
    self.user.shortname.as_str()
  }

  pub fn fullname(&self) -> &str {
    self.user.fullname.as_str()
  }
}

/// Interfaces available only in crate
impl Subject {
  pub(crate) fn match_auth_method(&self, auth_method: AuthMethod) -> bool {
    match (auth_method, &self.user.kind) {
      (AuthMethod::Kerberos, &UserKind::Kerberos) => true,
      _ => false,
    }
  }

  pub(crate) fn is_security_enabled(&self) -> bool {
    match self.user.kind {
      UserKind::Simple => false,
      _ => true,
    }
  }
}

impl User {
  fn from_kerberos_name<S: AsRef<str>>(name: S) -> Result<Self> {
    let name_str = name.as_ref();
    let kerberos_name = KerberosName::from_str(name_str)?;
    Ok(Self {
      shortname: kerberos_name.user().to_string(),
      fullname: kerberos_name.fullname(),
      kind: UserKind::Kerberos,
    })
  }

  fn from_os_user() -> Result<Self> {
    if let Some(username) = get_current_username() {
      username
        .into_string()
        .map_err(|os_string| HdfsLibError::from(FromOsStringError(os_string)))
        .map(|name| User {
          shortname: name.clone(),
          fullname: name.clone(),
          kind: UserKind::Simple,
        })
    } else {
      invalid_state!("{}", "Current user does not exist!");
    }
  }
}

impl KerberosName {
  fn from_str(source: &str) -> Result<Self> {
    if let Some(cap) = KERBEROS_USER_NAME.captures(source) {
      let fullname = cap.get(0).expect("Kerberos name match not found!").as_str();
      if fullname != source {
        // Check that whole string is matched
        invalid_argument!("{} is not a valie kerberos name!", source);
      }

      let shortname =
        cap.name("short").expect("Short name in kerberos not found!").as_str().to_string();
      let host = cap.name("host").map(|m| m.as_str().to_string());
      let domain = cap.name("domain").expect("Domain in kerberos not found!").as_str().to_string();

      Ok(Self { user: shortname, host, domain })
    } else {
      invalid_argument!("{} is not a valid kerberos name!", source);
    }
  }

  fn fullname(&self) -> String {
    match self.host {
      Some(ref h) => format!("{}/{}@{}", self.user, h, self.domain),
      None => format!("{}@{}", self.user, self.domain),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_create_kerberos_user() {
    {
      let source = "a_b@PROD.COM";
      let expected_kerberos_name =
        KerberosName { user: "a_b".to_string(), host: None, domain: "PROD.COM".to_string() };

      let expected_user = User {
        shortname: "a_b".to_string(),
        fullname: "a_b@PROD.COM".to_string(),
        kind: UserKind::Kerberos,
      };

      assert_eq!(
        expected_kerberos_name,
        KerberosName::from_str(source).expect("Failed to parse kerberos name")
      );
      assert_eq!(expected_user, User::from_kerberos_name(source).expect("Failed to create user!"));
    }

    {
      let source = "a_b/test1.stratus.com@PROD.COM";
      let expected_kerberos_name = KerberosName {
        user: "a_b".to_string(),
        host: Some("test1.stratus.com".to_string()),
        domain: "PROD.COM".to_string(),
      };

      let expected_user = User {
        shortname: "a_b".to_string(),
        fullname: "a_b/test1.stratus.com@PROD.COM".to_string(),
        kind: UserKind::Kerberos,
      };

      assert_eq!(
        expected_kerberos_name,
        KerberosName::from_str(source).expect("Failed to parse kerberos name")
      );
      assert_eq!(expected_user, User::from_kerberos_name(source).expect("Failed to create user!"));
    }

    {
      let source = "a-b";
      assert!(KerberosName::from_str(source).is_err());
      assert!(User::from_kerberos_name(source).is_err());
    }

    {
      let source = "ab@PROD-com";
      assert!(KerberosName::from_str(source).is_err());
      assert!(User::from_kerberos_name(source).is_err());
    }

    {
      let source = "ab/hos^@PROD.com";
      assert!(KerberosName::from_str(source).is_err());
      assert!(User::from_kerberos_name(source).is_err());
    }
  }

  // #[test]
  // fn test_os_user() {
  //   let expected_user = User {
  //     shortname: "renliu".to_string(),
  //     fullname: "renliu".to_string(),
  //     kind: UserKind::Simple,
  //   };

  //   assert_eq!(expected_user, User::from_os_user().expect("Failed to create os user!"));
  // }
}
