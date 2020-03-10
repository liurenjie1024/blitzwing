use crate::rpc::auth::AuthMethod;
use std::sync::Arc;

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

pub(crate) struct Credentials {
}

#[derive(Getters)]
pub(crate) struct Subject {
  #[get = "pub"]
  user: User,
  credential: Credentials
}

pub(crate) type SubjectRef = Arc<Subject>;

impl Subject {
  pub fn match_auth_method(&self, auth_method: AuthMethod) -> bool {
    match (auth_method, &self.user.kind) {
      (AuthMethod::Kerberos, &UserKind::Kerberos) => true,
      _ => false,
    }
  }
}