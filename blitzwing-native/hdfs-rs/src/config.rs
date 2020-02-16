use crate::error::{HdfsLibError, HdfsLibErrorKind, Result};
use failure::Fail;
use std::{collections::HashMap, str::FromStr, sync::Arc};

type ConfigKey = String;

struct ConfigData {
  value: String,
  _is_final: bool,
}

#[derive(Default)]
pub struct Configuration {
  data: HashMap<ConfigKey, ConfigData>,
}

impl Configuration {
  pub fn new() -> Self {
    Self { data: HashMap::new() }
  }

  pub fn get<T>(&self, key: &str) -> Result<Option<T>>
  where
    T: FromStr,
    T::Err: Fail,
  {
    self
      .data
      .get(key)
      .map(|v| {
        T::from_str(&v.value).map_err(|e| {
          HdfsLibError::from(e.context(HdfsLibErrorKind::ConfigError(key.to_string())))
        })
      })
      .transpose()
  }

  pub fn get_or<T>(&self, key: &str, default_value: T) -> Result<T>
  where
    T: FromStr,
    T::Err: Fail,
  {
    self.get(key).map(|r| r.unwrap_or(default_value))
  }
}

pub type ConfigRef = Arc<Configuration>;
