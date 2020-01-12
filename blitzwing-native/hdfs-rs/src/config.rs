use crate::error::Result;
use crate::error::{HdfsLibError};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

type ConfigKey = String;

struct ConfigData {
    value: String,
    _is_final: bool,
}

pub struct Configuration {
    data: HashMap<ConfigKey, ConfigData>,
}

impl Configuration {
    pub fn new() -> Self {
        Self {
            data: HashMap::new()
        }
    }
    
    pub fn get<T>(&self, key: &str) -> Result<Option<T>>
    where
        T: FromStr,
        T::Err: Into<HdfsLibError>,
    {
        self.data
            .get(key)
            .map(|v| T::from_str(&v.value).map_err(|e| e.into()))
            .transpose()
    }
}

pub type ConfigRef = Arc<Configuration>;


