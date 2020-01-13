use failure::Fail;
use std::env::var;
use std::env::VarError;
use std::process::exit;
use std::str::FromStr;
use tokio::runtime::{Builder, Runtime};

pub const HDFS_RS_RT_CONFIG_CORE_THREAD_SIZE: &'static str = "hdfs_rs_rt_core_thread_size";
pub const HDFS_RS_RT_CONFIG_MAX_THREAD_SIZE: &'static str = "hdfs_rs_rt_max_thread_size";

lazy_static! {
    static ref HDFS_RS_RT: Runtime = { build_runtime() };
}

fn parse_or_else<T: FromStr, F: FnOnce() -> T>(env_key: &str, f: F) -> T {
    match var(env_key) {
        Ok(s) => match T::from_str(&s) {
            Ok(t) => t,
            Err(_e) => {
                warn!("Failed to parse environment value: [{}={}]", env_key, &s);
                f()
            }
        },
        Err(e) => match e {
            VarError::NotPresent => f(),
            VarError::NotUnicode(s) => {
                warn!("Env [{}={:?}] is not unicode value.", env_key, &s);
                f()
            }
        },
    }
}

fn build_runtime() -> Runtime {
    let core_thread_size = parse_or_else(HDFS_RS_RT_CONFIG_CORE_THREAD_SIZE, || 1);
    let max_thread_size = parse_or_else(HDFS_RS_RT_CONFIG_MAX_THREAD_SIZE, || 4);

    let result = Builder::new()
        .core_threads(core_thread_size)
        .max_threads(max_thread_size)
        .enable_all()
        .threaded_scheduler()
        .thread_name("Lib hdfs-rs threads")
        .build();
    match result {
        Ok(rt) => rt,
        Err(e) => {
            error!("Failed to build tokio runtime: {}, {:?}", e, e.backtrace());
            exit(1);
        }
    }
}

pub fn get_runtime() -> &'static Runtime {
    &*HDFS_RS_RT
}
