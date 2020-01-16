use std::io::{Read, Seek};

pub trait FsInputStream: Read + Seek {}
