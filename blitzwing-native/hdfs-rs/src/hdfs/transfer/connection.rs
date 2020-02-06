use std::io::{Read, Write};
use std::net::SocketAddr;

pub trait Connection {
    type In: Read;
    type Out: Write;

    fn input_stream(&mut self) -> &mut Self::In;
    fn output_stream(&mut self) -> &mut Self::Out;

    fn local_address(&self) -> &SocketAddr;
    fn remote_address(&self) -> &SocketAddr;
}
