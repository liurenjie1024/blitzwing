use std::net::SocketAddr;
use crate::hdfs::transfer::connection::Connection;
use std::io::Cursor;


#[derive(new, MutGetters, Getters, Setters)]
#[set = "pub"]
pub(super) struct TestConnection {
    #[get_mut = "pub"]
    input: Cursor<Vec<u8>>,
    #[new(default)]
    #[get_mut = "pub"]
    output: Vec<u8>,
    #[get = "pub"]
    address: SocketAddr,
}



impl Connection for TestConnection {
    type In = Cursor<Vec<u8>>;
    type Out = Vec<u8>;
    
    fn input_stream(&mut self) -> &mut Self::In {
        &mut self.input
    }
    
    fn output_stream(&mut self) -> &mut Self::Out {
        &mut self.output
    }
    
    fn local_address(&self) -> &SocketAddr {
        &self.address
    }
    
    fn remote_address(&self) -> &SocketAddr {
        &self.address
    }
}
