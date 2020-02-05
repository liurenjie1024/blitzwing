use protobuf::{Message, parse_from_bytes};
use crate::error::{Result, HdfsLibErrorKind};
use std::io::Read;
use failure::ResultExt;

pub(crate) trait ProtobufTranslate<M: Message>: Sized {
    fn try_read_from(proto: &M) -> Result<Self>;
    fn try_write_to(&self) -> Result<M>;
}

pub fn parse_varint64<R: Read>(input: &mut R) -> Result<u64> {
    let mut r: u64 = 0;
    let mut i = 0;
    
    let mut buf = [1u8; 1];
    loop {
        check_protocol_content!(i <= 10, "{}", "varint should be contain at most 9 bytes!");
        input.read_exact(buf.as_mut())
            .context(HdfsLibErrorKind::IoError)?;
        let b = buf[0];
        // TODO: may overflow if i == 9
        r = r | (((b & 0x7f) as u64) << (i * 7));
        i += 1;
        if b < 0x80 {
            return Ok(r);
        }
    }
}

/// The method is different from similar methods in `protobuf` crate in that it consumes only
/// necessary bytes, rather stores extra bytes in buffer.
pub fn parse_delimited_message<R: Read, M: Message>(input: &mut R) -> Result<M> {
    let len = parse_varint64(input)? as usize;
    
    let mut buffer = Vec::with_capacity(len);
    buffer.resize(len, 0u8);
    
    input.read_exact(&mut buffer)
        .context(HdfsLibErrorKind::IoError)?;
    
    Ok(parse_from_bytes(&buffer)
        .context(HdfsLibErrorKind::ProtobufError)?)
}