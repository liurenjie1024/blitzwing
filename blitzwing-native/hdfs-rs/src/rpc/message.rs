use crate::error::{HdfsLibErrorKind, Result};
use failure::ResultExt;
use protobuf::{Message, CodedInputStream};
use std::io::Write;
use crate::error::HdfsLibErrorKind::ProtobufError;
use std::ops::Deref;

pub trait RpcMessageSerialize {
    fn serialize<W: Write>(&self, buf: &mut W) -> Result<()>;
    fn get_serialized_len(&self) -> Result<usize>;
}

//pub trait RpcMessageDeserialize
//where Self: Sized
//{
//    fn deserialize<R: Read>(buf: &mut R) -> Result<Self>;
//}

impl RpcMessageSerialize for &dyn Message {
    fn serialize<W: Write>(&self, buf: &mut W) -> Result<()> {
        self.write_length_delimited_to_writer(buf)
            .context(HdfsLibErrorKind::ProtobufError)?;
        Ok(())
    }
    
   
    fn get_serialized_len(&self) -> Result<usize> {
        let size = self.compute_size();
        Ok((size_of_varint32(size) + size) as usize)
    }
}

//impl<T: Message> RpcMessageDeserialize for T {
//    fn deserialize<R: Read>(buf: &mut R) -> Result<T> {
//        let mut input_stream = CodedInputStream::new(buf);
//        Ok(parse_length_delimited_from::<T>(&mut input_stream)
//            .context(HdfsLibErrorKind::ProtobufError)?)
//    }
//}

pub struct Messages<'a, I, R>
    where I: IntoIterator<Item = R> + Clone,
          R: Deref<Target = &'a dyn Message>
{
    messages: I
}

impl<'a, I, R> RpcMessageSerialize for Messages<'a, I, R>
    where I: IntoIterator<Item = R> + Clone,
          R: Deref<Target = &'a dyn Message>
{
    fn serialize<W: Write>(&self, buf: &mut W) -> Result<()> {
        for m in self.messages.clone().into_iter() {
            m.serialize(buf)?;
        }
        
        Ok(())
    }
    
    fn get_serialized_len(&self) -> Result<usize> {
        let mut sum = 0;
        for m in self.messages.clone().into_iter() {
            sum += m.get_serialized_len()?;
        }
        Ok(sum)
    }
}

//impl<'a> RpcMessageDeserialize for Messages<'a> {
//    fn deserialize<R: Read>(&mut self, buf: &mut R) -> Result<()> {
//        for m in  &mut self.messages {
//            m.deserialize(buf)?;
//        }
//
//        Ok(())
//    }
//}

impl<'a, I, R> Messages<'a, I, R>
    where I: IntoIterator<Item = R> + Clone,
          R: Deref<Target = &'a dyn Message>
{
    pub fn new(messages: I) -> Self {
        Self {
            messages
        }
    }
}

pub fn deserialize<M: Message>(input: &mut CodedInputStream) -> Result<M> {
    Ok(input.read_message().context(ProtobufError)?)
}
//
//
//pub struct RpcMessageWithHeader<H: Message, B: Message> {
//    header: H,
//    body: B,
//}
//
//impl<H, B> RpcMessageWithHeader<H, B>
//    where
//        H: Message,
//        B: Message
//{
//    pub fn new(header: H, body: B) -> Self {
//        Self {
//            header,
//            body
//        }
//    }
//}
//
//impl<H, B> RpcMessage for RpcMessageWithHeader<H, B>
//where
//    H: Message,
//    B: Message,
//{
//    fn serialize<W: Write>(&self, buf: &mut W) -> Result<()> {
//        self.header.as_ref()
//            .write_length_delimited_to_writer(buf)
//            .context(HdfsLibErrorKind::ProtobufError)?;
//        self.body
//            .write_length_delimited_to_writer(buf)
//            .context(HdfsLibErrorKind::ProtobufError)?;
//        Ok(())
//    }
//
//    fn deserialize<R: AsRef<[u8]>>(&mut self, buf: &R) -> Result<()> {
//        self.header =
//        self.body = parse_length_delimited_from(&mut input_stream)
//            .context(HdfsLibErrorKind::ProtobufError)?;
//        Ok(())
//    }
//
//    fn get_serialized_len(&self) -> Result<usize> {
//        let header_size = self.header.compute_size();
//        let body_size = self.body.compute_size();
//        Ok(
//            (size_of_varint32(header_size) + header_size + size_of_varint32(body_size) + body_size)
//                as usize,
//        )
//    }
//}
//
//pub type RpcRequestMessageWrapper<M: Message> = RpcMessageWithHeader<RpcRequestHeaderProto, M>;

fn size_of_varint32(mut value: u32) -> u32 {
    let mut i = 0;
    while (value & !0x7F) > 0 {
        value >>= 7;
        i += 1;
    }
    i + 1
}
