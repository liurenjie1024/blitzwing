use crate::error::HdfsLibErrorKind::ProtobufError;
use crate::error::{HdfsLibErrorKind, Result};
use failure::ResultExt;
use protobuf::{CodedInputStream, Message};
use std::io::Write;
use std::ops::Deref;

pub trait RpcMessageSerialize {
    fn serialize<W: Write>(&self, buf: &mut W) -> Result<()>;
    fn get_serialized_len(&self) -> Result<usize>;
}

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

pub struct Messages<'a, I, R>
where
    I: IntoIterator<Item = R> + Clone,
    R: Deref<Target = &'a dyn Message>,
{
    messages: I,
}

impl<'a, I, R> RpcMessageSerialize for Messages<'a, I, R>
where
    I: IntoIterator<Item = R> + Clone,
    R: Deref<Target = &'a dyn Message>,
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

impl<'a, I, R> Messages<'a, I, R>
where
    I: IntoIterator<Item = R> + Clone,
    R: Deref<Target = &'a dyn Message>,
{
    pub fn new(messages: I) -> Self {
        Self { messages }
    }
}

pub fn deserialize<M: Message>(input: &mut CodedInputStream) -> Result<M> {
    Ok(input.read_message().context(ProtobufError)?)
}

fn size_of_varint32(mut value: u32) -> u32 {
    let mut i = 0;
    while (value & !0x7F) > 0 {
        value >>= 7;
        i += 1;
    }
    i + 1
}
