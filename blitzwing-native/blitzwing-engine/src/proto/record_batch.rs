// This file is generated by rust-protobuf 2.8.1. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy::all)]

#![cfg_attr(rustfmt, rustfmt_skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unsafe_code)]
#![allow(unused_imports)]
#![allow(unused_results)]
//! Generated file from `record_batch.proto`

use protobuf::Message as Message_imported_for_functions;
use protobuf::ProtobufEnum as ProtobufEnum_imported_for_functions;

/// Generated files are compatible only with the same version
/// of protobuf runtime.
const _PROTOBUF_VERSION_CHECK: () = ::protobuf::VERSION_2_8_2;

#[derive(PartialEq,Clone,Default)]
pub struct JNIValueNode {
    // message fields
    length: ::std::option::Option<i32>,
    null_count: ::std::option::Option<i32>,
    // special fields
    pub unknown_fields: ::protobuf::UnknownFields,
    pub cached_size: ::protobuf::CachedSize,
}

impl<'a> ::std::default::Default for &'a JNIValueNode {
    fn default() -> &'a JNIValueNode {
        <JNIValueNode as ::protobuf::Message>::default_instance()
    }
}

impl JNIValueNode {
    pub fn new() -> JNIValueNode {
        ::std::default::Default::default()
    }

    // required int32 length = 1;


    pub fn get_length(&self) -> i32 {
        self.length.unwrap_or(0)
    }
    pub fn clear_length(&mut self) {
        self.length = ::std::option::Option::None;
    }

    pub fn has_length(&self) -> bool {
        self.length.is_some()
    }

    // Param is passed by value, moved
    pub fn set_length(&mut self, v: i32) {
        self.length = ::std::option::Option::Some(v);
    }

    // required int32 null_count = 2;


    pub fn get_null_count(&self) -> i32 {
        self.null_count.unwrap_or(0)
    }
    pub fn clear_null_count(&mut self) {
        self.null_count = ::std::option::Option::None;
    }

    pub fn has_null_count(&self) -> bool {
        self.null_count.is_some()
    }

    // Param is passed by value, moved
    pub fn set_null_count(&mut self, v: i32) {
        self.null_count = ::std::option::Option::Some(v);
    }
}

impl ::protobuf::Message for JNIValueNode {
    fn is_initialized(&self) -> bool {
        if self.length.is_none() {
            return false;
        }
        if self.null_count.is_none() {
            return false;
        }
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream<'_>) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int32()?;
                    self.length = ::std::option::Option::Some(tmp);
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int32()?;
                    self.null_count = ::std::option::Option::Some(tmp);
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(v) = self.length {
            my_size += ::protobuf::rt::value_size(1, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.null_count {
            my_size += ::protobuf::rt::value_size(2, v, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream<'_>) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.length {
            os.write_int32(1, v)?;
        }
        if let Some(v) = self.null_count {
            os.write_int32(2, v)?;
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &dyn (::std::any::Any) {
        self as &dyn (::std::any::Any)
    }
    fn as_any_mut(&mut self) -> &mut dyn (::std::any::Any) {
        self as &mut dyn (::std::any::Any)
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<dyn (::std::any::Any)> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        Self::descriptor_static()
    }

    fn new() -> JNIValueNode {
        JNIValueNode::new()
    }

    fn descriptor_static() -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt32>(
                    "length",
                    |m: &JNIValueNode| { &m.length },
                    |m: &mut JNIValueNode| { &mut m.length },
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt32>(
                    "null_count",
                    |m: &JNIValueNode| { &m.null_count },
                    |m: &mut JNIValueNode| { &mut m.null_count },
                ));
                ::protobuf::reflect::MessageDescriptor::new::<JNIValueNode>(
                    "JNIValueNode",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }

    fn default_instance() -> &'static JNIValueNode {
        static mut instance: ::protobuf::lazy::Lazy<JNIValueNode> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const JNIValueNode,
        };
        unsafe {
            instance.get(JNIValueNode::new)
        }
    }
}

impl ::protobuf::Clear for JNIValueNode {
    fn clear(&mut self) {
        self.length = ::std::option::Option::None;
        self.null_count = ::std::option::Option::None;
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for JNIValueNode {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for JNIValueNode {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct JNIBufferNode {
    // message fields
    address: ::std::option::Option<i64>,
    length: ::std::option::Option<i32>,
    // special fields
    pub unknown_fields: ::protobuf::UnknownFields,
    pub cached_size: ::protobuf::CachedSize,
}

impl<'a> ::std::default::Default for &'a JNIBufferNode {
    fn default() -> &'a JNIBufferNode {
        <JNIBufferNode as ::protobuf::Message>::default_instance()
    }
}

impl JNIBufferNode {
    pub fn new() -> JNIBufferNode {
        ::std::default::Default::default()
    }

    // required int64 address = 1;


    pub fn get_address(&self) -> i64 {
        self.address.unwrap_or(0)
    }
    pub fn clear_address(&mut self) {
        self.address = ::std::option::Option::None;
    }

    pub fn has_address(&self) -> bool {
        self.address.is_some()
    }

    // Param is passed by value, moved
    pub fn set_address(&mut self, v: i64) {
        self.address = ::std::option::Option::Some(v);
    }

    // required int32 length = 2;


    pub fn get_length(&self) -> i32 {
        self.length.unwrap_or(0)
    }
    pub fn clear_length(&mut self) {
        self.length = ::std::option::Option::None;
    }

    pub fn has_length(&self) -> bool {
        self.length.is_some()
    }

    // Param is passed by value, moved
    pub fn set_length(&mut self, v: i32) {
        self.length = ::std::option::Option::Some(v);
    }
}

impl ::protobuf::Message for JNIBufferNode {
    fn is_initialized(&self) -> bool {
        if self.address.is_none() {
            return false;
        }
        if self.length.is_none() {
            return false;
        }
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream<'_>) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int64()?;
                    self.address = ::std::option::Option::Some(tmp);
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int32()?;
                    self.length = ::std::option::Option::Some(tmp);
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(v) = self.address {
            my_size += ::protobuf::rt::value_size(1, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.length {
            my_size += ::protobuf::rt::value_size(2, v, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream<'_>) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.address {
            os.write_int64(1, v)?;
        }
        if let Some(v) = self.length {
            os.write_int32(2, v)?;
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &dyn (::std::any::Any) {
        self as &dyn (::std::any::Any)
    }
    fn as_any_mut(&mut self) -> &mut dyn (::std::any::Any) {
        self as &mut dyn (::std::any::Any)
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<dyn (::std::any::Any)> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        Self::descriptor_static()
    }

    fn new() -> JNIBufferNode {
        JNIBufferNode::new()
    }

    fn descriptor_static() -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt64>(
                    "address",
                    |m: &JNIBufferNode| { &m.address },
                    |m: &mut JNIBufferNode| { &mut m.address },
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt32>(
                    "length",
                    |m: &JNIBufferNode| { &m.length },
                    |m: &mut JNIBufferNode| { &mut m.length },
                ));
                ::protobuf::reflect::MessageDescriptor::new::<JNIBufferNode>(
                    "JNIBufferNode",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }

    fn default_instance() -> &'static JNIBufferNode {
        static mut instance: ::protobuf::lazy::Lazy<JNIBufferNode> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const JNIBufferNode,
        };
        unsafe {
            instance.get(JNIBufferNode::new)
        }
    }
}

impl ::protobuf::Clear for JNIBufferNode {
    fn clear(&mut self) {
        self.address = ::std::option::Option::None;
        self.length = ::std::option::Option::None;
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for JNIBufferNode {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for JNIBufferNode {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct JNIRecordBatch {
    // message fields
    length: ::std::option::Option<i32>,
    nodes: ::protobuf::RepeatedField<JNIValueNode>,
    buffers: ::protobuf::RepeatedField<JNIBufferNode>,
    // special fields
    pub unknown_fields: ::protobuf::UnknownFields,
    pub cached_size: ::protobuf::CachedSize,
}

impl<'a> ::std::default::Default for &'a JNIRecordBatch {
    fn default() -> &'a JNIRecordBatch {
        <JNIRecordBatch as ::protobuf::Message>::default_instance()
    }
}

impl JNIRecordBatch {
    pub fn new() -> JNIRecordBatch {
        ::std::default::Default::default()
    }

    // required int32 length = 1;


    pub fn get_length(&self) -> i32 {
        self.length.unwrap_or(0)
    }
    pub fn clear_length(&mut self) {
        self.length = ::std::option::Option::None;
    }

    pub fn has_length(&self) -> bool {
        self.length.is_some()
    }

    // Param is passed by value, moved
    pub fn set_length(&mut self, v: i32) {
        self.length = ::std::option::Option::Some(v);
    }

    // repeated .JNIValueNode nodes = 2;


    pub fn get_nodes(&self) -> &[JNIValueNode] {
        &self.nodes
    }
    pub fn clear_nodes(&mut self) {
        self.nodes.clear();
    }

    // Param is passed by value, moved
    pub fn set_nodes(&mut self, v: ::protobuf::RepeatedField<JNIValueNode>) {
        self.nodes = v;
    }

    // Mutable pointer to the field.
    pub fn mut_nodes(&mut self) -> &mut ::protobuf::RepeatedField<JNIValueNode> {
        &mut self.nodes
    }

    // Take field
    pub fn take_nodes(&mut self) -> ::protobuf::RepeatedField<JNIValueNode> {
        ::std::mem::replace(&mut self.nodes, ::protobuf::RepeatedField::new())
    }

    // repeated .JNIBufferNode buffers = 3;


    pub fn get_buffers(&self) -> &[JNIBufferNode] {
        &self.buffers
    }
    pub fn clear_buffers(&mut self) {
        self.buffers.clear();
    }

    // Param is passed by value, moved
    pub fn set_buffers(&mut self, v: ::protobuf::RepeatedField<JNIBufferNode>) {
        self.buffers = v;
    }

    // Mutable pointer to the field.
    pub fn mut_buffers(&mut self) -> &mut ::protobuf::RepeatedField<JNIBufferNode> {
        &mut self.buffers
    }

    // Take field
    pub fn take_buffers(&mut self) -> ::protobuf::RepeatedField<JNIBufferNode> {
        ::std::mem::replace(&mut self.buffers, ::protobuf::RepeatedField::new())
    }
}

impl ::protobuf::Message for JNIRecordBatch {
    fn is_initialized(&self) -> bool {
        if self.length.is_none() {
            return false;
        }
        for v in &self.nodes {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.buffers {
            if !v.is_initialized() {
                return false;
            }
        };
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream<'_>) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int32()?;
                    self.length = ::std::option::Option::Some(tmp);
                },
                2 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.nodes)?;
                },
                3 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.buffers)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(v) = self.length {
            my_size += ::protobuf::rt::value_size(1, v, ::protobuf::wire_format::WireTypeVarint);
        }
        for value in &self.nodes {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in &self.buffers {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream<'_>) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.length {
            os.write_int32(1, v)?;
        }
        for v in &self.nodes {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        for v in &self.buffers {
            os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &dyn (::std::any::Any) {
        self as &dyn (::std::any::Any)
    }
    fn as_any_mut(&mut self) -> &mut dyn (::std::any::Any) {
        self as &mut dyn (::std::any::Any)
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<dyn (::std::any::Any)> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        Self::descriptor_static()
    }

    fn new() -> JNIRecordBatch {
        JNIRecordBatch::new()
    }

    fn descriptor_static() -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt32>(
                    "length",
                    |m: &JNIRecordBatch| { &m.length },
                    |m: &mut JNIRecordBatch| { &mut m.length },
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<JNIValueNode>>(
                    "nodes",
                    |m: &JNIRecordBatch| { &m.nodes },
                    |m: &mut JNIRecordBatch| { &mut m.nodes },
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<JNIBufferNode>>(
                    "buffers",
                    |m: &JNIRecordBatch| { &m.buffers },
                    |m: &mut JNIRecordBatch| { &mut m.buffers },
                ));
                ::protobuf::reflect::MessageDescriptor::new::<JNIRecordBatch>(
                    "JNIRecordBatch",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }

    fn default_instance() -> &'static JNIRecordBatch {
        static mut instance: ::protobuf::lazy::Lazy<JNIRecordBatch> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const JNIRecordBatch,
        };
        unsafe {
            instance.get(JNIRecordBatch::new)
        }
    }
}

impl ::protobuf::Clear for JNIRecordBatch {
    fn clear(&mut self) {
        self.length = ::std::option::Option::None;
        self.nodes.clear();
        self.buffers.clear();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for JNIRecordBatch {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for JNIRecordBatch {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

static file_descriptor_proto_data: &'static [u8] = b"\
    \n\x12record_batch.proto\"E\n\x0cJNIValueNode\x12\x16\n\x06length\x18\
    \x01\x20\x02(\x05R\x06length\x12\x1d\n\nnull_count\x18\x02\x20\x02(\x05R\
    \tnullCount\"A\n\rJNIBufferNode\x12\x18\n\x07address\x18\x01\x20\x02(\
    \x03R\x07address\x12\x16\n\x06length\x18\x02\x20\x02(\x05R\x06length\"w\
    \n\x0eJNIRecordBatch\x12\x16\n\x06length\x18\x01\x20\x02(\x05R\x06length\
    \x12#\n\x05nodes\x18\x02\x20\x03(\x0b2\r.JNIValueNodeR\x05nodes\x12(\n\
    \x07buffers\x18\x03\x20\x03(\x0b2\x0e.JNIBufferNodeR\x07buffersB9\n#com.\
    ebay.hadoop.arrow.executor.planB\x12PlannedRecordBatchJ\xe7\x04\n\x06\
    \x12\x04\0\0\x13\x01\n\x08\n\x01\x0c\x12\x03\0\0\x12\n\x08\n\x01\x08\x12\
    \x03\x02\0:\n\t\n\x02\x08\x01\x12\x03\x02\0:\n\x08\n\x01\x08\x12\x03\x03\
    \03\n\t\n\x02\x08\x08\x12\x03\x03\03\n\n\n\x02\x04\0\x12\x04\x05\0\x08\
    \x01\n\n\n\x03\x04\0\x01\x12\x03\x05\x08\x14\n\x0b\n\x04\x04\0\x02\0\x12\
    \x03\x06\x04\x1e\n\x0c\n\x05\x04\0\x02\0\x04\x12\x03\x06\x04\x0c\n\x0c\n\
    \x05\x04\0\x02\0\x05\x12\x03\x06\r\x12\n\x0c\n\x05\x04\0\x02\0\x01\x12\
    \x03\x06\x13\x19\n\x0c\n\x05\x04\0\x02\0\x03\x12\x03\x06\x1c\x1d\n\x0b\n\
    \x04\x04\0\x02\x01\x12\x03\x07\x04\"\n\x0c\n\x05\x04\0\x02\x01\x04\x12\
    \x03\x07\x04\x0c\n\x0c\n\x05\x04\0\x02\x01\x05\x12\x03\x07\r\x12\n\x0c\n\
    \x05\x04\0\x02\x01\x01\x12\x03\x07\x13\x1d\n\x0c\n\x05\x04\0\x02\x01\x03\
    \x12\x03\x07\x20!\n\n\n\x02\x04\x01\x12\x04\n\0\r\x01\n\n\n\x03\x04\x01\
    \x01\x12\x03\n\x08\x15\n\x0b\n\x04\x04\x01\x02\0\x12\x03\x0b\x04\x1f\n\
    \x0c\n\x05\x04\x01\x02\0\x04\x12\x03\x0b\x04\x0c\n\x0c\n\x05\x04\x01\x02\
    \0\x05\x12\x03\x0b\r\x12\n\x0c\n\x05\x04\x01\x02\0\x01\x12\x03\x0b\x13\
    \x1a\n\x0c\n\x05\x04\x01\x02\0\x03\x12\x03\x0b\x1d\x1e\n\x0b\n\x04\x04\
    \x01\x02\x01\x12\x03\x0c\x04\x1e\n\x0c\n\x05\x04\x01\x02\x01\x04\x12\x03\
    \x0c\x04\x0c\n\x0c\n\x05\x04\x01\x02\x01\x05\x12\x03\x0c\r\x12\n\x0c\n\
    \x05\x04\x01\x02\x01\x01\x12\x03\x0c\x13\x19\n\x0c\n\x05\x04\x01\x02\x01\
    \x03\x12\x03\x0c\x1c\x1d\n\n\n\x02\x04\x02\x12\x04\x0f\0\x13\x01\n\n\n\
    \x03\x04\x02\x01\x12\x03\x0f\x08\x16\n\x0b\n\x04\x04\x02\x02\0\x12\x03\
    \x10\x04\x1e\n\x0c\n\x05\x04\x02\x02\0\x04\x12\x03\x10\x04\x0c\n\x0c\n\
    \x05\x04\x02\x02\0\x05\x12\x03\x10\r\x12\n\x0c\n\x05\x04\x02\x02\0\x01\
    \x12\x03\x10\x13\x19\n\x0c\n\x05\x04\x02\x02\0\x03\x12\x03\x10\x1c\x1d\n\
    \x0b\n\x04\x04\x02\x02\x01\x12\x03\x11\x04$\n\x0c\n\x05\x04\x02\x02\x01\
    \x04\x12\x03\x11\x04\x0c\n\x0c\n\x05\x04\x02\x02\x01\x06\x12\x03\x11\r\
    \x19\n\x0c\n\x05\x04\x02\x02\x01\x01\x12\x03\x11\x1a\x1f\n\x0c\n\x05\x04\
    \x02\x02\x01\x03\x12\x03\x11\"#\n\x0b\n\x04\x04\x02\x02\x02\x12\x03\x12\
    \x04'\n\x0c\n\x05\x04\x02\x02\x02\x04\x12\x03\x12\x04\x0c\n\x0c\n\x05\
    \x04\x02\x02\x02\x06\x12\x03\x12\r\x1a\n\x0c\n\x05\x04\x02\x02\x02\x01\
    \x12\x03\x12\x1b\"\n\x0c\n\x05\x04\x02\x02\x02\x03\x12\x03\x12%&\
";

static mut file_descriptor_proto_lazy: ::protobuf::lazy::Lazy<::protobuf::descriptor::FileDescriptorProto> = ::protobuf::lazy::Lazy {
    lock: ::protobuf::lazy::ONCE_INIT,
    ptr: 0 as *const ::protobuf::descriptor::FileDescriptorProto,
};

fn parse_descriptor_proto() -> ::protobuf::descriptor::FileDescriptorProto {
    ::protobuf::parse_from_bytes(file_descriptor_proto_data).unwrap()
}

pub fn file_descriptor_proto() -> &'static ::protobuf::descriptor::FileDescriptorProto {
    unsafe {
        file_descriptor_proto_lazy.get(|| {
            parse_descriptor_proto()
        })
    }
}
