pub(crate) mod buffer;
pub(crate) mod builder;
pub(crate) mod manager;
pub(crate) mod ops;

pub(crate) use buffer::{Buffer, BufferData};
pub(crate) use builder::{BooleanBufferBuilder, BufferBuilder, BufferBuilderTrait};
pub(crate) use manager::BufferManager;

use arrow::datatypes::*;

pub type Int8BufferBuilder = BufferBuilder<Int8Type>;
pub type Int16BufferBuilder = BufferBuilder<Int16Type>;
pub type Int32BufferBuilder = BufferBuilder<Int32Type>;
pub type Int64BufferBuilder = BufferBuilder<Int64Type>;
pub type UInt8BufferBuilder = BufferBuilder<UInt8Type>;
pub type UInt16BufferBuilder = BufferBuilder<UInt16Type>;
pub type UInt32BufferBuilder = BufferBuilder<UInt32Type>;
pub type UInt64BufferBuilder = BufferBuilder<UInt64Type>;
pub type Float32BufferBuilder = BufferBuilder<Float32Type>;
pub type Float64BufferBuilder = BufferBuilder<Float64Type>;

pub type TimestampSecondBufferBuilder = BufferBuilder<TimestampSecondType>;
pub type TimestampMillisecondBufferBuilder = BufferBuilder<TimestampMillisecondType>;
pub type TimestampMicrosecondBufferBuilder = BufferBuilder<TimestampMicrosecondType>;
pub type TimestampNanosecondBufferBuilder = BufferBuilder<TimestampNanosecondType>;
pub type Date32BufferBuilder = BufferBuilder<Date32Type>;
pub type Date64BufferBuilder = BufferBuilder<Date64Type>;
pub type Time32SecondBufferBuilder = BufferBuilder<Time32SecondType>;
pub type Time32MillisecondBufferBuilder = BufferBuilder<Time32MillisecondType>;
pub type Time64MicrosecondBufferBuilder = BufferBuilder<Time64MicrosecondType>;
pub type Time64NanosecondBufferBuilder = BufferBuilder<Time64NanosecondType>;
pub type IntervalYearMonthBufferBuilder = BufferBuilder<IntervalYearMonthType>;
pub type IntervalDayTimeBufferBuilder = BufferBuilder<IntervalDayTimeType>;
pub type DurationSecondBufferBuilder = BufferBuilder<DurationSecondType>;
pub type DurationMillisecondBufferBuilder = BufferBuilder<DurationMillisecondType>;
pub type DurationMicrosecondBufferBuilder = BufferBuilder<DurationMicrosecondType>;
pub type DurationNanosecondBufferBuilder = BufferBuilder<DurationNanosecondType>;
