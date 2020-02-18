use crate::aggregation::AggregateFunction;
use crate::error::{ArrowExecutorError, ArrowExecutorErrorKind, Result};
use arrow::array::{Array, PrimitiveArray};
use arrow::datatypes::{ArrowNumericType, ArrowPrimitiveType, DataType};
use arrow::datatypes::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use std::alloc::Layout;
use std::marker::PhantomData;
use std::mem::transmute;
use std::ops::Add;
use std::ptr::copy;

pub struct Sum<T> {
    _phantom: PhantomData<T>,
}

impl<T> AggregateFunction for Sum<T>
where
    T: ArrowNumericType,
    T::Native: Add<Output = T::Native>,
{
    fn result_type(&self) -> DataType {
        T::get_data_type()
    }

    fn data_layout(&self) -> Layout {
        Layout::new::<T::Native>()
    }

    fn init(&self, data: *mut u8) -> Result<()> {
        unsafe {
            let t: *mut T::Native = transmute(data);
            *t = T::default_value();
            Ok(())
        }
    }

    fn add(&self, data: *mut u8, args: &[&dyn Array], row: usize) -> Result<()> {
        unsafe {
            let value_to_add = args
                .first()
                .and_then(|arr| arr.as_any().downcast_ref::<PrimitiveArray<T>>())
                .map(|arr| arr.value(row))
                .ok_or_else(|| {
                    ArrowExecutorErrorKind::FatalError(
                        "Unable to cast to correct array!".to_string(),
                    )
                })?;

            let t: *mut T::Native = transmute(data);

            *t = *t + value_to_add;
            Ok(())
        }
    }

    fn append_to_column(&self, data: *mut u8, dest: *mut u8) -> Result<()> {
        unsafe {
            let src: *mut T::Native = transmute(data);
            let dest: *mut T::Native = transmute(dest);

            copy(src, dest, 1);

            Ok(())
        }
    }
}

impl<T> Sum<T>
where
    T: ArrowNumericType,
    T::Native: Add<Output = T::Native>,
{
    fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }

    fn boxed() -> Box<dyn AggregateFunction> {
        Box::new(Sum::<T>::new()) as Box<dyn AggregateFunction>
    }
}

pub fn create_sum(data_type: &DataType) -> Result<Box<dyn AggregateFunction>> {
    match data_type {
        &DataType::Int8 => Ok(Sum::<Int8Type>::boxed()),
        &DataType::Int16 => Ok(Sum::<Int16Type>::boxed()),
        &DataType::Int32 => Ok(Sum::<Int32Type>::boxed()),
        &DataType::Int64 => Ok(Sum::<Int64Type>::boxed()),
        &DataType::UInt8 => Ok(Sum::<UInt8Type>::boxed()),
        &DataType::UInt16 => Ok(Sum::<UInt16Type>::boxed()),
        &DataType::UInt32 => Ok(Sum::<UInt32Type>::boxed()),
        &DataType::UInt64 => Ok(Sum::<UInt64Type>::boxed()),
        &DataType::Float32 => Ok(Sum::<Float32Type>::boxed()),
        &DataType::Float64 => Ok(Sum::<Float64Type>::boxed()),
        t => Err(ArrowExecutorErrorKind::FatalError(format!(
            "{:?} is not supported for sum aggregation!",
            t
        ))
        .into()),
    }
}
