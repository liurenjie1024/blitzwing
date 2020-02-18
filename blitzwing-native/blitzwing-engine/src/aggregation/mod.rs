//mod sum;

//use crate::aggregation::sum::{create_sum, Sum};
//use crate::error::{ArrowExecutorErrorKind, Result};
//use crate::proto::expr::AggregateOpration;
//use arrow::array::Array;
//use arrow::datatypes::DataType;
//use std::alloc::Layout;
//
//pub trait AggregateFunction {
//    fn result_type(&self) -> DataType;
//    fn data_layout(&self) -> Layout;
//
//    fn init(&self, data: *mut u8) -> Result<()>;
//    fn add(&self, data: *mut u8, args: &[&dyn Array], row: usize) -> Result<()>;
//    fn append_to_column(&self, data: *mut u8, dest: *mut u8) -> Result<()>;
//}
//
//pub fn create_function(
//    function_type: AggregateOpration,
//    arg_types: &[&DataType],
//) -> Result<Box<dyn AggregateFunction>> {
//    match (function_type, arg_types) {
//        (AggregateOpration::SUM, [data_type]) => create_sum(data_type),
//        (op, args) => Err(ArrowExecutorErrorKind::FatalError(format!(
//            "Can't create aggregation function \
//             for \
//             {:?}, {:?}",
//            op, args
//        ))
//        .into()),
//    }
//}
