use crate::aggregation::create_function;
use crate::aggregation::AggregateFunction;
use crate::error::{ArrowExecutorErrorKind, Result};
use crate::executor::types::proto_type_to_data_type;
use crate::executor::{Context, Executor};
use crate::proto::plan::AggregationNode;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;
use std::vec::Vec;

struct AggregateDesc {
    _func: Box<dyn AggregateFunction>,
    _inputs: Vec<usize>,
}

pub struct AggregationExecutor {
    _keys: Vec<usize>,
    _aggregations: Vec<AggregateDesc>,
    _map: HashMap<Vec<Vec<u8>>, Vec<*mut u8>>,
    _output_schema: Arc<Schema>,
    _child: Box<dyn Executor>,
}

impl Executor for AggregationExecutor {
    fn output_schema(&self) -> &Schema {
        unimplemented!()
    }

    fn open(&mut self, context: &Context) -> Result<()> {
        unimplemented!()
    }

    fn next(&mut self, context: &Context) -> Result<Option<Arc<RecordBatch>>> {
        unimplemented!()
    }

    fn close(&mut self) -> Result<()> {
        unimplemented!()
    }
}

impl AggregationExecutor {
    pub fn from_plan_node(
        agg_plan_node: &AggregationNode,
        child: Box<dyn Executor>,
    ) -> Result<Box<dyn Executor>> {
        let input_schema = child.output_schema();

        let mut output_fields: Vec<Field> = Vec::with_capacity(
            agg_plan_node.get_groups().len() + agg_plan_node.get_aggregations().len(),
        );

        let mut keys: Vec<usize> = Vec::with_capacity(agg_plan_node.get_groups().len());
        for group_node in agg_plan_node.get_groups() {
            let expr_name = group_node.get_expr_type().get_name();
            if let Some((idx, field)) = input_schema.column_with_name(expr_name) {
                keys.push(idx);
                output_fields.push(field.clone());
            } else {
                Err(ArrowExecutorErrorKind::PlanError(format!(
                    "Column not found: {}",
                    expr_name
                )))?
            }
        }

        let mut aggregations: Vec<AggregateDesc> =
            Vec::with_capacity(agg_plan_node.get_aggregations().len());
        for agg_node in agg_plan_node.get_aggregations() {
            let mut arg_indexes: Vec<usize> = Vec::with_capacity(agg_node.get_children().len());
            let mut arg_types: Vec<&DataType> = Vec::with_capacity(agg_node.get_children().len());

            for arg in agg_node.get_children() {
                let arg_name = arg.get_expr_type().get_name();
                if let Some((idx, field)) = input_schema.column_with_name(arg_name) {
                    arg_indexes.push(idx);
                    arg_types.push(field.data_type());
                } else {
                    Err(ArrowExecutorErrorKind::PlanError(format!(
                        "Column not found: {}",
                        arg_name
                    )))?
                }
            }

            let agg_operation = if agg_node.has_agg_node() {
                agg_node.get_agg_node().get_operation()
            } else {
                Err(ArrowExecutorErrorKind::PlanError(
                    "Aggregation in aggregation plan node should be \
                     aggregation expression node!"
                        .to_string(),
                ))?
            };

            let agg_func = create_function(agg_operation, &arg_types)?;

            output_fields.push(Field::new(
                agg_node.get_expr_type().get_name(),
                agg_func.result_type(),
                false,
            ));

            aggregations.push(AggregateDesc {
                _func: agg_func,
                _inputs: arg_indexes,
            });
        }

        Ok(Box::new(Self {
            _keys: keys,
            _aggregations: aggregations,
            _map: HashMap::new(),
            _output_schema: Arc::new(Schema::new(output_fields)),
            _child: child,
        }))
    }
}
