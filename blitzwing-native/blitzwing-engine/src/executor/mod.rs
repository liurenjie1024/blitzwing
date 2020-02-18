//mod aggregation_executor;
mod chunk_manager;
//pub mod types;
pub mod parquet_scan_executor;

use crate::error::{ArrowExecutorErrorKind, Result};
use crate::executor::parquet_scan_executor::ParquetScanExecutor;
use crate::proto::plan::NodeType;
use crate::proto::plan::{Plan, PlanNode};
use crate::proto::record_batch::{JNIBufferNode, JNIRecordBatch, JNIValueNode};
use arrow::array::ArrayDataRef;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use chunk_manager::ChunkManager;
use failure::ResultExt;
use failure::_core::intrinsics::transmute;
use jni::objects::{JClass, JObject};
use jni::sys::{jbyteArray, jlong};
use jni::JNIEnv;
use protobuf::{parse_from_bytes, Message};
use std::collections::HashMap;
use std::rc::Rc;

pub struct JniContext<'a> {
    jni_env: JNIEnv<'a>,
    _jni_class: JClass<'a>,
}

impl<'a> JniContext<'a> {
    pub fn new(jni_env: JNIEnv<'a>, jni_class: JClass<'a>) -> Self {
        Self {
            jni_env,
            _jni_class: jni_class,
        }
    }
}

pub struct Context<'a> {
    chunk_manager: Rc<ChunkManager>,
    jni_env: JNIEnv<'a>,
}

impl<'a> Context<'a> {
    pub fn jni_env_ref(&self) -> &JNIEnv<'a> {
        &self.jni_env
    }

    pub fn chunk_manager_ref(&self) -> &ChunkManager {
        &self.chunk_manager
    }
}

pub trait Executor {
    fn output_schema(&self) -> &Schema;

    fn open(&mut self, context: &Context) -> Result<()>;
    fn next(&mut self, context: &Context) -> Result<Option<&RecordBatch>>;
    fn close(&mut self) -> Result<()>;
}

pub struct ArrowExecutor {
    plan: Box<dyn Executor>,
    buffers: HashMap<i64, ArrayDataRef>,
    chunk_manager: Rc<ChunkManager>,
}

impl ArrowExecutor {
    pub fn from_raw(ptr: jlong) -> Box<Self> {
        unsafe { Box::from_raw(transmute::<jlong, *mut Self>(ptr)) }
    }

    pub fn new(plan: Box<dyn Executor>) -> Self {
        Self {
            plan,
            buffers: HashMap::with_capacity(32),
            chunk_manager: Rc::new(ChunkManager::new()),
        }
    }
}

impl ArrowExecutor {
    fn get_context<'a>(&self, jni_context: &JniContext<'a>) -> Context<'a> {
        Context {
            chunk_manager: self.chunk_manager.clone(),
            jni_env: jni_context.jni_env.clone(),
        }
    }

    pub fn open(&mut self, jni_context: JniContext) -> Result<()> {
        let context = self.get_context(&jni_context);
        self.plan.open(&context)
    }

    pub fn next(&mut self, jni_context: JniContext) -> Result<jbyteArray> {
        let context = self.get_context(&jni_context);

        if let Some(record_batch) = self.plan.next(&context)? {
            let mut jni_record_batch = JNIRecordBatch::new();
            jni_record_batch.set_length(record_batch.num_rows() as i32);
            for i in 0..record_batch.num_columns() {
                let array = record_batch.column(i);

                let mut jni_value_node = JNIValueNode::new();
                jni_value_node.set_length(array.len() as i32);
                jni_value_node.set_null_count(array.null_count() as i32);
                jni_record_batch.mut_nodes().push(jni_value_node);

                if let Some(null_buffer) = array.data_ref().null_buffer() {
                    let mut jni_buffer_node = JNIBufferNode::new();
                    jni_buffer_node.set_address(null_buffer.raw_data() as i64);
                    jni_buffer_node.set_length(null_buffer.len() as i32);

                    self.buffers
                        .insert(jni_buffer_node.get_address(), array.data_ref().clone());
                    jni_record_batch.mut_buffers().push(jni_buffer_node);
                } else {
                    let mut jni_buffer_node = JNIBufferNode::new();
                    jni_buffer_node.set_address(0i64);
                    jni_buffer_node.set_length(0i32);

                    jni_record_batch.mut_buffers().push(jni_buffer_node);
                }

                for buffer in array.data_ref().buffers() {
                    let mut jni_buffer_node = JNIBufferNode::new();
                    jni_buffer_node.set_address(buffer.raw_data() as i64);
                    jni_buffer_node.set_length(buffer.len() as i32);

                    self.buffers
                        .insert(jni_buffer_node.get_address(), array.data_ref().clone());
                    jni_record_batch.mut_buffers().push(jni_buffer_node);
                }
            }

            jni_context
                .jni_env
                .byte_array_from_slice(
                    &jni_record_batch
                        .write_to_bytes()
                        .context(ArrowExecutorErrorKind::ProtobufError)?,
                )
                .context(ArrowExecutorErrorKind::JniError)
                .map_err(|err| err.into())
        } else {
            Ok(JObject::null().into_inner())
        }
    }

    pub fn close(&mut self, _jni_context: JniContext) -> Result<()> {
        self.plan.close()
    }

    pub fn free_buffer(&mut self, buffer_ptr: jlong) -> Result<()> {
        self.buffers.remove(&(buffer_ptr as i64));
        Ok(())
    }
}

pub struct ArrowExecutorBuilder<'a> {
    plan: Plan,
    jni_env: JNIEnv<'a>,
}

impl<'a> ArrowExecutorBuilder<'a> {
    pub fn new(serialized_plan: &[u8], jni_env: JNIEnv<'a>) -> Result<Self> {
        let plan = parse_from_bytes::<Plan>(serialized_plan)
            .context(ArrowExecutorErrorKind::ProtobufError)?;

        Ok(Self { plan, jni_env })
    }

    pub fn build(self) -> Result<Box<ArrowExecutor>> {
        let mut cur_child: Option<Box<dyn Executor>> = None;

        for plan_node in self.plan.get_plan_nodes().iter().rev() {
            cur_child = Some(self.build_one(plan_node, cur_child)?);
        }

        cur_child
            .map(|exe| Box::new(ArrowExecutor::new(exe)))
            .ok_or_else(|| {
                ArrowExecutorErrorKind::PlanError("plan can't be empty!".to_string()).into()
            })
    }

    fn build_one(
        &self,
        node: &PlanNode,
        child_opt: Option<Box<dyn Executor>>,
    ) -> Result<Box<dyn Executor>> {
        match (node.get_node_type(), child_opt) {
            //            (NodeType::AGG_NODE, Some(child)) => {
            //                AggregationExecutor::from_plan_node(node.get_agg_node(), child)
            //            }
            (NodeType::PARQUET_FILE_SCAN_NODE, _) => ParquetScanExecutor::from_plan_node(
                node.get_parquet_file_scan_node(),
                self.jni_env.clone(),
            ),
            (_, _) => Err(ArrowExecutorErrorKind::PlanError(
                "Failed to create arrow executor!".to_string(),
            )
            .into()),
        }
    }
}
