package com.ebay.hadoop.arrow.executor

import com.ebay.hadoop.arrow.executor.plan.ArrowPlan
import com.ebay.hadoop.arrow.executor.plan.types.ArrowTypes.{BasicType, ExprType, Schema}
import com.ebay.hadoop.arrow.executor.plan.ArrowPlan.NodeType
import com.ebay.hadoop.arrow.executor.plan.PlannedRecordBatch.JNIRecordBatch
import io.netty.buffer.ArrowBuf
import org.apache.arrow.vector.ipc.message.{ArrowFieldNode, ArrowRecordBatch}
import org.apache.arrow.vector.types.pojo.{Field, FieldType, Schema => ArrowSchema}
import org.apache.arrow.vector.types.Types.MinorType

import scala.collection.JavaConverters._

object ArrowPlanUtils {
  implicit class ArrowPlanWrapper(val plan: ArrowPlan.Plan) {
    def outputSchema(): ArrowSchema = {
      val rootPlanNode = plan.getPlanNodesList.asScala.head

      rootPlanNode.getNodeType match {
        case NodeType.AGG_NODE => {
          val aggNode = rootPlanNode.getAggNode
          expressionTypeListToArrowSchema(
            (aggNode.getGroupsList.asScala ++ aggNode.getAggregationsList.asScala)
              .map(_.getExprType))
        }
        case NodeType.PARQUET_FILE_SCAN_NODE => {
          schemaToArrowSchema(rootPlanNode.getParquetFileScanNode.getSchema)
        }
        case NodeType.JNI_INPUT_NODE  => {
          schemaToArrowSchema(rootPlanNode.getJniInputNode.getSchema)
        }
      }
    }
  }

  implicit class JNIRecordBatchWrapper(val recordBatch: JNIRecordBatch) {
    def toArrowRecordBatch()(implicit executorId: Long): ArrowRecordBatch = {
      val fieldNodes = recordBatch.getNodesList.asScala.map(n => new ArrowFieldNode(n.getLength, n.getNullCount))
        .asJava
      val buffers = recordBatch.getBuffersList.asScala.map(b => {
        new ArrowBuf(
          new ArrowReferenceManager(ArrowBufferWrapper(b, executorId)),
          null,  b.getLength, b.getAddress,
          b.getLength == 0)
      }).asJava

      new ArrowRecordBatch(recordBatch.getLength, fieldNodes, buffers)
    }
  }

  def planTypeToArrowType(t: ExprType): FieldType = {
    val minorType = t.getBasicType match {
      case BasicType.BOOL => MinorType.BIT
      case BasicType.UINT8 => MinorType.UINT8
      case BasicType.INT8 => MinorType.TINYINT
      case BasicType.UINT16 => MinorType.UINT2
      case BasicType.INT16 => MinorType.SMALLINT
      case BasicType.UINT32 => MinorType.UINT4
      case BasicType.INT32 => MinorType.INT
      case BasicType.UINT64 => MinorType.UINT8
      case BasicType.INT64 => MinorType.BIGINT
      case BasicType.FLOAT => MinorType.FLOAT4
      case BasicType.DOUBLE => MinorType.FLOAT8
      case basicType: BasicType => throw new IllegalArgumentException(s"Unsupported basic type: ${basicType}")
    }

    new FieldType(t.getNullable, minorType.getType, null)
  }

  def schemaToArrowSchema(schema: Schema): ArrowSchema = {
    expressionTypeListToArrowSchema(schema.getFieldsList.asScala)
  }

  def expressionTypeListToArrowSchema(fields : Iterable[ExprType]): ArrowSchema = {
    new ArrowSchema(fields
      .map(node => new Field("", ArrowPlanUtils.planTypeToArrowType(node), null))
      .asJava)
  }
}

