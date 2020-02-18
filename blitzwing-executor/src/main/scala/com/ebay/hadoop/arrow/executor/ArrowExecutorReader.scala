package com.ebay.hadoop.arrow.executor

import java.nio.ByteBuffer

import com.ebay.hadoop.arrow.executor.plan.ArrowPlan.Plan
import ArrowPlanUtils.JNIRecordBatchWrapper
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.Schema
import ArrowPlanUtils.ArrowPlanWrapper
import com.ebay.hadoop.arrow.executor.plan.PlannedRecordBatch.JNIRecordBatch
import sun.nio.ch.DirectBuffer

class ArrowExecutorReader(private val bufferAllocator: BufferAllocator, private val plan: Plan) extends
  ArrowReader(bufferAllocator) {

  ensureInitialized()

  implicit private lazy val executorId: Long = {
    val serializedPlan = plan.toByteArray
    val buffer = ByteBuffer.allocateDirect(serializedPlan.length)
    buffer.put(serializedPlan)
    val directBuf = buffer.asInstanceOf[DirectBuffer]
    JniExecutor.buildExecutor(directBuf.address(), serializedPlan.length)
  }

  override def loadNextBatch(): Boolean = {
    JniExecutor.next(executorId) match {
      case null => false
      case serializedRecordBatch => {
        val jniRecordBatch = JNIRecordBatch.parseFrom(serializedRecordBatch)
        val arrowRecordBatch = jniRecordBatch.toArrowRecordBatch()
        loadRecordBatch(arrowRecordBatch)
        true
      }
    }
  }

  override def bytesRead(): Long = 0

  override def closeReadSource(): Unit = JniExecutor.close(executorId)

  override def readSchema(): Schema = plan.outputSchema()

  override def initialize(): Unit = {
    super.initialize()
    JniExecutor.open(executorId)
  }
}
