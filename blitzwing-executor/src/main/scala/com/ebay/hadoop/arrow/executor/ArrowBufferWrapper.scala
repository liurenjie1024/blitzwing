package com.ebay.hadoop.arrow.executor

import com.ebay.hadoop.arrow.executor.plan.PlannedRecordBatch.JNIBufferNode

case class ArrowBufferWrapper(executorId: Long, memoryAddress: Long, length: Long) extends AutoCloseable {
  override def close(): Unit = {
    JniExecutor.freeBuffer(executorId, memoryAddress, length)
  }
}

object ArrowBufferWrapper {
  def apply(jniBufferNode: JNIBufferNode, executorId: Long): ArrowBufferWrapper = {
    new ArrowBufferWrapper(executorId, jniBufferNode.getAddress, jniBufferNode.getLength)
  }
}
