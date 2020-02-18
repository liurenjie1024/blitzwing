package com.ebay.hadoop.arrow.executor

import com.ebay.hadoop.arrow.executor.plan.ArrowPlan
import ArrowExecutor._
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{ValueVector, VectorSchemaRoot}
import scala.collection.JavaConverters._

case class RecordBatch(numRows: Int, vectors: Array[ValueVector])

object RecordBatch {
  def apply(vectors: VectorSchemaRoot): RecordBatch = {
    RecordBatch(vectors.getRowCount, vectors.getFieldVectors.asScala.toArray)
  }
}

case class ArrowExecutor(index: Int, plan: ArrowPlan.Plan) extends Iterator[RecordBatch] { self =>
  private lazy val reader = new ArrowExecutorReader(rootAllocator, plan)

  private var eof = false
  private var lastRecordBatch: RecordBatch = _

  def next(): RecordBatch = {
    if (eof) {
      throw new NoSuchElementException
    } else {
      if (lastRecordBatch != null) {
        val ret = lastRecordBatch
        lastRecordBatch = null
        ret
      } else {
        doNext
        if (eof) {
          throw new NoSuchElementException
        } else {
          lastRecordBatch
        }
      }
    }
  }

  private[executor] def doNext: Unit = {
    if (reader.loadNextBatch()) {
      lastRecordBatch = RecordBatch(reader.getVectorSchemaRoot)
    } else {
      lastRecordBatch = null
      eof = true
    }
  }
  override def hasNext: Boolean = {
    if (eof) {
      false
    } else if (lastRecordBatch == null) {
      doNext
      !eof
    } else {
      false
    }
  }
}

object ArrowExecutor {
  private val rootAllocator = new RootAllocator(Long.MaxValue)
}
