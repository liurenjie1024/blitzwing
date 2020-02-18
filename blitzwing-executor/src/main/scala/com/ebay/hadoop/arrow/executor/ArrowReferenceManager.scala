package com.ebay.hadoop.arrow.executor

import java.util.concurrent.atomic.AtomicInteger

import io.netty.buffer.ArrowBuf
import org.apache.arrow.memory.{BufferAllocator, OwnershipTransferResult, ReferenceManager}

class ArrowReferenceManager(private val memory: ArrowBufferWrapper) extends ReferenceManager {
  private val bufRefCnt = new AtomicInteger(0)

  override def getRefCount: Int = {
    bufRefCnt.get()
  }

  override def release(): Boolean = release(1)

  override def release(decrement: Int): Boolean = {
    require(decrement >= 1, "ref count decrement should be greater than or equal to 1")
    // decrement the ref count
    var refCnt = 0
    this synchronized {
      refCnt = bufRefCnt.addAndGet(-decrement)
      if (refCnt == 0) { // refcount of this reference manager has dropped to 0
        // release the underlying memory
        memory.close()
      }
    }

    // the new ref count should be >= 0
    require(refCnt >= 0, "RefCnt has gone negative")
    refCnt == 0
  }

  override def retain(): Unit = retain(1)

  override def retain(increment: Int): Unit = {
    require(increment >= 1, s"retain argument($increment) is not positive")
    bufRefCnt.addAndGet(increment)
  }

  override def retain(srcBuffer: ArrowBuf, targetAllocator: BufferAllocator): ArrowBuf = {
    retain()
    srcBuffer
  }

  override def deriveBuffer(sourceBuffer: ArrowBuf, index: Int, length: Int): ArrowBuf = ???

  override def transferOwnership(sourceBuffer: ArrowBuf, targetAllocator: BufferAllocator): OwnershipTransferResult = {
    throw new UnsupportedOperationException("transferOwnership")
  }

  override def getAllocator: BufferAllocator = null

  override def getSize: Int = memory.length.toInt

  override def getAccountedSize: Int = 0
}
