package com.ebay.hadoop.blitzwing.memory;

import io.netty.buffer.ArrowBuf;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OwnershipTransferResult;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.util.Preconditions;

public class ArrowReferenceManager implements ReferenceManager {
  private final ArrowBufferWrapper buffer;
  private final AtomicInteger bufferRefCount = new AtomicInteger(0);

  public ArrowReferenceManager(ArrowBufferWrapper buffer) {
    this.buffer = buffer;
  }


  @Override
  public int getRefCount() {
    return bufferRefCount.get();
  }

  @Override
  public boolean release() {
    return release(1);
  }

  @Override
  public boolean release(int decrement) {
    Preconditions.checkArgument(decrement >= 1, "ref count decrement should be greater than or equal to 1");

    synchronized (this) {
      int refCount = bufferRefCount.addAndGet(-decrement);
      if (refCount == 0) {
        buffer.close();
      }

      if (refCount < 0) {
        throw new IllegalStateException("Ref count has gone negative: " + refCount);
      }
      return refCount == 0;
    }
  }

  @Override
  public void retain() {
    retain(1);
  }

  @Override
  public void retain(int increment) {
    Preconditions.checkArgument(increment >= 1, "Retain argument is not positive");
    bufferRefCount.addAndGet(increment);
  }

  @Override
  public ArrowBuf retain(ArrowBuf srcBuffer, BufferAllocator targetAllocator) {
    retain();
    return srcBuffer;
  }

  @Override
  public ArrowBuf deriveBuffer(ArrowBuf sourceBuffer, int index, int length) {
    throw new UnsupportedOperationException("Derive buffer is not supported!");
  }

  @Override
  public OwnershipTransferResult transferOwnership(ArrowBuf sourceBuffer,
      BufferAllocator targetAllocator) {
    throw new UnsupportedOperationException("transferOwnership is not supported!");
  }

  @Override
  public BufferAllocator getAllocator() {
    return null;
  }

  @Override
  public int getSize() {
    return buffer.getLength();
  }

  @Override
  public int getAccountedSize() {
    return 0;
  }
}
