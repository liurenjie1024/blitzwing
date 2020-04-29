package com.ebay.hadoop.blitzwing.arrow.adaptor.parquet.memory;

import org.apache.arrow.memory.RootAllocator;
import org.apache.parquet.hadoop.blitzwing.BufferAllocator;
import org.apache.parquet.hadoop.blitzwing.ToByteBuffer;

public class ArrowBufManager implements BufferAllocator {
  private final org.apache.arrow.memory.BufferAllocator arrowAllocator = new RootAllocator();

  @Override
  public ToByteBuffer allocate(int i) {
    return new ArrowBufWrapper(i, arrowAllocator.buffer(i));
  }

  @Override
  public void release(Object o) {
    if (o instanceof ArrowBufWrapper) {
      ((ArrowBufWrapper)o).getInner().close();
    } else {
      throw new IllegalArgumentException("ArrowBufManager only manages ArrowBufWrapper!");
    }
  }
}
