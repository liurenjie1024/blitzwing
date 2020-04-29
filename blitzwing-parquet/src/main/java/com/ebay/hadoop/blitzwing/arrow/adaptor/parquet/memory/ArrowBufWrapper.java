package com.ebay.hadoop.blitzwing.arrow.adaptor.parquet.memory;

import io.netty.buffer.ArrowBuf;
import java.nio.ByteBuffer;
import org.apache.parquet.hadoop.blitzwing.ToByteBuffer;

public class ArrowBufWrapper implements ToByteBuffer {
  private final ArrowBuf inner;

  public ArrowBufWrapper(ArrowBuf inner) {
    this.inner = inner;
  }

  @Override
  public ByteBuffer toByteBuffer() {
    return inner.nioBuffer(0, inner.capacity());
  }

  ArrowBuf getInner() {
    return inner;
  }
}
