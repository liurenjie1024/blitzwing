package com.ebay.hadoop.blitzwing.arrow.adaptor.parquet.memory;

import io.netty.buffer.ArrowBuf;
import java.nio.ByteBuffer;
import org.apache.parquet.hadoop.blitzwing.ToByteBuffer;

public class ArrowBufWrapper implements ToByteBuffer {
  private final int actualLen;
  private final ArrowBuf inner;

  public ArrowBufWrapper(int actualLen, ArrowBuf inner) {
    this.actualLen = actualLen;
    this.inner = inner;
  }

  @Override
  public ByteBuffer toByteBuffer() {
    return inner.nioBuffer(0, actualLen);
  }

  ArrowBuf getInner() {
    return inner;
  }
}
