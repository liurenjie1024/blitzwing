package com.ebay.hadoop.blitzwing.arrow.adaptor.parquet.memory;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import org.apache.parquet.hadoop.blitzwing.ToByteBuffer;
import org.junit.Test;

public class ArrowBufManagerTest {
  @Test
  public void testWrite() {
    ArrowBufManager manager = new ArrowBufManager();
    ToByteBuffer rawBuffer = manager.allocate(10);
    ByteBuffer buffer = rawBuffer.toByteBuffer();
    assertEquals(0, buffer.position());
    assertEquals(10, buffer.limit());
    for (int i=0; i<10; i++) {
      buffer.put((byte)i);
    }
    buffer.flip();
    assertEquals(10, buffer.remaining());
  }
}
