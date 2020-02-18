package com.ebay.hadoop.arrow.executor.agg;

import java.nio.ByteBuffer;

public class JniWrapper {
  static {
    System.loadLibrary("arrow_executor_rs");
  }
  
  public native void update(long keysAddress, long valuesAddress, int size);
  public native int getSize();
  public native void toIterator();
  public native void next(ByteBuffer buf);
  public native void clean();
}
