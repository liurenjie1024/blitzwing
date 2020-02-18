package com.ebay.hadoop.arrow.executor;

public class JniExecutor {
  static {
    try {
      ArrowJniUtils.loadLibraryFromJar();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  public static native long buildExecutor(long planBufAddress,
                                   long planBufLen);
  
  public static native void open(long executorId);
  public static native byte[] next(long executorId);
  public static native void close(long executorId);
  
  public static native void freeBuffer(long executorId, long address, long size);
}
