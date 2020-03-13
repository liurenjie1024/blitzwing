package com.ebay.hadoop.blitzwing.memory;

public class ArrowBufferWrapper implements AutoCloseable {
  private final MemoryManager memoryManager;
  private final long address;
  private final int length;

  public ArrowBufferWrapper(MemoryManager memoryManager, long address, int length) {
    this.memoryManager = memoryManager;
    this.address = address;
    this.length = length;
  }

  @Override
  public void close() {
    memoryManager.freeBuffer(address, length);
  }

  public long getAddress() {
    return address;
  }

  public int getLength() {
    return length;
  }
}
