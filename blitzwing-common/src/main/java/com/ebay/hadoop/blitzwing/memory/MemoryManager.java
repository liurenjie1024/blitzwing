package com.ebay.hadoop.blitzwing.memory;

public interface MemoryManager {
  void freeBuffer(long memoryAddress, int length);
}
