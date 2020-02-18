package com.ebay.hadoop.arrow.executor.benchmark.reader;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class ReaderBenchmark {
  @Benchmark
  @BenchmarkMode({Mode.SampleTime})
  @Fork(value = 0)
  @Warmup(iterations = 1)
  @Measurement(iterations = 3)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void putIntVector(BenchmarkData state) {
    for (int i=0; i< state.arrayLength; i++) {
      state.intVector.setSafe(i, state.array[i]);
    }
  }
  
  @Benchmark
  @BenchmarkMode({Mode.SampleTime})
  @Fork(value = 0)
  @Warmup(iterations = 1)
  @Measurement(iterations = 3)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void copyToOffheadp(BenchmarkData state) {
    state.offHeapStorage.position(0);
    for (int i=0; i< state.arrayLength; i++) {
      int start = i*4;
      state.onHeapData[start] = (byte) (state.array[i] & 0xFF);
      state.onHeapData[start+1] = (byte) ((state.array[i] >> 8) & 0x00);
      state.onHeapData[start+2] = (byte) ((state.array[i] >> 16) & 0x00);
      state.onHeapData[start+3] = (byte) ((state.array[i] >> 24) & 0x00);
    }
    
    state.offHeapStorage.put(state.onHeapData);
  }
  
  
  @State(Scope.Thread)
  public static class BenchmarkData {
    
    @Param({"1024", "2048"})
    public int arrayLength;
    public int[] array;
    
    public RootAllocator allocator;
    public IntVector intVector;
    
    public byte[] onHeapData;
    public ByteBuffer offHeapStorage;
    
    
    @Setup
    public void init() {
      Random random = new Random();
      array = new int[arrayLength];
      for (int i=0; i<arrayLength; i++) {
        array[i] = random.nextInt();
      }
      
      allocator = new RootAllocator(Long.MAX_VALUE);
      intVector = new IntVector("", allocator);
      intVector.setInitialCapacity(arrayLength);
      
      onHeapData = new byte[arrayLength * 4];
      offHeapStorage = ByteBuffer.allocateDirect(onHeapData.length);
    }
  }
}
