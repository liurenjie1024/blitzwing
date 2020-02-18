package com.ebay.hadoop.blitzwing.parquet;

public class ArrowReaderOptions {
  private final int batchSize;
  
  public ArrowReaderOptions(Builder builder) {
    this.batchSize = builder.batchSize;
  }
  
  public int getBatchSize() {
    return batchSize;
  }
  
  public static class Builder {
    private int batchSize;
    
    public Builder withBatchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }
  }
}
