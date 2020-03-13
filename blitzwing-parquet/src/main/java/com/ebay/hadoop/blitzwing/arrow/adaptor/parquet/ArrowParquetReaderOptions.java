package com.ebay.hadoop.blitzwing.arrow.adaptor.parquet;

public class ArrowParquetReaderOptions {
  private final int batchSize;
  
  public ArrowParquetReaderOptions(Builder builder) {
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
