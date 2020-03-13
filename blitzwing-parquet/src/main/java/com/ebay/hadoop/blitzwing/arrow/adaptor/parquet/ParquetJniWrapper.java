package com.ebay.hadoop.blitzwing.arrow.adaptor.parquet;

public class ParquetJniWrapper {
  private final long instanceId;

  public ParquetJniWrapper(long instanceId) {
    this.instanceId = instanceId;
  }

  public int next() {
    throw new UnsupportedOperationException("Not implemented!");
  }


}
