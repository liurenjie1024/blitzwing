package com.ebay.hadoop.blitzwing.arrow.adaptor.parquet;

import com.ebay.hadoop.blitzwing.generated.arrow.adaptor.parquet.ParquetProto.ParquetReaderProto;
import org.apache.arrow.vector.types.pojo.Schema;

public class ParquetReaderOptions {
  private final int batchSize;
  private final Schema schema;
  
  public ParquetReaderOptions(Builder builder) {
    this.batchSize = builder.batchSize;
    this.schema = builder.schema;
  }
  
  public int getBatchSize() {
    return batchSize;
  }

  public Schema getSchema() {
    return schema;
  }

  public ParquetReaderProto toParquetReaderProto() {
    throw new UnsupportedOperationException("toParquetReaderProto");
  }
  
  public static class Builder {
    private int batchSize;
    private Schema schema;

    public Builder withBatchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public Builder withSchema(Schema schema) {
      this.schema = schema;
      return this;
    }
  }
}
