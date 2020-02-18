package com.ebay.hadoop.blitzwing.parquet;

import org.apache.arrow.vector.ValueVector;

import java.util.List;

public class RecordBatch {
  private final int rowCount;
  private final List<ValueVector> columns;
  
  public RecordBatch(int rowCount, List<ValueVector> columns) {
    this.rowCount = rowCount;
    this.columns = columns;
  }
  
  public int getRowCount() {
    return rowCount;
  }
  
  public List<ValueVector> getColumns() {
    return columns;
  }
}
