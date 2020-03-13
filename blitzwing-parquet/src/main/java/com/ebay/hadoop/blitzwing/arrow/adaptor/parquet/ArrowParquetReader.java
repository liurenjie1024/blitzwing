package com.ebay.hadoop.blitzwing.arrow.adaptor.parquet;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.ParquetFileReader;

import java.util.Iterator;
import java.util.List;

public class ArrowParquetReader implements Iterator<RecordBatch> {
  private final ArrowParquetReaderOptions options;
  private final List<ColumnDescriptor> columnDescriptors;
  private final ParquetFileReader parquetFileReader;
  
  public ArrowParquetReader(ArrowParquetReaderOptions options, List<ColumnDescriptor> columnDescriptors, ParquetFileReader parquetFileReader) {
    this.options = options;
    this.columnDescriptors = columnDescriptors;
    this.parquetFileReader = parquetFileReader;
  }
  
  @Override
  public boolean hasNext() {
    throw new UnsupportedOperationException("Not implemented yet!");
  }
  
  @Override
  public RecordBatch next() {
    throw new UnsupportedOperationException("Not implemented yet!");
  }
}
