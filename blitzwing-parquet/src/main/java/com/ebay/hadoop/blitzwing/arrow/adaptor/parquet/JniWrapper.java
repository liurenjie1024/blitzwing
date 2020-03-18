package com.ebay.hadoop.blitzwing.arrow.adaptor.parquet;

import com.ebay.hadoop.blitzwing.generated.arrow.adaptor.parquet.ParquetProto.RowGroupProto;
import com.ebay.hadoop.blitzwing.generated.vector.RecordBatchProto.JniRecordBatchProto;
import com.google.protobuf.InvalidProtocolBufferException;

public class JniWrapper implements AutoCloseable {
  private final long instanceId;

  public JniWrapper(long instanceId) {
    this.instanceId = instanceId;
  }

  public void setRowGroupData(RowGroupProto rowGroupData) {
    byte[] serializedData = rowGroupData.toByteArray();
    setRowGroupData(instanceId, serializedData);
  }

  public JniRecordBatchProto next() throws InvalidProtocolBufferException {
    return JniRecordBatchProto.parseFrom(next(instanceId));
  }

  public void freeBuffer(long address) {
    freeBuffer(instanceId, address);
  }

  @Override
  public void close() {
    close(instanceId);
  }

  public static JniWrapper create(ParquetReaderOptions options) {
    return new JniWrapper(newInstance(options.toParquetReaderProto().toByteArray()));
  }

  public native static long newInstance(byte[] parquetReaderProto);
  public native static void setRowGroupData(long instanceId, byte[] rowGroupData);
  public native static byte[] next(long instanceId);
  public native static void freeBuffer(long instanceId, long address);
  public native static void close(long instanceId);
}
