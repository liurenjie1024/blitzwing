package com.ebay.hadoop.blitzwing.arrow.adaptor.parquet;

import com.ebay.hadoop.blitzwing.generated.arrow.adaptor.parquet.ParquetProto.RowGroupProto;
import com.ebay.hadoop.blitzwing.generated.vector.RecordBatchProto.JniRecordBatchProto;
import com.google.protobuf.InvalidProtocolBufferException;

public class JniWrapper implements AutoCloseable {
  private long instanceId;

  public JniWrapper(long instanceId) {
    this.instanceId = instanceId;
  }

  public void setRowGroupData(RowGroupProto rowGroupData) {
    byte[] serializedData = rowGroupData.toByteArray();
    setRowGroupData(instanceId, serializedData);
  }

  public long next() {
    return next(instanceId);
  }

  public JniRecordBatchProto collect() throws InvalidProtocolBufferException {
    return JniRecordBatchProto.parseFrom(collect(instanceId));
  }

  public void freeBuffer(long address, int length) {
    freeBuffer(instanceId, address, length);
  }

  @Override
  public void close() {
    try {
      close(instanceId);
    } finally {
      instanceId = 0;
    }
  }

  public static JniWrapper create(ParquetReaderOptions options) {
    return new JniWrapper(newInstance(options.toParquetReaderProto().toByteArray()));
  }

  public native static long newInstance(byte[] parquetReaderProto);
  public native static void setRowGroupData(long instanceId, byte[] rowGroupData);
  public native static long next(long instanceId);
  public native static byte[] collect(long instanceId);
  public native static void freeBuffer(long instanceId, long address, int length);
  public native static void close(long instanceId);
}
