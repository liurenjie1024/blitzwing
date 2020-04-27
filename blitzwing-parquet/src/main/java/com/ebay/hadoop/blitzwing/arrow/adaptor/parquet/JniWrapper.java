package com.ebay.hadoop.blitzwing.arrow.adaptor.parquet;

import com.ebay.hadoop.blitzwing.generated.arrow.adaptor.parquet.ParquetProtoOuter.RowGroupProto;
import com.ebay.hadoop.blitzwing.generated.vector.RecordBatchProto.JniRecordBatchProto;
import com.google.protobuf.InvalidProtocolBufferException;

public class JniWrapper implements AutoCloseable {
  private long instanceId;

  JniWrapper(long instanceId) {
    this.instanceId = instanceId;
  }

  public synchronized void setRowGroupData(RowGroupProto rowGroupData) {
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
    System.out.println("Freeing buffer: " + address + ", " + length);
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

  native static long newInstance(byte[] parquetReaderProto);
  private native static void setRowGroupData(long instanceId, byte[] rowGroupData);
  private native static long next(long instanceId);
  private native static byte[] collect(long instanceId);
  private native static void freeBuffer(long instanceId, long address, int length);
  private native static void close(long instanceId);
}
