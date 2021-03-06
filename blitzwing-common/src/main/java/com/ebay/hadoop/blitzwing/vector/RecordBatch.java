package com.ebay.hadoop.blitzwing.vector;

import com.ebay.hadoop.blitzwing.generated.vector.RecordBatchProto.JniBufferNodeProto;
import com.ebay.hadoop.blitzwing.generated.vector.RecordBatchProto.JniRecordBatchProto;
import com.ebay.hadoop.blitzwing.memory.ArrowBufferWrapper;
import com.ebay.hadoop.blitzwing.memory.ArrowReferenceManager;
import com.ebay.hadoop.blitzwing.memory.MemoryManager;
import io.netty.buffer.ArrowBuf;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

public class RecordBatch implements AutoCloseable {
  private final int rowCount;
  private final List<FieldVector> columns;

  public RecordBatch(int rowCount, List<FieldVector> columns) {
    this.rowCount = rowCount;
    this.columns = columns;
  }

  public int getRowCount() {
    return rowCount;
  }

  public List<FieldVector> getColumns() {
    return columns;
  }

  public static ArrowRecordBatch build(JniRecordBatchProto jniRecordBatch, MemoryManager memoryManager) {
    List<ArrowFieldNode> fieldNodes = jniRecordBatch.getNodesList().stream()
        .map(n -> new ArrowFieldNode(n.getLength(), n.getNullCount()))
        .collect(Collectors.toList());

    List<ArrowBuf> buffers = jniRecordBatch.getBuffersList().stream()
        .map(b -> RecordBatch.toArrowBuf(b, memoryManager))
        .collect(Collectors.toList());

    return new ArrowRecordBatch(jniRecordBatch.getLength(), fieldNodes, buffers);
  }

  public static ArrowBuf toArrowBuf(JniBufferNodeProto b, MemoryManager memoryManager) {
    ArrowBufferWrapper bufferWrapper = new ArrowBufferWrapper(memoryManager, b.getAddress(), b.getLength());
    ArrowReferenceManager refManager = new ArrowReferenceManager(bufferWrapper);

    return new ArrowBuf(refManager, null, b.getLength(), b.getAddress(), b.getLength() == 0);
  }

  public static RecordBatch from(VectorSchemaRoot vectors) {
    return new RecordBatch(vectors.getRowCount(), vectors.getFieldVectors());
  }

  @Override
  public void close() throws Exception {
    Exception ret = null;
    for (FieldVector v: columns) {
      try {
        v.close();
      } catch (Exception e) {
        if (ret == null) {
          ret = e;
        } else {
          ret.addSuppressed(e);
        }
      }
    }

    if (ret != null) {
      throw ret;
    }
  }
}

