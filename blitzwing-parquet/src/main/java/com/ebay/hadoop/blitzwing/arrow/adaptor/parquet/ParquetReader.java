package com.ebay.hadoop.blitzwing.arrow.adaptor.parquet;

import com.ebay.hadoop.blitzwing.exception.BlitzwingException;
import com.ebay.hadoop.blitzwing.generated.arrow.adaptor.parquet.ParquetProto.ChunkProto;
import com.ebay.hadoop.blitzwing.generated.arrow.adaptor.parquet.ParquetProto.RowGroupProto;
import com.ebay.hadoop.blitzwing.generated.arrow.adaptor.parquet.ParquetProto.SegmentProto;
import com.ebay.hadoop.blitzwing.generated.vector.RecordBatchProto.JniRecordBatchProto;
import com.ebay.hadoop.blitzwing.memory.MemoryManager;
import com.ebay.hadoop.blitzwing.vector.RecordBatch;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Optional;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.blitzwing.RawChunkStore;
import sun.nio.ch.DirectBuffer;

public class ParquetReader extends ArrowReader implements MemoryManager, Iterator<RecordBatch> {
  private final ParquetReaderOptions options;
  private final Iterator<RawChunkStore> rawChunkStores;
  private final JniWrapper jni;

  private boolean eof;
  private RecordBatch lastBatch;

  public ParquetReader(Iterator<RawChunkStore> rawChunkStores, BufferAllocator allocator,
      ParquetReaderOptions options, JniWrapper jni) {
    super(allocator);
    this.rawChunkStores = rawChunkStores;
    this.options = options;
    this.jni = jni;

    this.eof = false;
    this.lastBatch = null;
  }

  @Override
  public boolean loadNextBatch() throws IOException {
    JniRecordBatchProto recordBatchProto = null;
    int curBatchLength = 0;

    while(true) {
      recordBatchProto = jni.next();
      curBatchLength = Optional.ofNullable(recordBatchProto)
          .map(JniRecordBatchProto::getLength)
          .orElse(0);

      if (curBatchLength < options.getBatchSize()) {
        if (!rawChunkStores.hasNext()) {
          break;
        }

        jni.setRowGroupData(toRowGroupProto(rawChunkStores.next()));
      } else {
        break;
      }
    }

    if (curBatchLength == 0) {
      return false;
    }

    loadRecordBatch(RecordBatch.build(recordBatchProto, this));
    return true;
  }

  @Override
  public long bytesRead() {
    return 0;
  }

  @Override
  protected void closeReadSource() throws IOException {
    jni.close();
  }

  @Override
  protected Schema readSchema() throws IOException {
    return options.getSchema();
  }

  public static RowGroupProto toRowGroupProto(RawChunkStore rawChunkStore) {
    RowGroupProto.Builder rowGroup = RowGroupProto.newBuilder();

    for (ColumnDescriptor column: rawChunkStore.getColumns()) {
      ChunkProto.Builder chunkProto = ChunkProto.newBuilder()
          .setColumnName(column.getPath()[0]);

      for(ByteBuffer byteBuffer: rawChunkStore.getChunk(column).getData()) {
        if (!byteBuffer.isDirect()) {
          throw new IllegalArgumentException("Parquet reader can only use direct byte buffer!");
        }

        SegmentProto segment = SegmentProto.newBuilder()
            .setAddress(((DirectBuffer)byteBuffer).address())
            .setLength(byteBuffer.capacity())
            .build();

        chunkProto.addSegments(segment);
      }

      rowGroup.addChunks(chunkProto.build());
    }

    return rowGroup.build();
  }

  @Override
  public void freeBuffer(long memoryAddress, long length) {
    jni.freeBuffer(memoryAddress);
  }

  @Override
  public boolean hasNext() {
    if (eof) {
      return false;
    }

    if (lastBatch != null) {
      return true;
    }

    doNext();

    return !eof;
  }

  @Override
  public RecordBatch next() {
    if (!eof && (lastBatch == null)) {
      doNext();
    }

    return lastBatch;
  }

  private void doNext() {
    try {
      lastBatch = null;
      boolean loaded = loadNextBatch();
      if (!loaded) {
        eof = true;
      } else {
        lastBatch = RecordBatch.from(getVectorSchemaRoot());
      }
    } catch (Exception e) {
      throw new BlitzwingException(e);
    }
  }
}
