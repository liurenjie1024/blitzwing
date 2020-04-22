package com.ebay.hadoop.blitzwing.arrow.adaptor.parquet;

import com.ebay.hadoop.blitzwing.exception.BlitzwingException;
import com.ebay.hadoop.blitzwing.generated.arrow.adaptor.parquet.ParquetProtoOuter.ColumnChunkProto;
import com.ebay.hadoop.blitzwing.generated.arrow.adaptor.parquet.ParquetProtoOuter.RowGroupProto;
import com.ebay.hadoop.blitzwing.generated.arrow.adaptor.parquet.ParquetProtoOuter.SegmentProto;
import com.ebay.hadoop.blitzwing.memory.MemoryManager;
import com.ebay.hadoop.blitzwing.vector.RecordBatch;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.blitzwing.RawChunkStore;
import sun.nio.ch.DirectBuffer;

public class ParquetArrowReader extends ArrowReader implements MemoryManager, Iterator<RecordBatch> {
  private final ParquetArrowReaderOptions options;
  private final ParquetFileReader fileReader;
  private final JniWrapper jni;

  private boolean eof;
  private RecordBatch lastBatch;

  public ParquetArrowReader(BufferAllocator allocator,
      ParquetArrowReaderOptions options) {
    super(allocator);
    this.fileReader = options.getFileReader();
    this.options = options;
    this.jni = options.getJniWrapper();

    this.eof = false;
    this.lastBatch = null;
  }

  @Override
  public boolean loadNextBatch() throws IOException {
    long totalLen = 0;

    while(true) {
      totalLen += jni.next();

      if (totalLen < options.getBatchSize()) {
        RawChunkStore rawChunkStore = fileReader.readNextRawRowGroup();
        if (rawChunkStore == null) {
          break;
        }

        jni.setRowGroupData(toRowGroupProto(rawChunkStore));
      } else {
        break;
      }
    }

    if (totalLen == 0) {
      return false;
    }

    loadRecordBatch(RecordBatch.build(jni.collect(), this));
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
      ColumnChunkProto.Builder chunkProto = ColumnChunkProto.newBuilder()
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

      rowGroup.addColumns(chunkProto.build());
    }

    return rowGroup.build();
  }

  @Override
  public void freeBuffer(long memoryAddress, int length) {
    jni.freeBuffer(memoryAddress, length);
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

    RecordBatch ret = lastBatch;
    lastBatch = null;
    return ret;
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

//  public static Iterator<RawChunkStore> fromParquetFileReader(ParquetFileReader fileReader) {
//    return new Iterator<RawChunkStore>() {
//      private boolean eof = false;
//      private RawChunkStore last = null;
//
//      @Override
//      public boolean hasNext() {
//        if (last == null && !eof) {
//          doNext();
//        }
//
//        return eof;
//      }
//
//      @Override
//      public RawChunkStore next() {
//        if (last == null && !eof) {
//          doNext();
//        }
//
//        if (last == null) {
//          throw new NoSuchElementException();
//        }
//
//        RawChunkStore ret = last;
//
//        last = null;
//        return ret;
//      }
//
//      private void doNext() {
//        try {
//          last = fileReader.readNextRawRowGroup();
//          if (last == null) {
//            eof = true;
//          }
//        } catch (Exception e) {
//          throw new BlitzwingException(e);
//        }
//      }
//    };
//  }
}
