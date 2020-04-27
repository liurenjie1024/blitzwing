package com.ebay.hadoop.blitzwing.arrow.adaptor.parquet;

import static com.ebay.hadoop.blitzwing.arrow.adaptor.parquet.ParquetArrowReaderOptions.fromCompressionCodecName;

import com.ebay.hadoop.blitzwing.exception.BlitzwingException;
import com.ebay.hadoop.blitzwing.generated.arrow.adaptor.parquet.ParquetProtoOuter.ColumnChunkProto;
import com.ebay.hadoop.blitzwing.generated.arrow.adaptor.parquet.ParquetProtoOuter.RowGroupProto;
import com.ebay.hadoop.blitzwing.generated.arrow.adaptor.parquet.ParquetProtoOuter.SegmentProto;
import com.ebay.hadoop.blitzwing.memory.MemoryManager;
import com.ebay.hadoop.blitzwing.vector.RecordBatch;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.blitzwing.RawChunk;
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

  public RowGroupProto toRowGroupProto(RawChunkStore rawChunkStore) {
    RowGroupProto.Builder rowGroup = RowGroupProto.newBuilder();


    for (ColumnDescriptor column: rawChunkStore.getColumns()) {
      if (!options.columnExists(column.getPath()[0])) {
        continue;
      }
      ColumnChunkProto.Builder chunkProto = ColumnChunkProto.newBuilder()
          .setColumnName(column.getPath()[0])
          .setNumValues(rawChunkStore.getRowCount());

      RawChunk rawChunk = rawChunkStore.getChunk(column);
      chunkProto.setCompression(fromCompressionCodecName(rawChunk.getChunkDescriptor().getCompressionCodec()));

      for(ByteBuffer byteBuffer: rawChunk.getData()) {
        if (!byteBuffer.isDirect()) {
          throw new IllegalArgumentException("Parquet reader can only use direct byte buffer!");
        }

        SegmentProto segment = SegmentProto.newBuilder()
            .setAddress(((DirectBuffer)byteBuffer).address() + byteBuffer.position())
            .setLength(byteBuffer.remaining())
            .build();

        chunkProto.addSegments(segment);
      }

//      printRawData(rawChunk.getData());
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
      ensureInitialized();
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

  // For test
  private static void printRawData(List<ByteBuffer> rawData) {
    StringJoiner joiner = new StringJoiner(",", "[", "]");
    System.out.print("Raw data: ");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    WritableByteChannel channel = Channels.newChannel(baos);
    for (ByteBuffer byteBuffer : rawData) {
      try {
        channel.write(byteBuffer);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    byte[] data = baos.toByteArray();
    for (byte b: data) {
      joiner.add(b + "");
    }

    System.out.println(joiner.toString());
  }
}
