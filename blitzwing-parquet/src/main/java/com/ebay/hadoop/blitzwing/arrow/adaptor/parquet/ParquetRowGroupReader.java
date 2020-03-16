package com.ebay.hadoop.blitzwing.arrow.adaptor.parquet;

import java.io.IOException;
import java.util.Iterator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.hadoop.blitzwing.RawChunkStore;

class ParquetRowGroupReader extends ArrowReader {
  private final ParquetReaderOptions options;
  private final Iterator<RawChunkStore> rawChunkStores;
  private final JniWrapper jni;

  protected ParquetRowGroupReader(Iterator<RawChunkStore> rawChunkStores, BufferAllocator allocator,
      ParquetReaderOptions options, JniWrapper jni) {
    super(allocator);
    this.rawChunkStores = rawChunkStores;
    this.options = options;
    this.jni = jni;
  }

  @Override
  public boolean loadNextBatch() throws IOException {
    return false;
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
}
