package com.ebay.hadoop.blitzwing.arrow.adaptor.parquet;

import com.ebay.hadoop.blitzwing.exception.BlitzwingException;
import com.ebay.hadoop.blitzwing.generated.arrow.adaptor.parquet.ParquetProtoOuter.ColumnDescProto;
import com.ebay.hadoop.blitzwing.generated.arrow.adaptor.parquet.ParquetProtoOuter.ParquetProto.PhysicalType;
import com.ebay.hadoop.blitzwing.generated.arrow.adaptor.parquet.ParquetProtoOuter.ParquetReaderProto;
import com.ebay.hadoop.blitzwing.utils.JniUtils;
import com.google.protobuf.ByteString;
import java.io.IOException;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.ParquetFileReader;

public class ParquetArrowReaderOptions {
  private final int batchSize;
  private final Schema schema;
  private final ParquetFileReader fileReader;
  private final JniWrapper jniWrapper;

  private ParquetArrowReaderOptions(int batchSize, Schema schema,
      ParquetFileReader fileReader,
      JniWrapper jniWrapper) {
    this.batchSize = batchSize;
    this.schema = schema;
    this.fileReader = fileReader;
    this.jniWrapper = jniWrapper;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public Schema getSchema() {
    return schema;
  }

  public ParquetFileReader getFileReader() {
    return fileReader;
  }

  public JniWrapper getJniWrapper() {
    return jniWrapper;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private int batchSize = 1024;
    private Schema schema;
    private ParquetFileReader fileReader;

    public Builder withBatchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public Builder withSchema(Schema schema) {
      this.schema = schema;
      return this;
    }

    public Builder withFileReader(ParquetFileReader fileReader) {
      this.fileReader = fileReader;
      return this;
    }

    public ParquetArrowReaderOptions build() {
      Preconditions.checkNotNull(schema, "Schema not set yet!");
      Preconditions.checkNotNull(fileReader, "File reader not set yet!");
      Preconditions.checkState(batchSize > 0, "Batch size must be positive!");

      return new ParquetArrowReaderOptions(batchSize, schema,fileReader, createJniWrapper());
    }

    private JniWrapper createJniWrapper() {
      try {
        JniUtils.loadLibraryFromJar("blitzwing_rs");
        return new JniWrapper(JniWrapper.newInstance(toParquetReaderProto().toByteArray()));
      } catch (IOException e) {
        throw new BlitzwingException(e);
      }
    }

    private ParquetReaderProto toParquetReaderProto() {
      ParquetReaderProto.Builder builder = ParquetReaderProto.newBuilder()
          .setBatchSize(batchSize)
          .setSchema(ByteString.copyFrom(schema.toByteArray()));

      for (Field field : schema.getFields()) {
        ColumnDescriptor c = fileReader.getFileMetaData().getSchema().getColumnDescription(new String[] { field.getName() });

        ColumnDescProto columnDesc = ColumnDescProto.newBuilder()
            .setColumnName(field.getName())
            .setMaxDefLevel(c.getMaxDefinitionLevel())
            .setTypeLength(c.getTypeLength())
            .setPhysicalType(PhysicalType.valueOf(c.getType().name()))
            .build();

        builder.addColumnDesc(columnDesc);
      }

      return builder.build();
    }
  }
}
