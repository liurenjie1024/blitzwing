package com.ebay.hadoop.blitzwing.arrow.adaptor.parquet;

import com.ebay.hadoop.blitzwing.vector.RecordBatch;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;

public class Demo {
  public static void main(String[] args) throws Exception {
    ParquetFileReader parquetFileReader = ParquetFileReader.open(new Configuration(), new Path(args[1]));

    Field nameField = Field.nullable("name", MinorType.VARCHAR.getType());
    Field ageField = Field.nullable("age", MinorType.INT.getType());
    Field hairCountField = Field.nullable("hairCount", MinorType.BIGINT.getType());
    Schema arrowSchema = new Schema(Lists.newArrayList(nameField, ageField, hairCountField));

    ParquetArrowReaderOptions options = ParquetArrowReaderOptions.newBuilder()
        .withBatchSize(4096)
        .withFileReader(parquetFileReader)
        .withSchema(arrowSchema)
        .build();

    ParquetArrowReader arrowReader = new ParquetArrowReader(new RootAllocator(), options);
    while (arrowReader.hasNext()) {
      RecordBatch recordBatch = arrowReader.next();
      System.out.println("Current row count: " + recordBatch.getRowCount());
      recordBatch.close();
    }
  }
}
