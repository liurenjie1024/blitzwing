package com.ebay.hadoop.blitzwing.arrow.adaptor.parquet;

import com.ebay.hadoop.blitzwing.vector.RecordBatch;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;

public class Demo {
  public static void main(String[] args) throws Exception {

    for (int i=0; i<100; i++) {
      long current = System.nanoTime();
      try {
        run(args[0]);
      } catch (Exception e) {
        e.printStackTrace();
      }
      System.out.println("It takes " + (System.nanoTime()-current) + " nano seconds");
    }
  }

  private static void run(String filename) throws Exception {
    Configuration conf = new Configuration();
    conf.set("parquet.read.allocation.class", "com.ebay.hadoop.blitzwing.arrow.adaptor.parquet.memory.ArrowBufManager");
    ParquetFileReader parquetFileReader = ParquetFileReader.open(conf, new Path(filename));

    Map<String, MinorType> fields = new HashMap<>();
    fields.put("item_id", MinorType.BIGINT);
    fields.put("auct_end_dt", MinorType.INT);
    fields.put("variation_id", MinorType.BIGINT);

    List<Field> fieldList = new ArrayList<>(fields.size());
    for (String filedName: fields.keySet()) {
      fieldList.add(Field.nullable(filedName, fields.get(filedName).getType()));
    }

    Schema arrowSchema = new Schema(fieldList);

    ParquetArrowReaderOptions options = ParquetArrowReaderOptions.newBuilder()
        .withBatchSize(4096)
        .withFileReader(parquetFileReader)
        .withSchema(arrowSchema)
        .build();
    ParquetArrowReader arrowReader = new ParquetArrowReader(new RootAllocator(), options);
    while (arrowReader.hasNext()) {
      RecordBatch recordBatch = arrowReader.next();
      recordBatch.close();
    }
  }
}
