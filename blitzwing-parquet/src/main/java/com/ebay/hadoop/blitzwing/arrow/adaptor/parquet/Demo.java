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
  public static void main(String[] args) {

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
    fields.put("user_id", MinorType.BIGINT);
    fields.put("watch_site_id", MinorType.INT);
    fields.put("session_skey", MinorType.BIGINT);
    fields.put("session_start_dt", MinorType.INT);
    fields.put("seqnum", MinorType.INT);
    fields.put("soj_site_id", MinorType.INT);
    fields.put("cobrand", MinorType.INT);
    fields.put("source_impr_id", MinorType.BIGINT);
    fields.put("agent_id", MinorType.BIGINT);
    fields.put("watch_page_id", MinorType.INT);
    fields.put("soj_watch_flags64", MinorType.BIGINT);
    fields.put("has_dw_rec", MinorType.INT);
    fields.put("bot_ind", MinorType.INT);
    fields.put("del_session_skey", MinorType.BIGINT);
    fields.put("del_session_start_dt", MinorType.INT);
    fields.put("del_seqnum", MinorType.INT);
    fields.put("del_site_id", MinorType.INT);

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
