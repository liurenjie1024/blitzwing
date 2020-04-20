package com.ebay.hadoop.blitzwing.arrow.adaptor.parquet;

import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY;
import static org.junit.Assert.*;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.avro.SchemaBuilderException;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.Test;

public class ParquetArrowReaderTest {

  @Test
  public void testReadIntoArrow() throws IOException {
    List<Person> personList = new ArrayList<>();
    personList.add(new Person("Alice", 1));
    personList.add(new Person("Amy", 3));
    personList.add(new Person("Bob", 20));
    personList.add(new Person("Blare", 14));
    personList.add(new Person("Beep", 5));

    Path dataFile = new Path("/tmp/demo.snappy.parquet");

    // Write as Parquet file.
    try (ParquetWriter<Person> writer = AvroParquetWriter.<Person>builder(dataFile)
        .withSchema(ReflectData.AllowNull.get().getSchema(Person.class))
        .withDataModel(ReflectData.get())
        .withConf(new Configuration())
        .withCompressionCodec(SNAPPY)
        .withWriteMode(OVERWRITE)
        .build()) {

      for (Person p: personList) {
        writer.write(p);
      }
    }

    // Read from Parquet file.
    ParquetFileReader parquetFileReader = ParquetFileReader.open(new Configuration(), dataFile);

    Field nameField = Field.nullable("name", MinorType.VARCHAR.getType());
    Field ageField = Field.nullable("age", MinorType.INT.getType());
    Schema arrowSchema = new Schema(Lists.newArrayList(nameField, ageField));

    ParquetArrowReaderOptions options = ParquetArrowReaderOptions.newBuilder()
        .withFileReader(parquetFileReader)
        .withSchema(arrowSchema)
        .build();


    ParquetArrowReader arrowReader = new ParquetArrowReader(new RootAllocator(), options);

    while (arrowReader.hasNext()) {
      System.out.println(arrowReader.next().getRowCount());
    }
  }
}