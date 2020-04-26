package com.ebay.hadoop.blitzwing.arrow.adaptor.parquet;

import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.ebay.hadoop.blitzwing.vector.RecordBatch;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.Test;

public class ParquetArrowReaderTest {

  @Test
  public void testReadIntoArrow() throws IOException {
    List<Person> personList = new ArrayList<>();
    personList.add(new Person(null, 1));
    personList.add(new Person("Amy", null));
    personList.add(new Person("Bob", 20));
    personList.add(new Person(null, null));
    personList.add(new Person("Beep", null));

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

    // Check first batch
    assertTrue(arrowReader.hasNext());
    RecordBatch recordBatch = arrowReader.next();
    assertEquals(personList.size(), recordBatch.getRowCount());

    VarCharVector names = checkByFieldName(recordBatch.getColumns(), "name", VarCharVector.class);
    IntVector ages = checkByFieldName(recordBatch.getColumns(), "age", IntVector.class);

    List<Person> returned = new ArrayList<>();
    for (int i=0; i<recordBatch.getRowCount(); i++) {
      String name = null;
      if (!names.isNull(i)) {
        name = names.getObject(i).toString();
      }

      Integer age = null;
      if (!ages.isNull(i)) {
        age = ages.getObject(i);
      }
      returned.add(new Person(name, age));
    }

    assertEquals(personList, returned);

    assertFalse(arrowReader.hasNext());
  }

  private static <T> T checkByFieldName(List<FieldVector> vectors, String fieldName, Class<T> klass) {
    Optional<FieldVector> result = vectors.stream().filter(v -> v.getField().getName().equals(fieldName))
        .findFirst();
    assertTrue(fieldName + " not found in vectors!", result.isPresent());
    FieldVector vector = result.get();

    assertTrue(fieldName + " 's type is not " + klass.getCanonicalName(), klass.isInstance(vector));
    return klass.cast(vector);
  }

  @Test
  public void schemaConversionTest() {
    Field nameField = Field.nullable("name", MinorType.VARCHAR.getType());
    Field ageField = Field.nullable("age", MinorType.INT.getType());
    Schema arrowSchema = new Schema(Lists.newArrayList(nameField, ageField));

    byte[] serSchema = arrowSchema.toByteArray();

    Schema desSchema = Schema.deserialize(ByteBuffer.wrap(serSchema));

    assertEquals(arrowSchema, desSchema);
  }
}