package org.apache.gobblin.test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.test.generator.DataGenerator;
import org.apache.gobblin.test.generator.config.DataGeneratorConfig;
import org.apache.gobblin.test.generator.config.FieldConfig;
import org.apache.gobblin.test.type.Type;


public class DataGeneratorTest {

  @Test
  public void testConfigLoad()
      throws ClassNotFoundException {
    Config foo = ConfigFactory.parseResources(getClass().getClassLoader(), "datagen.conf");
    Config dataGenConfig = foo.getConfig("dataGen");
    DataGeneratorConfig config = ConfigBeanFactory.create(dataGenConfig, DataGeneratorConfig.class);
    DataGenerator dataGenerator = new DataGenerator(config);
    Object schema = dataGenerator.getSchema();
    Assert.assertTrue(schema instanceof Schema);
    System.out.println("Schema:" + schema.toString());
    int i=0;
    while (dataGenerator.hasNext()) {
      Object record = dataGenerator.next();
      Assert.assertTrue(record instanceof GenericRecord);
      Assert.assertEquals(((GenericRecord) record).get("intField"), 42);
      Assert.assertNotNull(((GenericRecord) record).get("stringField"), "stringField should be present");
      System.out.println("Record " + i + ":" + record.toString());
      i++;
    }
    Assert.assertEquals(i, 20);
  }

  @Test
  public void testBuilder()
      throws ClassNotFoundException {
    FieldConfig fieldConfig = FieldConfig.builder()
        .type(Type.Struct)
        .name("testRecord")
        .valueGen("fixed")
        .field(FieldConfig.builder()
            .type(Type.Integer)
            .name("sequence")
            .valueGen("sequential")
            .build())
        .build();

    DataGeneratorConfig dataGeneratorConfig =
        DataGeneratorConfig.builder()
            .inMemoryFormat(InMemoryFormat.AVRO_GENERIC)
            .totalRecords(20)
            .fieldConfig(fieldConfig)
            .build();

    DataGenerator dataGenerator = new DataGenerator(dataGeneratorConfig);
    Object schema = dataGenerator.getSchema();
    Assert.assertTrue(schema instanceof Schema);
    System.out.println("Schema:" + schema.toString());
    int i = 0;
    while (dataGenerator.hasNext()) {
      Object record = dataGenerator.next();
      Assert.assertTrue(record instanceof GenericRecord);
      Assert.assertEquals(((GenericRecord) record).get("sequence"), i);
      //Assert.assertNotNull(((GenericRecord) record).get("stringField"), "stringField should be present");
      System.out.println("Record " + i + ":" + record.toString());
      i++;
    }
  }

  @Test
  public void testWriter()
      throws IOException {
    FieldConfig fieldConfig = FieldConfig.builder()
        .type(Type.Struct)
        .name("testRecord")
        .valueGen("random")
        .field(FieldConfig.builder().type(Type.Integer).name("sequence").valueGen("sequential").build())
        .field(FieldConfig.builder().type(Type.Enum).name("category").symbol("A").symbol("B").valueGen("random").build())
        .build();

    DataGeneratorConfig dataGeneratorConfig =
        DataGeneratorConfig.builder()
            .inMemoryFormat(InMemoryFormat.AVRO_GENERIC)
            .totalRecords(20)
            .fieldConfig(fieldConfig)
            .keepLocalCopy(true)
            .build();

    DataGenerator dataGenerator = new DataGenerator(dataGeneratorConfig);
    Object schema = dataGenerator.getSchema();
    Assert.assertTrue(schema instanceof Schema);
    System.out.println("Schema:" + schema.toString());
    File file=new File("data.avro");
    DatumWriter<GenericRecord> writer=new GenericDatumWriter<GenericRecord>((Schema) schema);
    DataFileWriter<GenericRecord> dataFileWriter=new DataFileWriter<GenericRecord>(writer);
    dataFileWriter.create((Schema) schema, file);
    int i = 0;
    while (dataGenerator.hasNext()) {
      GenericRecord record = (GenericRecord) dataGenerator.next();
      Assert.assertEquals(record.get("sequence"), i);
      Assert.assertTrue(record.get("category") instanceof GenericEnumSymbol);
      //Assert.assertNotNull(((GenericRecord) record).get("stringField"), "stringField should be present");
      System.out.println("Record " + i + ":" + record.toString());
      dataFileWriter.append(record);
      i++;
    }
    dataFileWriter.close();


    DatumReader<GenericRecord> reader=new GenericDatumReader<GenericRecord>((Schema) schema);
    DataFileReader<GenericRecord> dataFileReader=new DataFileReader<GenericRecord>(file, reader);
    Iterator<Object> replayRecords = dataGenerator.replay();
    while (dataFileReader.hasNext()) {
      GenericRecord record = dataFileReader.next();
      GenericRecord originalRecord = (GenericRecord) replayRecords.next();
      Assert.assertEquals(record, originalRecord);
      System.out.println(record);
    }
    dataFileReader.close();
  }
}
