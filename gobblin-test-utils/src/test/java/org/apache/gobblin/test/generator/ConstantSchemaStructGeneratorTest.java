package org.apache.gobblin.test.generator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.test.Optionality;
import org.apache.gobblin.test.type.Type;


public class ConstantSchemaStructGeneratorTest {

  @Test
  public void testConstructor() {
    ConstantSchemaGeneratorConfig config = new ConstantSchemaGeneratorConfig();
    FieldConfig fieldConfig = new FieldConfig();
    fieldConfig.setName("intField");
    fieldConfig.setOptional(Optionality.REQUIRED);
    fieldConfig.setType(Type.Integer);
    fieldConfig.setValueGen("constant");
    fieldConfig.setValueGenConfig(ConfigFactory.empty().withValue("value", ConfigValueFactory.fromAnyRef(42)));

    List<FieldConfig> fieldConfigList = new ArrayList<FieldConfig>();
    fieldConfigList.add(fieldConfig);
    FieldConfig structConfig = new FieldConfig();
    structConfig.setName("foobar");
    structConfig.setType(Type.Struct);
    structConfig.setTypeName("structfoo");
    structConfig.setValueGen("constant");
    structConfig.setFields(fieldConfigList);

    config.setFieldConfig(structConfig);

    ConstantSchemaStructGenerator generator = new ConstantSchemaStructGenerator(config);
    GenericRecord record = generator.get();
    System.out.println(record);
  }

  @Test
  public void parseConfigTest()
      throws IOException {
    Config foo = ConfigFactory.parseResources(getClass().getClassLoader(), "struct.conf");
    FieldConfig fieldConfig = ConfigBeanFactory.create(foo, FieldConfig.class);
    System.out.println(fieldConfig);
    ConstantSchemaGeneratorConfig config = ConfigBeanFactory.create(foo, ConstantSchemaGeneratorConfig.class);
    config.setFieldConfig(fieldConfig);

    for (int i = 0; i < 2; ++i) {
      ConstantSchemaStructGenerator generator = new ConstantSchemaStructGenerator(config);
      List<Field> structSchema = generator.getStructSchema();
      structSchema.stream()
          .map(field -> "LogicalField: " + field.toString())
          .forEach(System.out::println);
      Schema avroSchema = generator.getAvroSchema();
      System.out.println(avroSchema.toString(true));
      if (i >= 0) {
        File file = new File("data" + i + ".avro");
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(generator.getAvroSchema());
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);
        dataFileWriter.create(generator.getAvroSchema(), file);
        for (int j = 0; j < 2000; ++j) {
          GenericRecord record = generator.get();
          System.out.println(record);
          dataFileWriter.append(record);
        }
        dataFileWriter.close();
      }

    }
  }

  @Test
  public void testUnionRecord()
      throws IOException {
    Schema recordSchema = SchemaBuilder.record("rec")
        .fields()
        .name("s1")
        .type(Schema.create(Schema.Type.INT))
        .noDefault()
        .name("s2")
        .type(Schema.create(Schema.Type.STRING))
        .noDefault()
        .endRecord();

    List<Schema> unionSchema = new ArrayList<Schema>(2);
    unionSchema.add(Schema.create(Schema.Type.NULL));
    unionSchema.add(recordSchema);
    Schema unionWithNullSchema = Schema.createUnion(unionSchema);

    Schema unionSchemaRecord = SchemaBuilder.record("foo")
        .fields()
        .name("bar")
        .type(unionWithNullSchema)
        .withDefault(null)
        .endRecord();

    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("s1", 1);
    record.put("s2", new Utf8("hello"));
    System.out.println(record.toString());


    GenericRecord wrapped = new GenericData.Record(unionSchemaRecord);
    wrapped.put("bar", record);

    File file = new File("data.avro");
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>();
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);
    dataFileWriter.create(unionSchemaRecord, file);
    System.out.println(wrapped);
    dataFileWriter.append(wrapped);
    dataFileWriter.close();

  }
}
