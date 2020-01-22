package org.apache.gobblin.test.generator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.test.Optionality;
import org.apache.gobblin.test.generator.config.FieldConfig;
import org.apache.gobblin.test.generator.config.FixedSchemaGeneratorConfig;
import org.apache.gobblin.test.type.Type;


public abstract class FixedSchemaStructGeneratorTestBase<T extends FixedStructValueGenerator> {

  public interface Writer<T> {
    void write(T record)
        throws IOException;
    void close()
        throws IOException;
  }

  public interface Reader<T> {
    T read() throws IOException;
    void close() throws IOException;
  }

  protected abstract FixedStructValueGenerator constructGenerator(FixedSchemaGeneratorConfig config);
  /**
   * Prove that you can "consume" a bunch of different configs and not barf
   */
  public void testConstructor(Class<T> tClass)
      throws IOException {
    FixedSchemaGeneratorConfig config = new FixedSchemaGeneratorConfig();
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

    FixedStructValueGenerator generator = constructGenerator(config);
    proveWritableReadable(generator, 200);
  }

  /**
   * Prove that you can construct yourself from hocon config
   * @throws IOException
   */
  protected void parseConfigTest()
      throws IOException {
    Config foo = ConfigFactory.parseResources(getClass().getClassLoader(), "struct.conf");
    FieldConfig fieldConfig = ConfigBeanFactory.create(foo, FieldConfig.class);
    System.out.println(fieldConfig);
    FixedSchemaGeneratorConfig config = ConfigBeanFactory.create(foo, FixedSchemaGeneratorConfig.class);
    config.setFieldConfig(fieldConfig);

    for (int i = 0; i < 2; ++i) {
      FixedStructValueGenerator generator = constructGenerator(config);
      List<FieldConfig> structSchema = generator.getStructSchema();
      structSchema.stream()
          .map(field -> "LogicalField: " + field.toString())
          .forEach(System.out::println);
      /**
      Schema avroSchema = generator.getAvroSchema();
      System.out.println(avroSchema.toString(true));
       **/
      proveWritableReadable(generator, 200);

    }
  }

  private void proveWritableReadable(FixedStructValueGenerator generator, int numRecords)
      throws IOException {
      File file = new File("data.out");
      Writer writer = getFileWriter(generator, file);
      Object[] generatedRecords = new Object[numRecords];
      for (int j = 0; j < numRecords; ++j) {
        Object record = generator.get();
        writer.write(record);
        generatedRecords[j] = record;
      }
      writer.close();
      Reader reader = getFileReader(generator, file);
      for (int j=0; j < numRecords; ++j) {
        Object record = reader.read();
        System.out.println(record);
        Assert.assertEquals(generatedRecords[j], record);
      }
      reader.close();
  }
  protected abstract Writer getFileWriter(FixedStructValueGenerator generator, File file)
      throws IOException;

  protected abstract Reader getFileReader(FixedStructValueGenerator generator, File file)
      throws IOException;

  /**
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
  **/
}
