package org.apache.gobblin.test.generator.json;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Test;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

import org.apache.gobblin.test.generator.FixedSchemaStructGeneratorTestBase;
import org.apache.gobblin.test.generator.FixedStructValueGenerator;
import org.apache.gobblin.test.generator.config.FixedSchemaGeneratorConfig;


public class JsonFixedSchemaStructGeneratorTest extends FixedSchemaStructGeneratorTestBase<JsonFixedSchemaStructGenerator> {

  @Override
  protected FixedStructValueGenerator constructGenerator(FixedSchemaGeneratorConfig config) {
    return new JsonFixedSchemaStructGenerator(config);
  }

  @Override
  protected Writer getFileWriter(FixedStructValueGenerator generator, File file)
      throws IOException {
    return new Writer<JsonElement>() {
      private final Gson gson = new Gson();
      private final List<JsonElement> records = new ArrayList<>();
      private final FileWriter fileWriter = new FileWriter(file);
      @Override
      public void write(JsonElement record)
          throws IOException {
        records.add(record);
      }

      @Override
      public void close()
          throws IOException {
        gson.toJson(records, fileWriter);
        fileWriter.close();
      }
    };
  }

  @Override
  protected Reader getFileReader(FixedStructValueGenerator generator, File file)
      throws FileNotFoundException {
    Reader reader =new Reader() {
      private final Gson gson = new Gson();
      private final FileReader fileReader = new FileReader(file);
      private final JsonElement[] records = gson.fromJson(fileReader, JsonElement[].class);
      private int currentIndex = 0;
      @Override
      public Object read()
          throws IOException {
        if (currentIndex == records.length) {
          return null;
        }
        currentIndex = currentIndex + 1;
        return records[currentIndex-1];
      }

      @Override
      public void close()
          throws IOException {
        fileReader.close();
      }
    };
    return reader;
  }

  @Test
  public void testConstructor()
      throws IOException {
    super.testConstructor(JsonFixedSchemaStructGenerator.class);
  }


  @Test
  public void parseConfigTest()
      throws IOException {
    super.parseConfigTest();
  }

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
