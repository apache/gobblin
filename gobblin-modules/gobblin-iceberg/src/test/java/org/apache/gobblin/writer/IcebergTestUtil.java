package org.apache.gobblin.writer;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.testng.Assert;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.iceberg.hadoop.HadoopOutputFile.fromPath;

public class IcebergTestUtil {

  public static final Schema SCHEMA = new Schema(
      Types.NestedField.optional(1, "id", Types.IntegerType.get()),
      Types.NestedField.optional(2, "data", Types.StringType.get())
  );

  public static final Record RECORD = GenericRecord.create(SCHEMA);

  public static Table createTable(String path, Map<String, String> properties, boolean partitioned) {
    PartitionSpec spec;
    if (partitioned) {
      spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    } else {
      spec = PartitionSpec.unpartitioned();
    }
    return new HadoopTables().create(SCHEMA, spec, properties, path);
  }

  public static Record createRecord(Integer id, String data) {
    Record record = RECORD.copy();
    record.setField("id", id);
    record.setField("data", data);
    return record;
  }

  public static void assertTableRecords(Table table, List<Record> expected) throws IOException {
    table.refresh();
    try (CloseableIterable<Record> iterable = IcebergGenerics.read(table).build()) {
      Assert.assertEquals(
          Sets.newHashSet(expected), Sets.newHashSet(iterable));
    }
  }

  public static void assertTableRecords(String tablePath, List<Record> expected) throws IOException {
    Preconditions.checkArgument(expected != null, "expected records shouldn't be null");
    assertTableRecords(new HadoopTables().load(tablePath), expected);
  }

  public static org.apache.avro.Schema generateAvroSchema() {
    org.apache.avro.Schema avroSchema = (new org.apache.avro.Schema.Parser()).parse("{\n" +
        "  \"namespace\": \"com.linkedin.orc\",\n" +
        "  \"type\": \"record\",\n" +
        "  \"name\": \"IcebergTest\",\n" +
        "  \"fields\": [\n" +
        "    {\n" +
        "      \"name\": \"id\",\n" +
        "      \"type\": \"int\"\n" +
        "    },\n" +
        "    {\n" +
        "      \"name\": \"data\",\n" +
        "      \"type\": \"string\"\n" +
        "    }\n" +
        "  ]\n" +
        "}\n");
    return avroSchema;
  }

  public static List<org.apache.avro.generic.GenericRecord> genericAvroRecords() throws IOException {
    List<org.apache.avro.generic.GenericRecord> list = new ArrayList<>();
    org.apache.avro.Schema avroSchema = generateAvroSchema();

    GenericRecordBuilder builder_0 = new GenericRecordBuilder(avroSchema);
    builder_0.set("id", 1);
    builder_0.set("data", "alice");
    list.add(builder_0.build());

    GenericRecordBuilder builder_1 = new GenericRecordBuilder(avroSchema);
    builder_1.set("id", 2);
    builder_1.set("data", "bob");
    list.add(builder_1.build());
    return list;
  }



  public static List<org.apache.avro.generic.GenericRecord> deserializeAvroRecords(org.apache.avro.Schema schema, String schemaPath)
      throws IOException {
    List<org.apache.avro.generic.GenericRecord> records = new ArrayList<>();

    GenericDatumReader<org.apache.avro.generic.GenericRecord> reader = new GenericDatumReader<>(schema);
    File file = new File(schemaPath);

    InputStream dataInputStream = new FileInputStream(file);
    Decoder decoder = DecoderFactory.get().jsonDecoder(schema, dataInputStream);
    org.apache.avro.generic.GenericRecord recordContainer = reader.read(null, decoder);
    ;
    try {
      while (recordContainer != null) {
        records.add(recordContainer);
        recordContainer = reader.read(null, decoder);
      }
    } catch (IOException ioe) {
      dataInputStream.close();
    }
    return records;
  }
}
