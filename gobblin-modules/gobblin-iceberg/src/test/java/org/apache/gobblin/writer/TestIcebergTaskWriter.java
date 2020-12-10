package org.apache.gobblin.writer;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.*;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.avro.*;

import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.testng.Assert;
import org.testng.annotations.*;
import org.apache.commons.io.FileUtils;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestIcebergTaskWriter {

  private FileFormat format;
  private boolean partitioned;
  private static final Configuration CONF = new Configuration();
  private static final long TARGET_FILE_SIZE = 128 * 1024 * 1024;
  private File folder;
  private String path;
  private Table table;

//  public TestIcebergTaskWriter(String format, boolean partitioned) throws IOException {
//    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
//    this.partitioned = partitioned;
//  }

  @BeforeSuite
  public void before() throws IOException {
    format = FileFormat.valueOf(("AVRO"));
    partitioned = false;
    folder = new File("target/dummydir");

    folder.deleteOnExit();
    path = folder.getAbsolutePath();

    // Construct the iceberg table with the specified file format.
    Map<String, String> props = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name());
    table = IcebergTestUtil.createTable(path, props, partitioned);
  }

  @AfterSuite
  public void after() throws IOException {
    FileUtils.deleteDirectory(folder);
  }

  @Test
  public void testWriteZeroRecord() throws IOException {
    TaskWriter taskWriter = createTaskWriter(TARGET_FILE_SIZE);
    taskWriter.close();

    DataFile[] dataFiles = taskWriter.complete();
    Assert.assertNotNull(dataFiles);
    Assert.assertEquals(0, dataFiles.length);

    // Close again.
    taskWriter.close();
    dataFiles = taskWriter.complete();
    Assert.assertNotNull(dataFiles);
    Assert.assertEquals(0, dataFiles.length);
  }

  @Test
  public void testCloseTwice() throws IOException {
    try (TaskWriter taskWriter = createTaskWriter(TARGET_FILE_SIZE)) {
      List<GenericRecord> recordList = IcebergTestUtil.genericAvroRecords();
      Schema schema = new Schema(
          required(0, "id", Types.LongType.get()),
          required(1, "data", Types.StringType.get()));
      List<GenericData.Record> expected = RandomAvroData.generate(schema, 100, 0L);
      for (GenericRecord genericRecord : recordList) {
        taskWriter.write(genericRecord);
      }
      taskWriter.close(); // The first close
      taskWriter.close(); // The second close

      int expectedFiles = partitioned ? 2 : 1;
      DataFile[] dataFiles = taskWriter.complete();
      Assert.assertEquals(expectedFiles, dataFiles.length);

      FileSystem fs = FileSystem.get(CONF);
      for (DataFile dataFile : dataFiles) {
        Assert.assertTrue(fs.exists(new Path(dataFile.path().toString())));
      }
    }
  }

  @Test
  public void testCompleteFiles() throws IOException {
    TaskWriter taskWriter = createTaskWriter(TARGET_FILE_SIZE);
    org.apache.avro.Schema avroSchema = IcebergTestUtil.generateAvroSchema();
    List<GenericRecord> recordList = IcebergTestUtil.genericAvroRecords();
    for (GenericRecord genericRecord : recordList) {
      taskWriter.write(genericRecord);
    }
      DataFile[] dataFiles = taskWriter.complete();
      int expectedFiles = partitioned ? 4 : 1;
      Assert.assertEquals(expectedFiles, dataFiles.length);

      dataFiles = taskWriter.complete();
      Assert.assertEquals(expectedFiles, dataFiles.length);

      FileSystem fs = FileSystem.get(CONF);
      for (DataFile dataFile : dataFiles) {
        Assert.assertTrue(fs.exists(new Path(dataFile.path().toString())));
      }

      AppendFiles appendFiles = table.newAppend();
      for (DataFile dataFile : dataFiles) {
        appendFiles.appendFile(dataFile);
      }
      appendFiles.commit();

      // Assert the data rows.
    IcebergTestUtil.assertTableRecords(path, Lists.newArrayList(
        IcebergTestUtil.createRecord(1, "alice"),
        IcebergTestUtil.createRecord(2, "bob")
      ));

  }

  private TaskWriter createTaskWriter(long targetFileSize) {
    IcebergTaskWriterFactory taskWriterFactory = new IcebergTaskWriterFactory(table.schema(),
        table.spec(), table.locationProvider(), table.io(), table.encryption(),
        targetFileSize, format, table.properties());
    taskWriterFactory.initialize(1, 1);
    return taskWriterFactory.create();
  }

}
