/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.iceberg.writer;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FindFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hive.HiveMetastoreTest;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.hive.HivePartition;
import org.apache.gobblin.hive.HiveTable;
import org.apache.gobblin.hive.policy.HiveRegistrationPolicyBase;
import org.apache.gobblin.metadata.DataFile;
import org.apache.gobblin.metadata.DataMetrics;
import org.apache.gobblin.metadata.DataOrigin;
import org.apache.gobblin.metadata.DatasetIdentifier;
import org.apache.gobblin.metadata.GobblinMetadataChangeEvent;
import org.apache.gobblin.metadata.OperationType;
import org.apache.gobblin.metadata.SchemaSource;
import org.apache.gobblin.metrics.kafka.KafkaSchemaRegistry;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaPartition;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaStreamTestUtils;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaStreamingExtractor;
import org.apache.gobblin.stream.RecordEnvelope;
import org.apache.gobblin.util.ClustersNames;
import org.apache.gobblin.util.ConfigUtils;


public class IcebergMetadataWriterTest extends HiveMetastoreTest {

  org.apache.avro.Schema avroDataSchema = SchemaBuilder.record("test")
      .fields()
      .name("id")
      .type()
      .longType()
      .noDefault()
      .name("data")
      .type()
      .optional()
      .stringType()
      .endRecord();
  org.apache.avro.Schema _avroPartitionSchema;
  private String dbName = "hivedb";

  private GobblinMCEWriter gobblinMCEWriter;

  GobblinMetadataChangeEvent gmce;
  static File tmpDir;
  static File dataDir;
  static File hourlyDataFile_2;
  static File hourlyDataFile_1;
  static File dailyDataFile;

  @AfterClass
  public void clean() throws Exception {
    gobblinMCEWriter.close();
    FileUtils.forceDeleteOnExit(tmpDir);
  }
  @BeforeClass
  public void setUp() throws Exception {
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
    startMetastore();
    State state = ConfigUtils.configToState(ConfigUtils.propertiesToConfig(hiveConf.getAllProperties()));
    tmpDir = Files.createTempDir();
    hourlyDataFile_1 = new File(tmpDir, "data/tracking/testIcebergTable/hourly/2020/03/17/08/data.avro");
    Files.createParentDirs(hourlyDataFile_1);
    hourlyDataFile_2 = new File(tmpDir, "data/tracking/testIcebergTable/hourly/2020/03/17/09/data.avro");
    Files.createParentDirs(hourlyDataFile_2);
    dailyDataFile = new File(tmpDir, "data/tracking/testIcebergTable/daily/2020/03/17/data.avro");
    Files.createParentDirs(dailyDataFile);
    dataDir = new File(hourlyDataFile_1.getParent());
    Assert.assertTrue(dataDir.exists());
    writeRecord(hourlyDataFile_1);
    writeRecord(hourlyDataFile_2);
    writeRecord(dailyDataFile);
    gmce = GobblinMetadataChangeEvent.newBuilder()
        .setDatasetIdentifier(DatasetIdentifier.newBuilder()
            .setDataOrigin(DataOrigin.EI)
            .setDataPlatformUrn("urn:li:dataPlatform:hdfs")
            .setNativeName(new File(tmpDir, "data/tracking/testIcebergTable").getAbsolutePath())
            .build())
        .setTopicPartitionOffsetsRange(ImmutableMap.<String, String>builder().put("testTopic-1", "0-1000").build())
        .setFlowId("testFlow")
        .setNewFiles(Lists.newArrayList(DataFile.newBuilder()
            .setFilePath(hourlyDataFile_1.toString())
            .setFileFormat("avro")
            .setFileMetrics(DataMetrics.newBuilder().setRecordCount(10L).build())
            .build()))
        .setSchemaSource(SchemaSource.EVENT)
        .setOperationType(OperationType.add_files)
        .setTableSchema(avroDataSchema.toString())
        .setCluster(ClustersNames.getInstance().getClusterName())
        .setPartitionColumns(Lists.newArrayList("testpartition"))
        .setRegistrationPolicy(TestHiveRegistrationPolicyForIceberg.class.getName())
        .setRegistrationProperties(ImmutableMap.<String, String>builder().put("hive.database.name", dbName).build())
        .build();
    state.setProp(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_CLASS,
        KafkaStreamTestUtils.MockSchemaRegistry.class.getName());
    state.setProp("default.hive.registration.policy",
        TestHiveRegistrationPolicyForIceberg.class.getName());
    state.setProp("use.data.path.as.table.location", true);
    gobblinMCEWriter = new GobblinMCEWriter(new GobblinMCEWriterBuilder(), state);
    ((IcebergMetadataWriter) gobblinMCEWriter.getMetadataWriters().iterator().next()).setCatalog(
        HiveMetastoreTest.catalog);
    _avroPartitionSchema =
        SchemaBuilder.record("partitionTest").fields().name("ds").type().optional().stringType().endRecord();
  }

  @Test ( priority = 0 )
  public void testWriteAddFileGMCE() throws IOException {
    gobblinMCEWriter.writeEnvelope(new RecordEnvelope<>(gmce,
        new KafkaStreamingExtractor.KafkaWatermark(
            new KafkaPartition.Builder().withTopicName("GobblinMetadataChangeEvent_test").withId(1).build(),
            new LongWatermark(10L))));
    Assert.assertEquals(catalog.listTables(Namespace.of(dbName)).size(), 1);
    Table table = catalog.loadTable(catalog.listTables(Namespace.of(dbName)).get(0));
    Assert.assertFalse(table.properties().containsKey("offset.range.testTopic-1"));
    Assert.assertEquals(table.location(),
        new File(tmpDir, "data/tracking/testIcebergTable/_iceberg_metadata/").getAbsolutePath() + "/" + dbName);
    gmce.setTopicPartitionOffsetsRange(ImmutableMap.<String, String>builder().put("testTopic-1", "1000-2000").build());
    gobblinMCEWriter.writeEnvelope(new RecordEnvelope<>(gmce,
        new KafkaStreamingExtractor.KafkaWatermark(
            new KafkaPartition.Builder().withTopicName("GobblinMetadataChangeEvent_test").withId(1).build(),
            new LongWatermark(20L))));
    gobblinMCEWriter.flush();
    table = catalog.loadTable(catalog.listTables(Namespace.of(dbName)).get(0));
    Assert.assertEquals(table.properties().get("offset.range.testTopic-1"), "0-2000");
    Assert.assertEquals(table.currentSnapshot().allManifests().size(), 1);
    // Assert low watermark and high watermark set properly
    Assert.assertEquals(table.properties().get("gmce.low.watermark.GobblinMetadataChangeEvent_test-1"), "9");
    Assert.assertEquals(table.properties().get("gmce.high.watermark.GobblinMetadataChangeEvent_test-1"), "20");

    /*test flush twice*/
    gmce.setTopicPartitionOffsetsRange(ImmutableMap.<String, String>builder().put("testTopic-1", "2000-3000").build());
    gmce.setNewFiles(Lists.newArrayList(DataFile.newBuilder()
        .setFilePath(hourlyDataFile_2.toString())
        .setFileFormat("avro")
        .setFileMetrics(DataMetrics.newBuilder().setRecordCount(10L).build())
        .build()));
    gobblinMCEWriter.writeEnvelope(new RecordEnvelope<>(gmce,
        new KafkaStreamingExtractor.KafkaWatermark(
            new KafkaPartition.Builder().withTopicName("GobblinMetadataChangeEvent_test").withId(1).build(),
            new LongWatermark(30L))));
    gobblinMCEWriter.flush();
    table = catalog.loadTable(catalog.listTables(Namespace.of(dbName)).get(0));
    Assert.assertEquals(table.properties().get("offset.range.testTopic-1"), "0-3000");
    Assert.assertEquals(table.currentSnapshot().allManifests().size(), 2);
    Assert.assertEquals(table.properties().get("gmce.low.watermark.GobblinMetadataChangeEvent_test-1"), "20");
    Assert.assertEquals(table.properties().get("gmce.high.watermark.GobblinMetadataChangeEvent_test-1"), "30");

    /* Test it will skip event with lower watermark*/
    gmce.setTopicPartitionOffsetsRange(ImmutableMap.<String, String>builder().put("testTopic-1", "3000-4000").build());
    gobblinMCEWriter.writeEnvelope(new RecordEnvelope<>(gmce,
        new KafkaStreamingExtractor.KafkaWatermark(
            new KafkaPartition.Builder().withTopicName("GobblinMetadataChangeEvent_test").withId(1).build(),
            new LongWatermark(30L))));
    gobblinMCEWriter.flush();
    table = catalog.loadTable(catalog.listTables(Namespace.of(dbName)).get(0));
    Assert.assertEquals(table.properties().get("offset.range.testTopic-1"), "0-3000");
    Assert.assertEquals(table.currentSnapshot().allManifests().size(), 2);
  }

  //Make sure hive test execute later and close the metastore
  @Test( priority = 1 )
  public void testWriteRewriteFileGMCE() throws IOException {
    gmce.setTopicPartitionOffsetsRange(null);
    FileSystem fs = FileSystem.get(new Configuration());
    String filePath = new Path(hourlyDataFile_1.getParentFile().getAbsolutePath()).toString();
    String filePath_1 = new Path(hourlyDataFile_2.getParentFile().getAbsolutePath()).toString();
    DataFile dailyFile = DataFile.newBuilder()
        .setFilePath(dailyDataFile.toString())
        .setFileFormat("avro")
        .setFileMetrics(DataMetrics.newBuilder().setRecordCount(10L).build())
        .build();
    gmce.setNewFiles(Lists.newArrayList(dailyFile));
    gmce.setOldFilePrefixes(Lists.newArrayList(filePath, filePath_1));
    gmce.setOperationType(OperationType.rewrite_files);
    Table table = catalog.loadTable(catalog.listTables(Namespace.of(dbName)).get(0));
    Iterator<org.apache.iceberg.DataFile>
        result = FindFiles.in(table).withMetadataMatching(Expressions.startsWith("file_path", filePath_1)).collect().iterator();
    Assert.assertEquals(table.currentSnapshot().allManifests().size(), 2);
    Assert.assertTrue(result.hasNext());
    gobblinMCEWriter.writeEnvelope(new RecordEnvelope<>(gmce,
        new KafkaStreamingExtractor.KafkaWatermark(
            new KafkaPartition.Builder().withTopicName("GobblinMetadataChangeEvent_test").withId(1).build(),
            new LongWatermark(40L))));
    gobblinMCEWriter.flush();
    table = catalog.loadTable(catalog.listTables(Namespace.of(dbName)).get(0));
    String dailyFilePath = new Path(dailyDataFile.toString()).toString();
    result = FindFiles.in(table).withMetadataMatching(Expressions.startsWith("file_path", dailyFilePath)).collect().iterator();
    Assert.assertEquals(result.next().path(), dailyFilePath);
    Assert.assertFalse(result.hasNext());
    result = FindFiles.in(table).withMetadataMatching(Expressions.startsWith("file_path", filePath)).collect().iterator();
    Assert.assertFalse(result.hasNext());
    result = FindFiles.in(table).withMetadataMatching(Expressions.startsWith("file_path", filePath_1)).collect().iterator();
    Assert.assertFalse(result.hasNext());
  }

  @Test( priority = 2 )
  public void testChangeProperty() throws IOException {
    Table table = catalog.loadTable(catalog.listTables(Namespace.of(dbName)).get(0));
    Assert.assertEquals(table.properties().get("offset.range.testTopic-1"), "0-3000");
    Assert.assertEquals(table.currentSnapshot().allManifests().size(), 3);
    Assert.assertEquals(table.properties().get("gmce.low.watermark.GobblinMetadataChangeEvent_test-1"), "30");
    Assert.assertEquals(table.properties().get("gmce.high.watermark.GobblinMetadataChangeEvent_test-1"), "40");

    gmce.setOldFilePrefixes(null);
    DataFile dailyFile = DataFile.newBuilder()
        .setFilePath(dailyDataFile.toString())
        .setFileFormat("avro")
        .setFileMetrics(DataMetrics.newBuilder().setRecordCount(0L).build())
        .build();
    gmce.setNewFiles(Lists.newArrayList(dailyFile));
    gmce.setOperationType(OperationType.change_property);
    gmce.setTopicPartitionOffsetsRange(ImmutableMap.<String, String>builder().put("testTopic-1", "2000-4000").build());
    gobblinMCEWriter.writeEnvelope(new RecordEnvelope<>(gmce,
        new KafkaStreamingExtractor.KafkaWatermark(
            new KafkaPartition.Builder().withTopicName("GobblinMetadataChangeEvent_test").withId(1).build(),
            new LongWatermark(45L))));
    gobblinMCEWriter.flush();
    table = catalog.loadTable(catalog.listTables(Namespace.of(dbName)).get(0));
    // Assert the offset has been updated
    Assert.assertEquals(table.properties().get("offset.range.testTopic-1"), "0-4000");
    Assert.assertEquals(table.currentSnapshot().allManifests().size(), 3);
    // Assert low watermark and high watermark set properly
    Assert.assertEquals(table.properties().get("gmce.low.watermark.GobblinMetadataChangeEvent_test-1"), "40");
    Assert.assertEquals(table.properties().get("gmce.high.watermark.GobblinMetadataChangeEvent_test-1"), "45");
  }

  private String writeRecord(File file) throws IOException {
    GenericData.Record record = new GenericData.Record(avroDataSchema);
    record.put("id", 1L);
    record.put("data", "data");
    String path = file.toString();
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>();
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(avroDataSchema, file);
    dataFileWriter.append(record);
    dataFileWriter.close();
    return path;
  }

  public static class TestHiveRegistrationPolicyForIceberg extends HiveRegistrationPolicyBase {

    public TestHiveRegistrationPolicyForIceberg(State props) throws IOException {
      super(props);
    }
    protected Optional<HivePartition> getPartition(Path path, HiveTable table) throws IOException {
      String partitionValue = "";
      if (path.toString().contains("hourly/2020/03/17/08")) {
        partitionValue = "2020-03-17-08";
      } else if (path.toString().contains("hourly/2020/03/17/09")) {
        partitionValue = "2020-03-17-09";
      } else if (path.toString().contains("daily/2020/03/17")) {
        partitionValue = "2020-03-17-00";
      }
      return Optional.of(new HivePartition.Builder().withPartitionValues(Lists.newArrayList(partitionValue))
          .withDbName("hivedb").withTableName("testIcebergTable").build());
    }
    protected Iterable<String> getDatabaseNames(Path path) {
      return Lists.newArrayList("hivedb");
    }
    protected List<String> getTableNames(Optional<String> dbPrefix, Path path) {
      return Lists.newArrayList("testIcebergTable");
    }
  }
}

