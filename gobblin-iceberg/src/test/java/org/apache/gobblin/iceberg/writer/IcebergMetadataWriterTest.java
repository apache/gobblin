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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificData;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.iceberg.FindFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hive.HiveMetastoreTest;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import org.apache.gobblin.completeness.audit.AuditCountClient;
import org.apache.gobblin.completeness.audit.AuditCountClientFactory;
import org.apache.gobblin.completeness.audit.TestAuditClientFactory;
import org.apache.gobblin.completeness.verifier.KafkaAuditCountVerifier;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.hive.HivePartition;
import org.apache.gobblin.hive.HiveRegistrationUnit;
import org.apache.gobblin.hive.HiveTable;
import org.apache.gobblin.hive.policy.HiveRegistrationPolicyBase;
import org.apache.gobblin.hive.writer.MetadataWriterKeys;
import org.apache.gobblin.hive.writer.MetadataWriter;
import org.apache.gobblin.metadata.DataFile;
import org.apache.gobblin.metadata.DataMetrics;
import org.apache.gobblin.metadata.DataOrigin;
import org.apache.gobblin.metadata.DatasetIdentifier;
import org.apache.gobblin.metadata.GobblinMetadataChangeEvent;
import org.apache.gobblin.metadata.OperationType;
import org.apache.gobblin.metadata.SchemaSource;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;
import org.apache.gobblin.metrics.kafka.KafkaSchemaRegistry;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaPartition;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaStreamTestUtils;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaStreamingExtractor;
import org.apache.gobblin.stream.RecordEnvelope;
import org.apache.gobblin.util.ClustersNames;
import org.apache.gobblin.util.ConfigUtils;
import static org.apache.gobblin.iceberg.writer.IcebergMetadataWriterConfigKeys.*;

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
  private GobblinMCEWriter gobblinMCEWriterWithCompletness;
  private GobblinMCEWriter gobblinMCEWriterWithAcceptClusters;

  GobblinMetadataChangeEvent gmce;
  static File tmpDir;
  static File dataDir;
  static File hourlyDataFile_2;
  static File hourlyDataFile_1;
  static File dailyDataFile;

  List<GobblinEventBuilder> eventsSent = new ArrayList<>();

  @AfterClass
  public void clean() throws Exception {
    gobblinMCEWriter.close();
    gobblinMCEWriterWithAcceptClusters.close();
    FileUtils.forceDeleteOnExit(tmpDir);
  }
  @BeforeClass
  public void setUp() throws Exception {
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
    startMetastore();

    tmpDir = Files.createTempDir();
    hourlyDataFile_1 = new File(tmpDir, "testDB/testIcebergTable/hourly/2020/03/17/08/data.avro");
    Files.createParentDirs(hourlyDataFile_1);
    hourlyDataFile_2 = new File(tmpDir, "testDB/testIcebergTable/hourly/2020/03/17/09/data.avro");
    Files.createParentDirs(hourlyDataFile_2);
    dailyDataFile = new File(tmpDir, "testDB/testIcebergTable/daily/2020/03/17/data.avro");
    Files.createParentDirs(dailyDataFile);
    dataDir = new File(hourlyDataFile_1.getParent());
    Assert.assertTrue(dataDir.exists());
    writeRecord(hourlyDataFile_1);
    writeRecord(hourlyDataFile_2);
    writeRecord(dailyDataFile);
    gmce = GobblinMetadataChangeEvent.newBuilder()
        .setDatasetIdentifier(DatasetIdentifier.newBuilder()
            .setDataOrigin(DataOrigin.EI)
            .setDataPlatformUrn("urn:namespace:dataPlatform:hdfs")
            .setNativeName(new File(tmpDir, "testDB/testIcebergTable").getAbsolutePath())
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
        .setAllowedMetadataWriters(Arrays.asList(IcebergMetadataWriter.class.getName()))
        .build();

    State state = getState();
    gobblinMCEWriter = new GobblinMCEWriter(new GobblinMCEWriterBuilder(), state);
    ((IcebergMetadataWriter) gobblinMCEWriter.getMetadataWriters().iterator().next()).setCatalog(
        HiveMetastoreTest.catalog);

    State stateWithCompletenessConfig = getStateWithCompletenessConfig();
    gobblinMCEWriterWithCompletness = new GobblinMCEWriter(new GobblinMCEWriterBuilder(), stateWithCompletenessConfig);
    ((IcebergMetadataWriter) gobblinMCEWriterWithCompletness.getMetadataWriters().iterator().next()).setCatalog(
        HiveMetastoreTest.catalog);

    state.setProp(GobblinMCEWriter.ACCEPTED_CLUSTER_NAMES, "randomCluster");
    gobblinMCEWriterWithAcceptClusters = new GobblinMCEWriter(new GobblinMCEWriterBuilder(), state);

    _avroPartitionSchema =
        SchemaBuilder.record("partitionTest").fields().name("ds").type().optional().stringType().endRecord();

    gobblinMCEWriter.eventSubmitter = Mockito.mock(EventSubmitter.class);
    Mockito.doAnswer(invocation -> eventsSent.add(invocation.getArgumentAt(0, GobblinEventBuilder.class)))
        .when(gobblinMCEWriter.eventSubmitter).submit(Mockito.any(GobblinEventBuilder.class));
  }

  private State getState() {
    State state = ConfigUtils.configToState(ConfigUtils.propertiesToConfig(hiveConf.getAllProperties()));
    state.setProp(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_CLASS,
        KafkaStreamTestUtils.MockSchemaRegistry.class.getName());
    state.setProp("default.hive.registration.policy",
        TestHiveRegistrationPolicyForIceberg.class.getName());
    state.setProp("use.data.path.as.table.location", true);
    return state;
  }

  private State getStateWithCompletenessConfig() {
    State state = getState();
    state.setProp(ICEBERG_NEW_PARTITION_ENABLED, true);
    state.setProp(ICEBERG_COMPLETENESS_ENABLED, true);
    state.setProp(NEW_PARTITION_KEY, "late");
    state.setProp(NEW_PARTITION_TYPE_KEY, "int");
    state.setProp(AuditCountClientFactory.AUDIT_COUNT_CLIENT_FACTORY, TestAuditClientFactory.class.getName());
    state.setProp(KafkaAuditCountVerifier.SOURCE_TIER, "gobblin");
    state.setProp(KafkaAuditCountVerifier.REFERENCE_TIERS, "producer");
    return state;
  }

  @Test(dependsOnGroups={"hiveMetadataWriterTest"})
  public void testWriteAddFileGMCE() throws IOException {
    // Creating a copy of gmce with static type in GenericRecord to work with writeEnvelop method
    // without risking running into type cast runtime error.
    GenericRecord genericGmce = GenericData.get().deepCopy(gmce.getSchema(), gmce);

    gobblinMCEWriterWithAcceptClusters.writeEnvelope(new RecordEnvelope<>(genericGmce,
        new KafkaStreamingExtractor.KafkaWatermark(
            new KafkaPartition.Builder().withTopicName("GobblinMetadataChangeEvent_test").withId(1).build(),
            new LongWatermark(10L))));
    //Test when accept clusters does not contain the gmce cluster, we will skip
    Assert.assertEquals(catalog.listTables(Namespace.of(dbName)).size(), 0);
    gobblinMCEWriter.writeEnvelope(new RecordEnvelope<>(genericGmce,
        new KafkaStreamingExtractor.KafkaWatermark(
            new KafkaPartition.Builder().withTopicName("GobblinMetadataChangeEvent_test").withId(1).build(),
            new LongWatermark(10L))));
    Assert.assertEquals(catalog.listTables(Namespace.of(dbName)).size(), 1);
    Table table = catalog.loadTable(catalog.listTables(Namespace.of(dbName)).get(0));
    Assert.assertFalse(table.properties().containsKey("offset.range.testTopic-1"));
    Assert.assertEquals(table.location(),
        new File(tmpDir, "testDB/testIcebergTable/_iceberg_metadata/").getAbsolutePath() + "/" + dbName);

    gmce.setTopicPartitionOffsetsRange(ImmutableMap.<String, String>builder().put("testTopic-1", "1000-2000").build());
    GenericRecord genericGmce_1000_2000 = GenericData.get().deepCopy(gmce.getSchema(), gmce);

    gobblinMCEWriter.writeEnvelope(new RecordEnvelope<>(genericGmce_1000_2000,
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
    GenericRecord genericGmce_2000_3000 = GenericData.get().deepCopy(gmce.getSchema(), gmce);
    gobblinMCEWriter.writeEnvelope(new RecordEnvelope<>(genericGmce_2000_3000,
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
    gobblinMCEWriter.writeEnvelope(new RecordEnvelope<>(genericGmce,
        new KafkaStreamingExtractor.KafkaWatermark(
            new KafkaPartition.Builder().withTopicName("GobblinMetadataChangeEvent_test").withId(1).build(),
            new LongWatermark(30L))));
    gobblinMCEWriter.flush();
    table = catalog.loadTable(catalog.listTables(Namespace.of(dbName)).get(0));
    Assert.assertEquals(table.properties().get("offset.range.testTopic-1"), "0-3000");
    Assert.assertEquals(table.currentSnapshot().allManifests().size(), 2);
  }

  //Make sure hive test execute later and close the metastore
  @Test(dependsOnMethods={"testWriteAddFileGMCE"}, groups={"icebergMetadataWriterTest"})
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
    GenericRecord genericGmce = GenericData.get().deepCopy(gmce.getSchema(), gmce);
    gobblinMCEWriter.writeEnvelope(new RecordEnvelope<>(genericGmce,
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

  @Test(dependsOnMethods={"testWriteRewriteFileGMCE"}, groups={"icebergMetadataWriterTest"} )
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
    GenericRecord genericGmce = GenericData.get().deepCopy(gmce.getSchema(), gmce);
    gobblinMCEWriter.writeEnvelope(new RecordEnvelope<>(genericGmce,
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

  @Test(dependsOnMethods={"testWriteAddFileGMCECompleteness"}, groups={"icebergMetadataWriterTest"})
  public void testFaultTolerant() throws Exception {
    // Set fault tolerant dataset number to be 1
    gobblinMCEWriter.setMaxErrorDataset(1);

    // Add a mock writer that always throws exception so that write will fail
    MetadataWriter mockWriter = Mockito.mock(MetadataWriter.class);
    Mockito.doThrow(new IOException("Test failure")).when(mockWriter).writeEnvelope(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    gobblinMCEWriter.metadataWriters.add(0, mockWriter);

    GobblinMetadataChangeEvent gmceWithMockWriter = SpecificData.get().deepCopy(gmce.getSchema(), gmce);
    gmceWithMockWriter.setAllowedMetadataWriters(Arrays.asList(IcebergMetadataWriter.class.getName(), mockWriter.getClass().getName()));

    GenericRecord genericGmce = GenericData.get().deepCopy(gmceWithMockWriter.getSchema(), gmceWithMockWriter);
    gobblinMCEWriter.writeEnvelope(new RecordEnvelope<>(genericGmce,
        new KafkaStreamingExtractor.KafkaWatermark(
            new KafkaPartition.Builder().withTopicName("GobblinMetadataChangeEvent_test").withId(1).build(),
            new LongWatermark(51L))));
    gobblinMCEWriter.writeEnvelope(new RecordEnvelope<>(genericGmce,
        new KafkaStreamingExtractor.KafkaWatermark(
            new KafkaPartition.Builder().withTopicName("GobblinMetadataChangeEvent_test").withId(1).build(),
            new LongWatermark(52L))));
    Assert.assertEquals(gobblinMCEWriter.getDatasetErrorMap().size(), 1);
    Assert.assertEquals(gobblinMCEWriter.getDatasetErrorMap().values().iterator().next().size(), 1);
    Assert.assertEquals(gobblinMCEWriter.getDatasetErrorMap()
        .get(new File(tmpDir, "testDB/testIcebergTable").getAbsolutePath())
        .get("hivedb.testIcebergTable").get(0).lowWatermark, 50L);
    Assert.assertEquals(gobblinMCEWriter.getDatasetErrorMap()
        .get(new File(tmpDir, "testDB/testIcebergTable").getAbsolutePath())
        .get("hivedb.testIcebergTable").get(0).highWatermark, 52L);

    // No events sent yet since the topic has not been flushed
    Assert.assertEquals(eventsSent.size(), 0);

    //We should not see exception as we have fault tolerant
    gobblinMCEWriter.flush();

    // Since this topic has been flushed, there should be an event sent for previous failure, and the table
    // should be removed from the error map
    Assert.assertEquals(eventsSent.size(), 1);
    Assert.assertEquals(eventsSent.get(0).getMetadata().get(MetadataWriterKeys.TABLE_NAME_KEY), "testIcebergTable");
    Assert.assertEquals(eventsSent.get(0).getMetadata().get(MetadataWriterKeys.GMCE_LOW_WATERMARK), "50");
    Assert.assertEquals(eventsSent.get(0).getMetadata().get(MetadataWriterKeys.GMCE_HIGH_WATERMARK), "52");
    Assert.assertEquals(gobblinMCEWriter.getDatasetErrorMap().values().iterator().next().size(), 0);

    gmceWithMockWriter.getDatasetIdentifier().setNativeName("testDB/testFaultTolerant");
    GenericRecord genericGmce_differentDb = GenericData.get().deepCopy(gmceWithMockWriter.getSchema(), gmceWithMockWriter);
    Assert.expectThrows(IOException.class, () -> gobblinMCEWriter.writeEnvelope((new RecordEnvelope<>(genericGmce_differentDb,
        new KafkaStreamingExtractor.KafkaWatermark(
            new KafkaPartition.Builder().withTopicName("GobblinMetadataChangeEvent_test").withId(1).build(),
            new LongWatermark(54L))))));

    gobblinMCEWriter.metadataWriters.remove(0);
  }

  @Test(dependsOnMethods={"testChangeProperty"}, groups={"icebergMetadataWriterTest"})
  public void testWriteAddFileGMCECompleteness() throws IOException {
    // Creating a copy of gmce with static type in GenericRecord to work with writeEnvelop method
    // without risking running into type cast runtime error.
    gmce.setOperationType(OperationType.add_files);
    File hourlyFile = new File(tmpDir, "testDB/testIcebergTable/hourly/2021/09/16/10/data.avro");
    long timestampMillis = 1631811600000L;
    Files.createParentDirs(hourlyFile);
    writeRecord(hourlyFile);
    gmce.setNewFiles(Lists.newArrayList(DataFile.newBuilder()
        .setFilePath(hourlyFile.toString())
        .setFileFormat("avro")
        .setFileMetrics(DataMetrics.newBuilder().setRecordCount(10L).build())
        .build()));
    gmce.setTopicPartitionOffsetsRange(ImmutableMap.<String, String>builder().put("testTopic-1", "3000-4000").build());
    GenericRecord genericGmce_3000_4000 = GenericData.get().deepCopy(gmce.getSchema(), gmce);
    gobblinMCEWriterWithCompletness.writeEnvelope(new RecordEnvelope<>(genericGmce_3000_4000,
        new KafkaStreamingExtractor.KafkaWatermark(
            new KafkaPartition.Builder().withTopicName("GobblinMetadataChangeEvent_test").withId(1).build(),
            new LongWatermark(50L))));

    Table table = catalog.loadTable(catalog.listTables(Namespace.of(dbName)).get(0));
    Assert.assertEquals(table.properties().get("offset.range.testTopic-1"), "0-4000");
    Assert.assertTrue(table.spec().fields().size() == 2);
    Assert.assertEquals(table.spec().fields().get(1).name(), "late");

    // Test when completeness watermark = -1 bootstrap case
    KafkaAuditCountVerifier verifier = Mockito.mock(TestAuditCountVerifier.class);
    Mockito.when(verifier.isComplete("testIcebergTable", timestampMillis - TimeUnit.HOURS.toMillis(1), timestampMillis)).thenReturn(true);
    ((IcebergMetadataWriter) gobblinMCEWriterWithCompletness.metadataWriters.iterator().next()).setAuditCountVerifier(verifier);
    gobblinMCEWriterWithCompletness.flush();
    table = catalog.loadTable(catalog.listTables(Namespace.of(dbName)).get(0));
    //completeness watermark = "2020-09-16-10"
    Assert.assertEquals(table.properties().get(TOPIC_NAME_KEY), "testIcebergTable");
    Assert.assertEquals(table.properties().get(COMPLETION_WATERMARK_TIMEZONE_KEY), "America/Los_Angeles");
    Assert.assertEquals(table.properties().get(COMPLETION_WATERMARK_KEY), String.valueOf(timestampMillis));

    Iterator<org.apache.iceberg.DataFile> dfl = FindFiles.in(table).withMetadataMatching(Expressions.startsWith("file_path", hourlyFile.getAbsolutePath())).collect().iterator();
    Assert.assertTrue(dfl.hasNext());

    // Test when completeness watermark is still "2021-09-16-10" but have a late file for "2021-09-16-09"
    File hourlyFile1 = new File(tmpDir, "testDB/testIcebergTable/hourly/2021/09/16/09/data1.avro");
    Files.createParentDirs(hourlyFile1);
    writeRecord(hourlyFile1);
    gmce.setNewFiles(Lists.newArrayList(DataFile.newBuilder()
        .setFilePath(hourlyFile1.toString())
        .setFileFormat("avro")
        .setFileMetrics(DataMetrics.newBuilder().setRecordCount(10L).build())
        .build()));
    gmce.setTopicPartitionOffsetsRange(ImmutableMap.<String, String>builder().put("testTopic-1", "4000-5000").build());
    GenericRecord genericGmce_4000_5000 = GenericData.get().deepCopy(gmce.getSchema(), gmce);
    gobblinMCEWriterWithCompletness.writeEnvelope(new RecordEnvelope<>(genericGmce_4000_5000,
        new KafkaStreamingExtractor.KafkaWatermark(
            new KafkaPartition.Builder().withTopicName("GobblinMetadataChangeEvent_test").withId(1).build(),
            new LongWatermark(55L))));
    gobblinMCEWriterWithCompletness.flush();
    table = catalog.loadTable(catalog.listTables(Namespace.of(dbName)).get(0));
    Assert.assertEquals(table.properties().get(COMPLETION_WATERMARK_KEY), String.valueOf(timestampMillis));

    dfl = FindFiles.in(table).withMetadataMatching(Expressions.startsWith("file_path", hourlyFile1.getAbsolutePath())).collect().iterator();
    Assert.assertTrue(dfl.hasNext());
    Assert.assertEquals((int) dfl.next().partition().get(1, Integer.class), 1);

    // Test when completeness watermark will advance to "2021-09-16-11"
    File hourlyFile2 = new File(tmpDir, "testDB/testIcebergTable/hourly/2021/09/16/11/data.avro");
    long timestampMillis1 = timestampMillis + TimeUnit.HOURS.toMillis(1);
    Files.createParentDirs(hourlyFile2);
    writeRecord(hourlyFile2);
    gmce.setNewFiles(Lists.newArrayList(DataFile.newBuilder()
        .setFilePath(hourlyFile2.toString())
        .setFileFormat("avro")
        .setFileMetrics(DataMetrics.newBuilder().setRecordCount(10L).build())
        .build()));
    gmce.setTopicPartitionOffsetsRange(ImmutableMap.<String, String>builder().put("testTopic-1", "5000-6000").build());
    GenericRecord genericGmce_5000_6000 = GenericData.get().deepCopy(gmce.getSchema(), gmce);
    gobblinMCEWriterWithCompletness.writeEnvelope(new RecordEnvelope<>(genericGmce_5000_6000,
        new KafkaStreamingExtractor.KafkaWatermark(
            new KafkaPartition.Builder().withTopicName("GobblinMetadataChangeEvent_test").withId(1).build(),
            new LongWatermark(60L))));

    Mockito.when(verifier.isComplete("testIcebergTable", timestampMillis1 - TimeUnit.HOURS.toMillis(1), timestampMillis1)).thenReturn(true);
    gobblinMCEWriterWithCompletness.flush();
    table = catalog.loadTable(catalog.listTables(Namespace.of(dbName)).get(0));
    Assert.assertEquals(table.properties().get(COMPLETION_WATERMARK_KEY), String.valueOf(timestampMillis1));

    dfl = FindFiles.in(table).withMetadataMatching(Expressions.startsWith("file_path", hourlyFile2.getAbsolutePath())).collect().iterator();
    Assert.assertTrue(dfl.hasNext());
    Assert.assertTrue(dfl.next().partition().get(1, Integer.class) == 0);

  }

  @Test(dependsOnMethods={"testWriteAddFileGMCECompleteness"}, groups={"icebergMetadataWriterTest"})
  public void testChangePropertyGMCECompleteness() throws IOException {

    Table table = catalog.loadTable(catalog.listTables(Namespace.of(dbName)).get(0));
    long watermark = Long.parseLong(table.properties().get(COMPLETION_WATERMARK_KEY));
    long expectedWatermark = watermark + TimeUnit.HOURS.toMillis(1);
    File hourlyFile2 = new File(tmpDir, "testDB/testIcebergTable/hourly/2021/09/16/11/data.avro");
    gmce.setOldFilePrefixes(null);
    gmce.setNewFiles(Lists.newArrayList(DataFile.newBuilder()
        .setFilePath(hourlyFile2.toString())
        .setFileFormat("avro")
        .setFileMetrics(DataMetrics.newBuilder().setRecordCount(10L).build())
        .build()));
    gmce.setOperationType(OperationType.change_property);
    gmce.setTopicPartitionOffsetsRange(ImmutableMap.<String, String>builder().put("testTopic-1", "6000-7000").build());
    GenericRecord genericGmce = GenericData.get().deepCopy(gmce.getSchema(), gmce);
    gobblinMCEWriterWithCompletness.writeEnvelope(new RecordEnvelope<>(genericGmce,
        new KafkaStreamingExtractor.KafkaWatermark(
            new KafkaPartition.Builder().withTopicName("GobblinMetadataChangeEvent_test").withId(1).build(),
            new LongWatermark(65L))));

    KafkaAuditCountVerifier verifier = Mockito.mock(TestAuditCountVerifier.class);
    Mockito.when(verifier.isComplete("testIcebergTable", watermark, expectedWatermark)).thenReturn(true);
    ((IcebergMetadataWriter) gobblinMCEWriterWithCompletness.metadataWriters.iterator().next()).setAuditCountVerifier(verifier);
    gobblinMCEWriterWithCompletness.flush();

    table = catalog.loadTable(catalog.listTables(Namespace.of(dbName)).get(0));
    Assert.assertEquals(table.properties().get("offset.range.testTopic-1"), "0-7000");
    Assert.assertEquals(table.spec().fields().get(1).name(), "late");
    Assert.assertEquals(table.properties().get(TOPIC_NAME_KEY), "testIcebergTable");
    Assert.assertEquals(table.properties().get(COMPLETION_WATERMARK_TIMEZONE_KEY), "America/Los_Angeles");
    Assert.assertEquals(table.properties().get(COMPLETION_WATERMARK_KEY), String.valueOf(expectedWatermark));

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
      } else if (path.toString().contains("hourly/2021/09/16/09")) {
        partitionValue = "2021-09-16-09";
      } else if (path.toString().contains("hourly/2021/09/16/10")) {
        partitionValue = "2021-09-16-10";
      } else if (path.toString().contains("hourly/2021/09/16/11")) {
        partitionValue = "2021-09-16-11";
      }  else if (path.toString().contains("daily/2020/03/17")) {
        partitionValue = "2020-03-17-00";
      }
      return Optional.of(new HivePartition.Builder().withPartitionValues(Lists.newArrayList(partitionValue))
          .withDbName("hivedb").withTableName("testIcebergTable").build());
    }
    @Override
    protected List<HiveTable> getTables(Path path) throws IOException {
      List<HiveTable> tables = super.getTables(path);
      for (HiveTable table : tables) {
        table.setPartitionKeys(ImmutableList.<HiveRegistrationUnit.Column>of(
            new HiveRegistrationUnit.Column("datepartition", serdeConstants.STRING_TYPE_NAME, StringUtils.EMPTY)));
        //table.setLocation(tmpDir.getAbsolutePath());
      }
      return tables;
    }
    protected Iterable<String> getDatabaseNames(Path path) {
      return Lists.newArrayList("hivedb");
    }
    protected List<String> getTableNames(Optional<String> dbPrefix, Path path) {
      if (path.toString().contains("testFaultTolerant")) {
        return Lists.newArrayList("testFaultTolerantIcebergTable");
      }
      return Lists.newArrayList("testIcebergTable");
    }
  }

  static class TestAuditCountVerifier extends KafkaAuditCountVerifier {

    public TestAuditCountVerifier(State state) {
      super(state);
    }

    public TestAuditCountVerifier(State state, AuditCountClient client) {
      super(state, client);
    }
  }
}

