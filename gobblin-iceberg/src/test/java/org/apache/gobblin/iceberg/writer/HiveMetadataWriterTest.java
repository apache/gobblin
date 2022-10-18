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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import java.util.Map;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.hive.metastore.HiveMetaStoreBasedRegister;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.iceberg.hive.HiveMetastoreTest;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.thrift.TException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.hive.HiveMetastoreClientPool;
import org.apache.gobblin.hive.HivePartition;
import org.apache.gobblin.hive.HiveRegister;
import org.apache.gobblin.hive.HiveRegistrationUnit;
import org.apache.gobblin.hive.HiveTable;
import org.apache.gobblin.hive.policy.HiveRegistrationPolicyBase;
import org.apache.gobblin.hive.writer.HiveMetadataWriter;
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


public class HiveMetadataWriterTest extends HiveMetastoreTest {

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
  private String dedupedDbName = "hivedb_deduped";
  private String tableName = "testTable";

  private GobblinMCEWriter gobblinMCEWriter;

  GobblinMetadataChangeEvent gmce;
  static File tmpDir;
  static File dataDir;
  static File hourlyDataFile_2;
  static File hourlyDataFile_1;
  static File dailyDataFile;
  HiveMetastoreClientPool hc;
  IMetaStoreClient client;
  private static TestHiveMetastore testHiveMetastore;

  @AfterClass
  public void clean() throws Exception {
    //Finally stop the metaStore
    stopMetastore();
    gobblinMCEWriter.close();
    FileUtils.forceDeleteOnExit(tmpDir);
  }
  @BeforeClass
  public void setUp() throws Exception {
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
    startMetastore();
    State state = ConfigUtils.configToState(ConfigUtils.propertiesToConfig(hiveConf.getAllProperties()));
    Optional<String> metastoreUri = Optional.fromNullable(state.getProperties().getProperty(HiveRegister.HIVE_METASTORE_URI_KEY));
    hc = HiveMetastoreClientPool.get(state.getProperties(), metastoreUri);
    client = hc.getClient().get();
    tmpDir = Files.createTempDir();
    try {
      client.getDatabase(dbName);
    } catch (NoSuchObjectException e) {
      client.createDatabase(
          new Database(dbName, "database", tmpDir.getAbsolutePath() + "/metastore", Collections.emptyMap()));
    }
    try {
      client.getDatabase(dedupedDbName);
    } catch (NoSuchObjectException e) {
      client.createDatabase(
          new Database(dedupedDbName, "dedupeddatabase", tmpDir.getAbsolutePath() + "/metastore_deduped", Collections.emptyMap()));
    }
    hourlyDataFile_1 = new File(tmpDir, "testDB/testTable/hourly/2020/03/17/08/data.avro");
    Files.createParentDirs(hourlyDataFile_1);
    hourlyDataFile_2 = new File(tmpDir, "testDB/testTable/hourly/2020/03/17/09/data.avro");
    Files.createParentDirs(hourlyDataFile_2);
    dailyDataFile = new File(tmpDir, "testDB/testTable/daily/2020/03/17/data.avro");
    Files.createParentDirs(dailyDataFile);
    dataDir = new File(hourlyDataFile_1.getParent());
    Assert.assertTrue(dataDir.exists());
    writeRecord(hourlyDataFile_1);
    writeRecord(hourlyDataFile_2);
    writeRecord(dailyDataFile);
    Map<String, String> registrationState = new HashMap();
    registrationState.put("hive.database.name", dbName);
    gmce = GobblinMetadataChangeEvent.newBuilder()
        .setDatasetIdentifier(DatasetIdentifier.newBuilder()
            .setDataOrigin(DataOrigin.EI)
            .setDataPlatformUrn("urn:namespace:dataPlatform:hdfs")
            .setNativeName("/testDB/testTable")
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
        .setRegistrationPolicy(TestHiveRegistrationPolicy.class.getName())
        .setRegistrationProperties(registrationState)
        .setAllowedMetadataWriters(Collections.singletonList(HiveMetadataWriter.class.getName()))
        .build();
    state.setProp(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_CLASS,
        KafkaStreamTestUtils.MockSchemaRegistry.class.getName());
    state.setProp("default.hive.registration.policy",
        TestHiveRegistrationPolicy.class.getName());
    state.setProp("gmce.metadata.writer.classes", "org.apache.gobblin.hive.writer.HiveMetadataWriter");
    gobblinMCEWriter = new GobblinMCEWriter(new GobblinMCEWriterBuilder(), state);
  }
  @Test
  public void testHiveWriteAddFileGMCE() throws IOException {
    gobblinMCEWriter.writeEnvelope(new RecordEnvelope<>(gmce,
        new KafkaStreamingExtractor.KafkaWatermark(
            new KafkaPartition.Builder().withTopicName("GobblinMetadataChangeEvent_test").withId(1).build(),
            new LongWatermark(10L))));
    gobblinMCEWriter.writeEnvelope(new RecordEnvelope<>(gmce,
        new KafkaStreamingExtractor.KafkaWatermark(
            new KafkaPartition.Builder().withTopicName("GobblinMetadataChangeEvent_test").withId(1).build(),
            new LongWatermark(20L))));
    gobblinMCEWriter.flush();


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

    //Test Hive writer can register partition
    try {
      Assert.assertTrue(client.tableExists("hivedb", "testTable"));
      Assert.assertTrue(client.getPartition("hivedb", "testTable",Lists.newArrayList("2020-03-17-09")) != null);
      Assert.assertTrue(client.getPartition("hivedb", "testTable",Lists.newArrayList("2020-03-17-08")) != null);
    } catch (TException e) {
      throw new IOException(e);
    }

  }

  @Test(dependsOnMethods = {"testHiveWriteAddFileGMCE"}, groups={"hiveMetadataWriterTest"})
  public void testHiveWriteRewriteFileGMCE() throws IOException {
    gmce.setTopicPartitionOffsetsRange(null);
    Map<String, String> registrationState = gmce.getRegistrationProperties();
    registrationState.put("additional.hive.database.names", dedupedDbName);
    registrationState.put(HiveMetaStoreBasedRegister.SCHEMA_SOURCE_DB, dbName);
    gmce.setRegistrationProperties(registrationState);
    gmce.setSchemaSource(SchemaSource.NONE);
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
    gobblinMCEWriter.writeEnvelope(new RecordEnvelope<>(gmce,
        new KafkaStreamingExtractor.KafkaWatermark(
            new KafkaPartition.Builder().withTopicName("GobblinMetadataChangeEvent_test").withId(1).build(),
            new LongWatermark(40L))));
    gobblinMCEWriter.flush();

    //Test hive writer re-write operation can de-register old partitions and register new one
    try {
      Assert.assertTrue(client.getPartition("hivedb", "testTable",Lists.newArrayList("2020-03-17-00")) != null);
      // Test additional table been registered
      Assert.assertTrue(client.tableExists(dedupedDbName, "testTable"));
    } catch (TException e) {
      throw new IOException(e);
    }
    Assert.assertThrows(new Assert.ThrowingRunnable() {
      @Override public void run() throws Throwable {
        client.getPartition("hivedb", "testTable",Lists.newArrayList("2020-03-17-08"));
      }
    });
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

  public static class TestHiveRegistrationPolicy extends HiveRegistrationPolicyBase {

    public TestHiveRegistrationPolicy(State props) throws IOException {
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
      HivePartition partition = new HivePartition.Builder().withPartitionValues(Lists.newArrayList(partitionValue))
          .withDbName(table.getDbName()).withTableName(table.getTableName()).build();
      partition.setLocation(path.toString());
      return Optional.of(partition);
    }
    @Override
    protected List<HiveTable> getTables(Path path) throws IOException {
      List<HiveTable> tables = super.getTables(path);
      for (HiveTable table : tables) {
        table.setPartitionKeys(ImmutableList.<HiveRegistrationUnit.Column>of(
            new HiveRegistrationUnit.Column("testpartition", serdeConstants.STRING_TYPE_NAME, StringUtils.EMPTY)));
        table.setLocation(tmpDir.getAbsolutePath());
      }
      return tables;
    }
    protected List<String> getTableNames(Optional<String> dbPrefix, Path path) {
      return Lists.newArrayList("testTable");
    }
  }
}

