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
import java.util.function.Function;

import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.iceberg.hive.HiveMetastoreTest;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.thrift.TException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.data.management.copy.hive.WhitelistBlacklist;
import org.apache.gobblin.hive.HiveMetastoreClientPool;
import org.apache.gobblin.hive.HivePartition;
import org.apache.gobblin.hive.HiveRegister;
import org.apache.gobblin.hive.HiveRegistrationUnit;
import org.apache.gobblin.hive.HiveTable;
import org.apache.gobblin.hive.metastore.HiveMetaStoreBasedRegister;
import org.apache.gobblin.hive.policy.HiveRegistrationPolicyBase;
import org.apache.gobblin.hive.spec.HiveSpec;
import org.apache.gobblin.hive.spec.SimpleHiveSpec;
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
import org.apache.gobblin.util.function.CheckedExceptionFunction;

import static org.mockito.Mockito.eq;


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

  @AfterSuite
  public void clean() throws Exception {
    gobblinMCEWriter.close();
    FileUtils.forceDeleteOnExit(tmpDir);
    //Finally stop the metaStore
    stopMetastore();
  }
  @BeforeSuite
  public void setUp() throws Exception {
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
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

  /**
   * Goal: General test for de-registering a partition created in
   * {@link HiveMetadataWriterTest#testHiveWriteRewriteFileGMCE()}
   */
  @Test(dependsOnMethods = {"testHiveWriteRewriteFileGMCE"}, groups={"hiveMetadataWriterTest"})
  public void testHiveWriteDeleteFileGMCE() throws IOException, TException {
    // partitions should exist from the previous test
    Assert.assertNotNull(client.getPartition(dbName, "testTable", Lists.newArrayList("2020-03-17-00")));
    Assert.assertNotNull(client.getPartition(dedupedDbName, "testTable", Lists.newArrayList("2020-03-17-00")));

    gmce.setTopicPartitionOffsetsRange(null);
    Map<String, String> registrationState = gmce.getRegistrationProperties();
    registrationState.put("additional.hive.database.names", dedupedDbName);
    registrationState.put(HiveMetaStoreBasedRegister.SCHEMA_SOURCE_DB, dbName);
    gmce.setRegistrationProperties(registrationState);
    gmce.setSchemaSource(SchemaSource.NONE);
    gmce.setOldFilePrefixes(Lists.newArrayList(dailyDataFile.toString()));
    gmce.setOperationType(OperationType.drop_files);

    gobblinMCEWriter.writeEnvelope(new RecordEnvelope<>(gmce,
        new KafkaStreamingExtractor.KafkaWatermark(
            new KafkaPartition.Builder().withTopicName("GobblinMetadataChangeEvent_test").withId(1).build(),
            new LongWatermark(40L))));
    gobblinMCEWriter.flush();

    // Partition created in previous test should now be dropped in the DB and the additional DB
    Assert.assertThrows(NoSuchObjectException.class, () ->
        client.getPartition(dbName, "testTable",Lists.newArrayList("2020-03-17-00")));
    Assert.assertThrows(NoSuchObjectException.class, () ->
        client.getPartition(dedupedDbName, "testTable",Lists.newArrayList("2020-03-17-00")));

    // Test additional table still registered, since this operation should only drop partitions but not table
    Assert.assertTrue(client.tableExists(dedupedDbName, "testTable"));
    Assert.assertTrue(client.tableExists(dbName, "testTable"));

    // dropping a partition that does not exist anymore should be safe
    gobblinMCEWriter.writeEnvelope(new RecordEnvelope<>(gmce,
        new KafkaStreamingExtractor.KafkaWatermark(
            new KafkaPartition.Builder().withTopicName("GobblinMetadataChangeEvent_test").withId(1).build(),
            new LongWatermark(40L))));
  }

  /**
   * Goal: to ensure that errors when creating a table do not bubble up any exceptions (which would otherwise
   * cause the container to fail and metadata registration to be blocked)
   * <ul>
   *   <li>add file to non existent DB should swallow up exception</li>
   * </ul>
   */
  @Test(dependsOnMethods = {"testHiveWriteDeleteFileGMCE"}, groups={"hiveMetadataWriterTest"})
  public void testHiveWriteSwallowsExceptionOnCreateTable() throws IOException {
    // add file to a DB that does not exist should trigger an exception and the exception should be swallowed
    HiveSpec spec = new SimpleHiveSpec.Builder(new org.apache.hadoop.fs.Path("pathString"))
        .withTable(new HiveTable.Builder()
            .withDbName("dbWhichDoesNotExist")
            .withTableName("testTable")
        .build()).build();
    gmce.setOperationType(OperationType.add_files);
    HiveMetadataWriter hiveWriter = (HiveMetadataWriter) gobblinMCEWriter.getMetadataWriters().get(0);
    hiveWriter.write(gmce, null, null, spec, "someTopicPartition");
  }

  @Test(dependsOnMethods = {"testHiveWriteSwallowsExceptionOnCreateTable"}, groups={"hiveMetadataWriterTest"})
  public void testDropFilesDoesNotCreateTable() throws IOException {
    HiveMetadataWriter hiveWriter = (HiveMetadataWriter) gobblinMCEWriter.getMetadataWriters().get(0);
    HiveRegister mockRegister = Mockito.mock(HiveRegister.class);
    HiveSpec spec = new SimpleHiveSpec.Builder(new org.apache.hadoop.fs.Path("pathString"))
        .withTable(new HiveTable.Builder().withDbName("stubDB").withTableName("stubTable").build()).build();

    // Since there are no old file prefixes, there are no files to delete. And the writer shouldn't touch the hive register
    // i.e. dropping files will not create a table
    gmce.setOperationType(OperationType.drop_files);
    gmce.setOldFilePrefixes(null);
    hiveWriter.write(gmce, null, null, spec, "someTopicPartition");
    Mockito.verifyNoInteractions(mockRegister);
  }

  /**
   * Goal: Ensure the logic for always using the latest schema in Hive table is working properly:
   * <ul>
   *   <li>deny listed topics should fetch the schema once, and then use a cached version for all future calls</li>
   *   <li>allow listed topics should fetch the schema each time</li>
   * </ul>
   */
  @Test
  public void testUpdateLatestSchemaWithExistingSchema() throws IOException {
    final String tableNameAllowed = "tableAllowed";
    final String tableNameDenied = "tableDenied";
    final WhitelistBlacklist useExistingTableSchemaAllowDenyList = new WhitelistBlacklist(
        "hivedb.tableAllowed", "hivedb.tableDenied", true);
    final HiveRegister hiveRegister = Mockito.mock(HiveRegister.class);
    final HashMap<String, String> latestSchemaMap = new HashMap<>();
    final Function<String,String> getTableKey = (tableName) -> String.format("%s.%s", dbName, tableName);
    final HiveTable mockTable = Mockito.mock(HiveTable.class);
    final State avroSchemaProp = Mockito.mock(State.class);
    final String avroSchema = "avro schema";

    Mockito.when(hiveRegister.getTable(eq(dbName), eq(tableNameAllowed))).thenReturn(Optional.of(mockTable));
    Mockito.when(hiveRegister.getTable(eq(dbName), eq(tableNameDenied))).thenReturn(Optional.of(mockTable));
    Mockito.when(avroSchemaProp.getProp(eq(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName()))).thenReturn(avroSchema);
    Mockito.when(mockTable.getSerDeProps()).thenReturn(avroSchemaProp);

    CheckedExceptionFunction<String, Boolean, IOException> updateLatestSchema = (tableName) ->
        TestHiveMetadataWriter.updateLatestSchemaMapWithExistingSchema(dbName, tableName, getTableKey.apply(tableName),
            useExistingTableSchemaAllowDenyList, hiveRegister, latestSchemaMap);


    // Tables part of deny list, schema only fetched from hive on the first time and the all future calls will use the cache
    Assert.assertTrue(updateLatestSchema.apply(tableNameDenied));
    Assert.assertFalse(updateLatestSchema.apply(tableNameDenied));
    Assert.assertEquals(latestSchemaMap, ImmutableMap.of(
        getTableKey.apply(tableNameDenied), avroSchema
    ));
    Mockito.verify(hiveRegister, Mockito.times(1)).getTable(eq(dbName), eq(tableNameDenied));

    // For tables included in the allow list, hive should be called and schema map should be updated with the latest schema
    Assert.assertTrue(updateLatestSchema.apply(tableNameAllowed));
    Assert.assertEquals(latestSchemaMap, ImmutableMap.of(
        getTableKey.apply(tableNameAllowed), avroSchema,
        getTableKey.apply(tableNameDenied), avroSchema
    ));
    Assert.assertTrue(updateLatestSchema.apply(tableNameAllowed));
    Mockito.verify(hiveRegister, Mockito.times(2)).getTable(eq(dbName), eq(tableNameAllowed));
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

  /**
   * Test class for exposing internal {@link HiveMetadataWriter} functions without making them public.
   * Although the ultimate fix would be to break up the logic in the hive metadata writer into smaller pieces,
   * this a stop gap way to make testing internal logic easier.
   *
   * This approach was taken because the writer lives in a separate module from this test class, and dependencies make
   * putting the test and implementation classes in the same module difficult
   */
  public static class TestHiveMetadataWriter extends HiveMetadataWriter {
    public TestHiveMetadataWriter(State state) throws IOException {
      super(state);
    }

    public static boolean updateLatestSchemaMapWithExistingSchema(
        String dbName,
        String tableName,
        String tableKey,
        WhitelistBlacklist useExistingTableSchemaAllowDenyList,
        HiveRegister hiveRegister,
        HashMap<String, String> latestSchemaMap
    ) throws IOException{
      return HiveMetadataWriter.updateLatestSchemaMapWithExistingSchema(dbName,
          tableName,
          tableKey,
          useExistingTableSchemaAllowDenyList,
          hiveRegister,
          latestSchemaMap
      );
    }
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

