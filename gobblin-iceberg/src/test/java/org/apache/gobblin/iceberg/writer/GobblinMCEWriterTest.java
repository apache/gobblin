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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BiConsumer;
import lombok.SneakyThrows;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.hive.HiveTable;
import org.apache.gobblin.hive.spec.HiveSpec;
import org.apache.gobblin.hive.writer.MetadataWriter;
import org.apache.gobblin.metadata.DatasetIdentifier;
import org.apache.gobblin.metadata.GobblinMetadataChangeEvent;
import org.apache.gobblin.metadata.OperationType;
import org.apache.gobblin.metadata.SchemaSource;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaPartition;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaStreamingExtractor;
import org.apache.gobblin.stream.RecordEnvelope;
import org.apache.gobblin.util.ClustersNames;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;
import org.apache.hadoop.fs.FileSystem;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockObjectFactory;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.IObjectFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

import static org.mockito.Mockito.*;


/**
 * Test class uses PowerMockito and Testng
 * References:
 * https://github.com/powermock/powermock/issues/434
 * https://jivimberg.io/blog/2016/04/03/using-powermock-plus-testng-to-mock-a-static-class/
 * https://www.igorkromin.net/index.php/2018/10/04/how-to-fix-powermock-exception-linkageerror-loader-constraint-violation/
 */
@PrepareForTest({GobblinConstructorUtils.class, FileSystem.class})
@PowerMockIgnore("javax.management.*")
public class GobblinMCEWriterTest extends PowerMockTestCase {

  private String dbName = "hivedb";
  private String tableName = "testTable";
  private GobblinMCEWriter gobblinMCEWriter;
  private KafkaStreamingExtractor.KafkaWatermark watermark;

  private GobblinMetadataChangeEvent.Builder gmceBuilder;

  // Not using field injection because they must be different classes
  private MetadataWriter mockWriter;
  private MetadataWriter exceptionWriter;

  @Mock
  private FileSystem fs;

  @Mock
  private HiveSpec mockHiveSpec;

  @Mock
  private HiveTable mockTable;

  @AfterMethod
  public void clean() throws Exception {
    gobblinMCEWriter.close();
  }

  @BeforeMethod
  public void setUp() throws Exception {
    initMocks();
    gmceBuilder = GobblinMetadataChangeEvent.newBuilder()
        .setDatasetIdentifier(DatasetIdentifier.newBuilder()
            .setDataPlatformUrn("urn:namespace:dataPlatform:hdfs")
            .setNativeName("testDB/testTable")
            .build())
        .setFlowId("testFlow")
        .setSchemaSource(SchemaSource.EVENT)
        .setOperationType(OperationType.add_files)
        .setCluster(ClustersNames.getInstance().getClusterName());
    watermark = new KafkaStreamingExtractor.KafkaWatermark(
        new KafkaPartition.Builder().withTopicName("GobblinMetadataChangeEvent_test").withId(1).build(),
        new LongWatermark(10L));

    State state = new State();
    String metadataWriters = String.join(",",
        Arrays.asList(mockWriter.getClass().getName(), exceptionWriter.getClass().getName()));
    state.setProp("gmce.metadata.writer.classes", metadataWriters);

    Mockito.doNothing().when(mockWriter)
        .writeEnvelope(
            Mockito.any(RecordEnvelope.class), Mockito.anyMap(), Mockito.anyMap(), Mockito.any(HiveSpec.class));
    Mockito.doThrow(new IOException("Test Exception")).when(exceptionWriter)
        .writeEnvelope(
            Mockito.any(RecordEnvelope.class), Mockito.anyMap(), Mockito.anyMap(), Mockito.any(HiveSpec.class));

    PowerMockito.mockStatic(GobblinConstructorUtils.class);
    when(GobblinConstructorUtils.invokeConstructor(
            eq(MetadataWriter.class), eq(mockWriter.getClass().getName()), any(State.class)))
        .thenReturn(mockWriter);
    when(GobblinConstructorUtils.invokeConstructor(
        eq(MetadataWriter.class), eq(exceptionWriter.getClass().getName()), any(State.class)))
        .thenReturn(exceptionWriter);

    PowerMockito.mockStatic(FileSystem.class);
    when(FileSystem.get(any()))
        .thenReturn(fs);

    when(mockTable.getDbName()).thenReturn(dbName);
    when(mockTable.getTableName()).thenReturn(tableName);
    when(mockHiveSpec.getTable()).thenReturn(mockTable);

    gobblinMCEWriter = new GobblinMCEWriter(new GobblinMCEWriterBuilder(), state);
  }

  @Test
  public void testWriteWhenWriterSpecified() throws IOException {
    gmceBuilder.setAllowedMetadataWriters(Arrays.asList(mockWriter.getClass().getName()));
    writeWithMetadataWriters(gmceBuilder.build());

    Mockito.verify(mockWriter, Mockito.times(1)).writeEnvelope(
        Mockito.any(RecordEnvelope.class), Mockito.anyMap(), Mockito.anyMap(), Mockito.any(HiveSpec.class));
    Mockito.verify(exceptionWriter, never()).writeEnvelope(
        Mockito.any(RecordEnvelope.class), Mockito.anyMap(), Mockito.anyMap(), Mockito.any(HiveSpec.class));
  }

  @Test
  public void testFaultTolerance() throws IOException {

    gobblinMCEWriter.setMaxErrorDataset(1);
    gobblinMCEWriter.metadataWriters = Arrays.asList(mockWriter, exceptionWriter, mockWriter);
    gobblinMCEWriter.tableOperationTypeMap = new HashMap<>();

    String dbName2 = dbName + "2";
    String otherDb = "someOtherDB";
    addTableStatus(dbName, "datasetPath");
    addTableStatus(dbName2, "datasetPath");
    addTableStatus(otherDb, "otherDatasetPath");

    BiConsumer<String, String> verifyMocksCalled = new BiConsumer<String, String>(){
      private int timesCalled = 0;
      @Override
      @SneakyThrows
      public void accept(String dbName, String tableName) {
        timesCalled++;

        // also validates that order is maintained since all writers after an exception should reset instead of write
        Mockito.verify(mockWriter, Mockito.times(timesCalled)).writeEnvelope(
            Mockito.any(RecordEnvelope.class), Mockito.anyMap(), Mockito.anyMap(), Mockito.any(HiveSpec.class));
        Mockito.verify(exceptionWriter, Mockito.times(timesCalled)).writeEnvelope(
            Mockito.any(RecordEnvelope.class), Mockito.anyMap(), Mockito.anyMap(), Mockito.any(HiveSpec.class));
        Mockito.verify(exceptionWriter, Mockito.times(1)).reset(dbName, tableName);
        Mockito.verify(mockWriter, Mockito.times(1)).reset(dbName, tableName);
      }
    };

    writeWithMetadataWriters(gmceBuilder.build());
    verifyMocksCalled.accept(dbName, tableName);

    // Another exception for same dataset but different db
    when(mockTable.getDbName()).thenReturn(dbName2);
    writeWithMetadataWriters(gmceBuilder.build());
    verifyMocksCalled.accept(dbName2, tableName);

    // exception thrown because exceeds max number of datasets with errors
    when(mockTable.getDbName()).thenReturn(otherDb);
    Assert.expectThrows(IOException.class, () -> writeWithMetadataWriters(gmceBuilder.setDatasetIdentifier(DatasetIdentifier.newBuilder()
        .setDataPlatformUrn("urn:namespace:dataPlatform:hdfs")
        .setNativeName("someOtherDB/testTable")
        .build()).build()));
  }

  @Test(dataProvider = "AllowMockMetadataWriter")
  public void testGetAllowedMetadataWriters(List<String> metadataWriters) {
    Assert.assertNotEquals(mockWriter.getClass().getName(), exceptionWriter.getClass().getName());
    gmceBuilder.setAllowedMetadataWriters(metadataWriters);
    List<MetadataWriter> allowedWriters = GobblinMCEWriter.getAllowedMetadataWriters(
        gmceBuilder.build(),
        Arrays.asList(mockWriter, exceptionWriter));

    Assert.assertEquals(allowedWriters.size(), 2);
    Assert.assertEquals(allowedWriters.get(0).getClass().getName(), mockWriter.getClass().getName());
    Assert.assertEquals(allowedWriters.get(1).getClass().getName(), exceptionWriter.getClass().getName());
  }

  @Test
  public void testDetectTransientException() {
    Set<String> transientExceptions = Sets.newHashSet("Filesystem closed", "Hive timeout", "RejectedExecutionException");
    IOException transientException = new IOException("test1 Filesystem closed test");
    IOException wrapperException = new IOException("wrapper exception", transientException);
    Assert.assertTrue(GobblinMCEWriter.isExceptionTransient(transientException, transientExceptions));
    Assert.assertTrue(GobblinMCEWriter.isExceptionTransient(wrapperException, transientExceptions));
    IOException nonTransientException = new IOException("Write failed due to bad schema");
    Assert.assertFalse(GobblinMCEWriter.isExceptionTransient(nonTransientException, transientExceptions));
    RejectedExecutionException rejectedExecutionException = new RejectedExecutionException("");
    Assert.assertTrue(GobblinMCEWriter.isExceptionTransient(rejectedExecutionException, transientExceptions));
  }

  @DataProvider(name="AllowMockMetadataWriter")
  public Object[][] allowMockMetadataWriterParams() {
    initMocks();
    return new Object[][] {
        {Arrays.asList(mockWriter.getClass().getName(), exceptionWriter.getClass().getName())},
        {Collections.emptyList()}
    };
  }

  private void initMocks() {
    MockitoAnnotations.initMocks(this);
    // Hacky way to have 2 mock MetadataWriter "classes" with different underlying names
    mockWriter = Mockito.mock(MetadataWriter.class);
    exceptionWriter = Mockito.mock(TestExceptionMetadataWriter.class);
  }

  private static abstract class TestExceptionMetadataWriter implements MetadataWriter { }

  private void writeWithMetadataWriters(GobblinMetadataChangeEvent gmce) throws IOException {
    List<MetadataWriter> allowedMetadataWriters = GobblinMCEWriter.getAllowedMetadataWriters(
        gmce, gobblinMCEWriter.getMetadataWriters());

    gobblinMCEWriter.writeWithMetadataWriters(new RecordEnvelope<>(gmce, watermark), allowedMetadataWriters,
        new ConcurrentHashMap(), new ConcurrentHashMap(), mockHiveSpec);
  }

  private void addTableStatus(String dbName, String datasetPath) {
    gobblinMCEWriter.tableOperationTypeMap.put(dbName + "." + tableName, new GobblinMCEWriter.TableStatus(
        OperationType.add_files, datasetPath, "GobblinMetadataChangeEvent_test-1", 0, 50));
  }

  @ObjectFactory
  public IObjectFactory getObjectFactory() {
    return new PowerMockObjectFactory();
  }
}

