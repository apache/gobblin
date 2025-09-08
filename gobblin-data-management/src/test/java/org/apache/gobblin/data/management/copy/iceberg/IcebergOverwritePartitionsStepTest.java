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

package org.apache.gobblin.data.management.copy.iceberg;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.TableIdentifier;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

import static org.apache.gobblin.util.retry.RetryerFactory.RETRY_TIMES;

/** Tests for {@link IcebergOverwritePartitionsStep} */
public class IcebergOverwritePartitionsStepTest {
  private final String destTableIdStr = "db.foo";
  private final String testPartitionColName = "testPartition";
  private final String testPartitionColValue = "testValue";
  private IcebergTable mockIcebergTable;
  private IcebergCatalog mockIcebergCatalog;
  private Properties mockProperties;
  private IcebergOverwritePartitionsStep spyIcebergOverwritePartitionsStep;

  @BeforeMethod
  public void setUp() throws IOException {
    mockIcebergTable = Mockito.mock(IcebergTable.class);
    mockIcebergCatalog = Mockito.mock(IcebergCatalog.class);
    mockProperties = new Properties();

    spyIcebergOverwritePartitionsStep = Mockito.spy(new IcebergOverwritePartitionsStep(destTableIdStr,
        testPartitionColName, testPartitionColValue, mockProperties));

    spyIcebergOverwritePartitionsStep.setDataFiles(getDummyDataFiles());

    Mockito.when(mockIcebergCatalog.openTable(Mockito.any(TableIdentifier.class))).thenReturn(mockIcebergTable);
    Mockito.doReturn(mockIcebergCatalog).when(spyIcebergOverwritePartitionsStep).createDestinationCatalog();
  }

  @Test
  public void testNeverIsCompleted() {
    Assert.assertFalse(spyIcebergOverwritePartitionsStep.isCompleted());
  }

  @Test
  public void testExecute() {
    try {
      Mockito.doNothing().when(mockIcebergTable).overwritePartition(Mockito.anyList(), Mockito.anyString(),
          Mockito.anyString());
      spyIcebergOverwritePartitionsStep.execute();
      Mockito.verify(mockIcebergTable, Mockito.times(1)).overwritePartition(Mockito.anyList(),
          Mockito.anyString(), Mockito.anyString());
    } catch (IOException e) {
      Assert.fail(String.format("Unexpected IOException : %s", e));
    }
  }

  @Test
  public void testExecuteWithRetry() {
    try {
      // first call throw exception which will be retried and on second call nothing happens
      Mockito.doThrow(new RuntimeException()).doNothing().when(mockIcebergTable).overwritePartition(Mockito.anyList(),
          Mockito.anyString(), Mockito.anyString());
      spyIcebergOverwritePartitionsStep.execute();
      Mockito.verify(mockIcebergTable, Mockito.times(2)).overwritePartition(Mockito.anyList(),
          Mockito.anyString(), Mockito.anyString());
    } catch (IOException e) {
      Assert.fail(String.format("Unexpected IOException : %s", e));
    }
  }

  @Test
  public void testExecuteWithDefaultRetry() throws IcebergTable.TableNotFoundException {
    try {
      // Always throw exception
      Mockito.doThrow(new RuntimeException()).when(mockIcebergTable).overwritePartition(Mockito.anyList(),
          Mockito.anyString(), Mockito.anyString());
      spyIcebergOverwritePartitionsStep.execute();
    } catch (RuntimeException e) {
      Mockito.verify(mockIcebergTable, Mockito.times(3)).overwritePartition(Mockito.anyList(),
          Mockito.anyString(), Mockito.anyString());
      assertRetryTimes(e, 3);
    } catch (IOException e) {
      Assert.fail(String.format("Unexpected IOException : %s", e));
    }
  }

  @Test
  public void testExecuteWithCustomRetryConfig() throws IOException {
    int retryCount = 7;
    mockProperties.setProperty(IcebergOverwritePartitionsStep.OVERWRITE_PARTITIONS_RETRYER_CONFIG_PREFIX + "." + RETRY_TIMES,
        Integer.toString(retryCount));
    spyIcebergOverwritePartitionsStep = Mockito.spy(new IcebergOverwritePartitionsStep(destTableIdStr,
        testPartitionColName, testPartitionColValue, mockProperties));

    spyIcebergOverwritePartitionsStep.setDataFiles(getDummyDataFiles());

    Mockito.when(mockIcebergCatalog.openTable(Mockito.any(TableIdentifier.class))).thenReturn(mockIcebergTable);
    Mockito.doReturn(mockIcebergCatalog).when(spyIcebergOverwritePartitionsStep).createDestinationCatalog();
    try {
      // Always throw exception
      Mockito.doThrow(new RuntimeException()).when(mockIcebergTable).overwritePartition(Mockito.anyList(),
          Mockito.anyString(), Mockito.anyString());
      spyIcebergOverwritePartitionsStep.execute();
    } catch (RuntimeException e) {
      Mockito.verify(mockIcebergTable, Mockito.times(retryCount)).overwritePartition(Mockito.anyList(),
          Mockito.anyString(), Mockito.anyString());
      assertRetryTimes(e, retryCount);
    } catch (IOException e) {
      Assert.fail(String.format("Unexpected IOException : %s", e));
    }
  }

  private List<DataFile> getDummyDataFiles() {
    DataFile dataFile1 = DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath("/path/to/db/foo/data/datafile1.orc")
        .withFileSizeInBytes(1234)
        .withRecordCount(100)
        .build();

    DataFile dataFile2 = DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath("/path/to/db/foo/data/datafile2.orc")
        .withFileSizeInBytes(9876)
        .withRecordCount(50)
        .build();

    return ImmutableList.of(dataFile1, dataFile2);
  }

  private void assertRetryTimes(RuntimeException re, Integer retryTimes) {
    String msg = String.format("~%s~ Failure attempting to overwrite partition [num failures: %d]", destTableIdStr, retryTimes);
    Assert.assertTrue(re.getMessage().startsWith(msg), re.getMessage());
  }
}
