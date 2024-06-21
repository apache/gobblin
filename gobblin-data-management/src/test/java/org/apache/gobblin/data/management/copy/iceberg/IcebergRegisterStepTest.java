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
import java.util.Properties;

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

import static org.apache.gobblin.util.retry.RetryerFactory.RETRY_TIMES;


/** Tests for {@link org.apache.gobblin.data.management.copy.iceberg.IcebergRegisterStep} */
public class IcebergRegisterStepTest {

  private TableIdentifier srcTableId = TableIdentifier.of("db", "foo");
  private TableIdentifier destTableId = TableIdentifier.of("db", "bar");

  @Test
  public void testDestSideMetadataMatchSucceeds() throws IOException {
    TableMetadata justPriorDestTableMetadata = mockTableMetadata("foo", "bar");
    TableMetadata readTimeSrcTableMetadata = Mockito.mock(TableMetadata.class); // (no mocked behavior)
    IcebergTable mockTable = mockIcebergTable("foo", "bar"); // matches!
    IcebergRegisterStep regStep = createIcebergRegisterStepInstance(readTimeSrcTableMetadata, justPriorDestTableMetadata, mockTable, new Properties());
    try {
      regStep.execute();
      Mockito.verify(mockTable, Mockito.times(1)).registerIcebergTable(any(), any());
    } catch (IOException ioe) {
      Assert.fail("got unexpected IOException", ioe);
    }
  }

  @Test
  public void testDestSideMetadataMismatchThrowsIOException() throws IOException {
    TableMetadata justPriorDestTableMetadata = mockTableMetadata("foo", "bar");
    TableMetadata readTimeSrcTableMetadata = Mockito.mock(TableMetadata.class); // (no mocked behavior)
    IcebergRegisterStep regStep =
        new IcebergRegisterStep(srcTableId, destTableId, readTimeSrcTableMetadata, justPriorDestTableMetadata, new Properties()) {
          @Override
          protected IcebergCatalog createDestinationCatalog() throws IOException {
            return mockSingleTableIcebergCatalog("foo", "notBar");
          }
        };
    try {
      regStep.execute();
      Assert.fail("expected IOException");
    } catch (IOException ioe) {
      Assert.assertTrue(ioe.getMessage().startsWith("error: likely concurrent writing to destination"), ioe.getMessage());
    }
  }

  @Test
  public void testRegisterIcebergTableWithRetryer() throws IOException {
    TableMetadata justPriorDestTableMetadata = mockTableMetadata("foo", "bar");
    TableMetadata readTimeSrcTableMetadata = Mockito.mock(TableMetadata.class);
    IcebergTable mockTable = mockIcebergTable("foo", "bar");
    // Mocking registerIcebergTable() call to fail for first two attempts and then succeed
    // So total number of invocations to registerIcebergTable() should be 3 only
    Mockito.doThrow(new RuntimeException())
        .doThrow(new RuntimeException())
        .doNothing()
        .when(mockTable).registerIcebergTable(any(), any());
    IcebergRegisterStep regStep = createIcebergRegisterStepInstance(readTimeSrcTableMetadata, justPriorDestTableMetadata, mockTable, new Properties());
    try {
      regStep.execute();
      Mockito.verify(mockTable, Mockito.times(3)).registerIcebergTable(any(), any());
    } catch (RuntimeException re) {
      Assert.fail("Got Unexpected Runtime Exception", re);
    }
  }

  @Test
  public void testRegisterIcebergTableWithDefaultRetryerConfig() throws IOException {
    TableMetadata justPriorDestTableMetadata = mockTableMetadata("foo", "bar");
    TableMetadata readTimeSrcTableMetadata = Mockito.mock(TableMetadata.class);
    IcebergTable mockTable = mockIcebergTable("foo", "bar");
    // Mocking registerIcebergTable() call to always throw exception
    Mockito.doThrow(new RuntimeException()).when(mockTable).registerIcebergTable(any(), any());
    IcebergRegisterStep regStep = createIcebergRegisterStepInstance(readTimeSrcTableMetadata, justPriorDestTableMetadata, mockTable, new Properties());
    try {
      regStep.execute();
      Assert.fail("Expected Runtime Exception");
    } catch (RuntimeException re) {
      // The default number of retries is 5 so register iceberg table should fail after retrying for 5 times
      assertRetryTimes(re, 5);
    }
  }

  @Test
  public void testRegisterIcebergTableWithOverrideRetryerConfig() throws IOException {
    TableMetadata justPriorDestTableMetadata = mockTableMetadata("foo", "bar");
    TableMetadata readTimeSrcTableMetadata = Mockito.mock(TableMetadata.class);
    IcebergTable mockTable = mockIcebergTable("foo", "bar");
    // Mocking registerIcebergTable() call to always throw exception
    Mockito.doThrow(new RuntimeException()).when(mockTable).registerIcebergTable(any(), any());
    Properties properties = new Properties();
    String retryCount = "10";
    // Changing the number of retries to 10
    properties.setProperty(IcebergRegisterStep.RETRYER_CONFIG_PREFIX + "." + RETRY_TIMES, retryCount);
    IcebergRegisterStep regStep = createIcebergRegisterStepInstance(readTimeSrcTableMetadata, justPriorDestTableMetadata, mockTable, properties);
    try {
      regStep.execute();
      Assert.fail("Expected Runtime Exception");
    } catch (RuntimeException re) {
      // register iceberg table should fail after retrying for retryCount times mentioned above
      assertRetryTimes(re, Integer.parseInt(retryCount));
    }
  }

  protected TableMetadata mockTableMetadata(String uuid, String metadataFileLocation) throws IOException {
    TableMetadata mockMetadata = Mockito.mock(TableMetadata.class);
    Mockito.when(mockMetadata.uuid()).thenReturn(uuid);
    Mockito.when(mockMetadata.metadataFileLocation()).thenReturn(metadataFileLocation);
    return mockMetadata;
  }

  protected IcebergTable mockIcebergTable(String tableUuid, String tableMetadataFileLocation) throws IOException {
    TableMetadata mockMetadata = mockTableMetadata(tableUuid, tableMetadataFileLocation);

    IcebergTable mockTable = Mockito.mock(IcebergTable.class);
    Mockito.when(mockTable.accessTableMetadata()).thenReturn(mockMetadata);
    return mockTable;
  }

  protected IcebergCatalog mockSingleTableIcebergCatalog(String tableUuid, String tableMetadataFileLocation) throws IOException {
    IcebergTable mockTable = mockIcebergTable(tableUuid, tableMetadataFileLocation);
    return mockSingleTableIcebergCatalog(mockTable);
  }

  protected IcebergCatalog mockSingleTableIcebergCatalog(IcebergTable mockTable) throws IOException {
    IcebergCatalog catalog = Mockito.mock(IcebergCatalog.class);
    Mockito.when(catalog.openTable(any())).thenReturn(mockTable);
    return catalog;
  }

  private IcebergRegisterStep createIcebergRegisterStepInstance(TableMetadata readTimeSrcTableMetadata,
                                                                TableMetadata justPriorDestTableMetadata,
                                                                IcebergTable mockTable,
                                                                Properties properties) {
    return new IcebergRegisterStep(srcTableId, destTableId, readTimeSrcTableMetadata, justPriorDestTableMetadata, properties) {
      @Override
      protected IcebergCatalog createDestinationCatalog() throws IOException {
        return mockSingleTableIcebergCatalog(mockTable);
      }
    };
  }

  private void assertRetryTimes(RuntimeException re, Integer retryTimes) {
    String msg = String.format("Failed to register iceberg table : (src: {%s}) - (dest: {%s}) : (retried %d times)", srcTableId, destTableId, retryTimes);
    Assert.assertTrue(re.getMessage().startsWith(msg), re.getMessage());
  }
}
