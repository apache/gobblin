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


/** Tests for {@link org.apache.gobblin.data.management.copy.iceberg.IcebergRegisterStep} */
public class IcebergRegisterStepTest {

  private TableIdentifier srcTableId = TableIdentifier.of("db", "foo");
  private TableIdentifier destTableId = TableIdentifier.of("db", "bar");

  @Test
  public void testDestSideMetadataMatchSucceeds() throws IOException {
    TableMetadata justPriorDestTableMetadata = mockTableMetadata("foo", "bar");
    TableMetadata readTimeSrcTableMetadata = Mockito.mock(TableMetadata.class); // (no mocked behavior)
    IcebergTable mockTable = mockIcebergTable("foo", "bar"); // matches!
    IcebergRegisterStep regStep =
        new IcebergRegisterStep(srcTableId, destTableId, readTimeSrcTableMetadata, justPriorDestTableMetadata, new Properties()) {
          @Override
          protected IcebergCatalog createDestinationCatalog() throws IOException {
            return mockSingleTableIcebergCatalog(mockTable);
          }
        };
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
    IcebergTable mockTable = mockIcebergTableForRetryer("foo", "bar", Boolean.FALSE);
    IcebergRegisterStep regStep =
        new IcebergRegisterStep(srcTableId, destTableId, readTimeSrcTableMetadata, justPriorDestTableMetadata, new Properties()) {
          @Override
          protected IcebergCatalog createDestinationCatalog() throws IOException {
            return mockSingleTableIcebergCatalog(mockTable);
          }
        };
    try {
      regStep.execute();
      Mockito.verify(mockTable, Mockito.times(3)).registerIcebergTable(any(), any());
    } catch (RuntimeException re) {
      Assert.fail("Got Unexpected Runtime Exception", re);
    }
  }

  @Test
  public void testRegisterIcebergTableWithRetryerThrowsRuntimeException() throws IOException {
    TableMetadata justPriorDestTableMetadata = mockTableMetadata("foo", "bar");
    TableMetadata readTimeSrcTableMetadata = Mockito.mock(TableMetadata.class);
    IcebergTable mockTable = mockIcebergTableForRetryer("foo", "bar", Boolean.TRUE);
    IcebergRegisterStep regStep =
        new IcebergRegisterStep(srcTableId, destTableId, readTimeSrcTableMetadata, justPriorDestTableMetadata, new Properties()) {
          @Override
          protected IcebergCatalog createDestinationCatalog() throws IOException {
            return mockSingleTableIcebergCatalog(mockTable);
          }
        };
    try {
      regStep.execute();
      Assert.fail("Expected Runtime Exception");
    } catch (RuntimeException re) {
      Assert.assertTrue(re.getMessage().startsWith("Failed to register iceberg table (retried"), re.getMessage());
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

  protected IcebergTable mockIcebergTableForRetryer(String tableUuid, String tableMetadataFileLocation, Boolean throwOnlyExceptionsForRegisterIcebergTable) throws IOException {
    TableMetadata mockMetadata = mockTableMetadata(tableUuid, tableMetadataFileLocation);

    IcebergTable mockTable = Mockito.mock(IcebergTable.class);
    Mockito.when(mockTable.accessTableMetadata()).thenReturn(mockMetadata);
    if (throwOnlyExceptionsForRegisterIcebergTable) {
      Mockito.doThrow(new RuntimeException()).when(mockTable).registerIcebergTable(any(), any());
    } else {
      Mockito.doThrow(new RuntimeException()).doThrow(new RuntimeException()).doNothing().when(mockTable).registerIcebergTable(any(), any());
    }
    return mockTable;
  }
}
