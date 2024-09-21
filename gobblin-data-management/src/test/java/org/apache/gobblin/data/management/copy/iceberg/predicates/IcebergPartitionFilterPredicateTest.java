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

package org.apache.gobblin.data.management.copy.iceberg.predicates;

import java.util.Properties;

import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableMetadata;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests for {@link org.apache.gobblin.data.management.copy.iceberg.predicates.IcebergPartitionFilterPredicate} */
public class IcebergPartitionFilterPredicateTest {

  private TableMetadata mockTableMetadata;
  private Properties mockProperties;
  private MockedStatic<IcebergPartitionFilterPredicateUtil> icebergPartitionFilterPredicateUtilMockedStatic;
  private static final String TEST_ICEBERG_PARTITION_VALUES_KEY = "iceberg.dataset.source.partition.values";
  private static final String TEST_ICEBERG_PARTITION_VALUES_EXCEPTION_MESSAGE = "Partition column values cannot be empty";
  private static final String TEST_ICEBERG_PARTITION_COLUMN = "col1";
  private static final String TEST_ICEBERG_PARTITION_VALUES = "value1,value2";
  private static final String TEST_ICEBERG_PARTITION_VALUES_2 = "value1,value3,value2,value4";
  private static final String TEST_ICEBERG_PARTITION_VALUES_3 = "1,2,3,4";

  @BeforeMethod
  public void setup() {
    mockTableMetadata = Mockito.mock(TableMetadata.class);
    mockProperties = new Properties();
    icebergPartitionFilterPredicateUtilMockedStatic = Mockito.mockStatic(IcebergPartitionFilterPredicateUtil.class);
    icebergPartitionFilterPredicateUtilMockedStatic.when(
            () -> IcebergPartitionFilterPredicateUtil.getPartitionColumnIndex(Mockito.anyString(), Mockito.any(TableMetadata.class), Mockito.anyList()))
        .thenReturn(0);
  }

  @AfterMethod
  public void cleanup() {
    icebergPartitionFilterPredicateUtilMockedStatic.close();
  }

  @Test
  public void testPartitionColumnNotFound() {
    icebergPartitionFilterPredicateUtilMockedStatic.when(
            () -> IcebergPartitionFilterPredicateUtil.getPartitionColumnIndex(Mockito.anyString(), Mockito.any(TableMetadata.class), Mockito.anyList()))
        .thenReturn(-1);
    IllegalArgumentException exception = Assert.expectThrows(IllegalArgumentException.class, () -> {
      new IcebergPartitionFilterPredicate("nonexistentColumn", mockTableMetadata, mockProperties);
    });
    Assert.assertEquals(exception.getMessage(), "Partition column nonexistentColumn not found");
  }

  @Test
  public void testPartitionColumnValuesEmpty() {
    mockProperties.setProperty(TEST_ICEBERG_PARTITION_VALUES_KEY, "");
    verifyIllegalArgumentExceptionWithMessage();
  }

  @Test
  public void testPartitionColumnValuesNULL() {
    // Not setting values in mockProperties to test NULL value
    verifyIllegalArgumentExceptionWithMessage();
  }

  @Test
  public void testPartitionColumnValuesWhitespaces() {
    mockProperties.setProperty(TEST_ICEBERG_PARTITION_VALUES_KEY, "   ");
    verifyIllegalArgumentExceptionWithMessage();
  }

  @Test
  public void testPartitionValueNULL() {
    mockProperties.setProperty(TEST_ICEBERG_PARTITION_VALUES_KEY, TEST_ICEBERG_PARTITION_VALUES);
    IcebergPartitionFilterPredicate predicate = new IcebergPartitionFilterPredicate(TEST_ICEBERG_PARTITION_COLUMN,
        mockTableMetadata, mockProperties);
    // Just mocking, so that the partition value is NULL
    Assert.assertFalse(predicate.test(Mockito.mock(StructLike.class)));
  }

  @Test
  public void testWhenPartitionIsNull() {
    mockProperties.setProperty(TEST_ICEBERG_PARTITION_VALUES_KEY, TEST_ICEBERG_PARTITION_VALUES);
    IcebergPartitionFilterPredicate predicate = new IcebergPartitionFilterPredicate(TEST_ICEBERG_PARTITION_COLUMN,
        mockTableMetadata, mockProperties);
    Assert.assertFalse(predicate.test(null));
  }

  @Test
  public void testPartitionValueMatch() {
    mockProperties.setProperty(TEST_ICEBERG_PARTITION_VALUES_KEY, TEST_ICEBERG_PARTITION_VALUES);

    IcebergPartitionFilterPredicate predicate = new IcebergPartitionFilterPredicate(TEST_ICEBERG_PARTITION_COLUMN,
        mockTableMetadata, mockProperties);

    StructLike mockPartition = Mockito.mock(StructLike.class);
    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(Object.class))).thenReturn("value1");

    Assert.assertTrue(predicate.test(mockPartition));
  }

  @Test
  public void testPartitionValueMatch2() {
    mockProperties.setProperty(TEST_ICEBERG_PARTITION_VALUES_KEY, TEST_ICEBERG_PARTITION_VALUES_2);

    IcebergPartitionFilterPredicate predicate = new IcebergPartitionFilterPredicate(TEST_ICEBERG_PARTITION_COLUMN,
        mockTableMetadata, mockProperties);

    StructLike mockPartition = Mockito.mock(StructLike.class);
    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(Object.class))).thenReturn("value2");

    Assert.assertTrue(predicate.test(mockPartition));
  }

  @Test
  public void testPartitionValueNoMatch() {
    mockProperties.setProperty(TEST_ICEBERG_PARTITION_VALUES_KEY, TEST_ICEBERG_PARTITION_VALUES);

    IcebergPartitionFilterPredicate predicate = new IcebergPartitionFilterPredicate(TEST_ICEBERG_PARTITION_COLUMN,
        mockTableMetadata, mockProperties);

    StructLike mockPartition = Mockito.mock(StructLike.class);
    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(Object.class))).thenReturn("value3");

    Assert.assertFalse(predicate.test(mockPartition));
  }

  @Test
  public void testPartitionValuesAsInt() {
    mockProperties.setProperty(TEST_ICEBERG_PARTITION_VALUES_KEY, TEST_ICEBERG_PARTITION_VALUES_3);

    IcebergPartitionFilterPredicate predicate = new IcebergPartitionFilterPredicate(TEST_ICEBERG_PARTITION_COLUMN,
        mockTableMetadata, mockProperties);

    StructLike mockPartition = Mockito.mock(StructLike.class);
    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(Object.class))).thenReturn(3);
    Assert.assertTrue(predicate.test(mockPartition));

    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(Object.class))).thenReturn(4);
    Assert.assertTrue(predicate.test(mockPartition));

    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(Object.class))).thenReturn(10);
    Assert.assertFalse(predicate.test(mockPartition));

    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(Object.class))).thenReturn(Integer.MAX_VALUE);
    Assert.assertFalse(predicate.test(mockPartition));

    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(Object.class))).thenReturn(Integer.MIN_VALUE);
    Assert.assertFalse(predicate.test(mockPartition));
  }

  @Test
  public void testPartitionValuesAsIntMaxMin() {
    mockProperties.setProperty(TEST_ICEBERG_PARTITION_VALUES_KEY,
        String.join(",", String.valueOf(Integer.MIN_VALUE), String.valueOf(Integer.MAX_VALUE))
    );

    IcebergPartitionFilterPredicate predicate = new IcebergPartitionFilterPredicate(TEST_ICEBERG_PARTITION_COLUMN,
        mockTableMetadata, mockProperties);

    StructLike mockPartition = Mockito.mock(StructLike.class);

    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(Object.class))).thenReturn(Integer.MAX_VALUE);
    Assert.assertTrue(predicate.test(mockPartition));

    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(Object.class))).thenReturn(-1);
    Assert.assertFalse(predicate.test(mockPartition));

    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(Object.class))).thenReturn(0);
    Assert.assertFalse(predicate.test(mockPartition));

    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(Object.class))).thenReturn(Integer.MIN_VALUE);
    Assert.assertTrue(predicate.test(mockPartition));
  }

  private void verifyIllegalArgumentExceptionWithMessage() {
    IllegalArgumentException exception = Assert.expectThrows(IllegalArgumentException.class, () -> {
      new IcebergPartitionFilterPredicate(TEST_ICEBERG_PARTITION_COLUMN, mockTableMetadata, mockProperties);
    });
    Assert.assertTrue(exception.getMessage().startsWith(TEST_ICEBERG_PARTITION_VALUES_EXCEPTION_MESSAGE));
  }
}
