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

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.transforms.Transform;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;


/** Tests for {@link org.apache.gobblin.data.management.copy.iceberg.predicates.IcebergPartitionFilterPredicate} */
public class IcebergPartitionFilterPredicateTest {

  private TableMetadata mockTableMetadata;
  private Properties mockProperties;
  private static final String TEST_ICEBERG_PARTITION_VALUES_KEY = "iceberg.dataset.source.partition.values";
  private static final String TEST_ICEBERG_PARTITION_VALUES_EXCEPTION_MESSAGE = "Partition column values cannot be empty";
  private static final String TEST_ICEBERG_PARTITION_COLUMN = "col1";
  private static final String TEST_ICEBERG_PARTITION_VALUES = "value1,value2";
  private static final String TEST_ICEBERG_PARTITION_VALUES_2 = "value1,value3,value2,value4";
  private static final String TEST_ICEBERG_TRANSFORM = "identity";

  @BeforeMethod
  public void setup() {
    mockTableMetadata = Mockito.mock(TableMetadata.class);
    mockProperties = new Properties();
  }

  private void setupMockMetadata(TableMetadata mockTableMetadata) {
    PartitionSpec mockPartitionSpec = Mockito.mock(PartitionSpec.class);
    PartitionField mockPartitionField = Mockito.mock(PartitionField.class);
    Transform mockTransform = Mockito.mock(Transform.class);

    Mockito.when(mockTableMetadata.spec()).thenReturn(mockPartitionSpec);
    Mockito.when(mockPartitionSpec.fields()).thenReturn(ImmutableList.of(mockPartitionField));
    Mockito.when(mockPartitionField.name()).thenReturn(TEST_ICEBERG_PARTITION_COLUMN);
    Mockito.when(mockPartitionField.transform()).thenReturn(mockTransform);
    Mockito.when(mockTransform.toString()).thenReturn(TEST_ICEBERG_TRANSFORM);
  }

  @Test
  public void testPartitionColumnNotFound() {
    Mockito.when(mockTableMetadata.spec()).thenReturn(Mockito.mock(PartitionSpec.class));
    IllegalArgumentException exception = Assert.expectThrows(IllegalArgumentException.class, () -> {
      new IcebergPartitionFilterPredicate("nonexistentColumn", mockTableMetadata, mockProperties);
    });
    Assert.assertEquals(exception.getMessage(), "Partition column nonexistentColumn not found");
  }

  @Test
  public void testPartitionColumnValuesEmpty() {
    mockProperties.setProperty(TEST_ICEBERG_PARTITION_VALUES_KEY, "");
    setupMockMetadata(mockTableMetadata);
    verifyIllegalArgumentExceptionWithMessage();
  }

  @Test
  public void testPartitionColumnValuesNULL() {
    // Not setting values in mockProperties to test NULL value
    setupMockMetadata(mockTableMetadata);
    verifyIllegalArgumentExceptionWithMessage();
  }

  @Test
  public void testPartitionColumnValuesWhitespaces() {
    mockProperties.setProperty(TEST_ICEBERG_PARTITION_VALUES_KEY, "   ");
    setupMockMetadata(mockTableMetadata);
    verifyIllegalArgumentExceptionWithMessage();
  }

  @Test
  public void testPartitionValueNULL() {
    mockProperties.setProperty(TEST_ICEBERG_PARTITION_VALUES_KEY, TEST_ICEBERG_PARTITION_VALUES);
    setupMockMetadata(mockTableMetadata);
    IcebergPartitionFilterPredicate predicate = new IcebergPartitionFilterPredicate(TEST_ICEBERG_PARTITION_COLUMN,
        mockTableMetadata, mockProperties);
    // Just mocking, so that the partition value is NULL
    Assert.assertFalse(predicate.test(Mockito.mock(StructLike.class)));
  }

  @Test
  public void testPartitionValueMatch() {
    mockProperties.setProperty(TEST_ICEBERG_PARTITION_VALUES_KEY, TEST_ICEBERG_PARTITION_VALUES);
    setupMockMetadata(mockTableMetadata);

    IcebergPartitionFilterPredicate predicate = new IcebergPartitionFilterPredicate(TEST_ICEBERG_PARTITION_COLUMN,
        mockTableMetadata, mockProperties);

    StructLike mockPartition = Mockito.mock(StructLike.class);
    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(String.class))).thenReturn("value1");

    Assert.assertTrue(predicate.test(mockPartition));
  }

  @Test
  public void testPartitionValueMatch2() {
    mockProperties.setProperty(TEST_ICEBERG_PARTITION_VALUES_KEY, TEST_ICEBERG_PARTITION_VALUES_2);
    setupMockMetadata(mockTableMetadata);

    IcebergPartitionFilterPredicate predicate = new IcebergPartitionFilterPredicate(TEST_ICEBERG_PARTITION_COLUMN,
        mockTableMetadata, mockProperties);

    StructLike mockPartition = Mockito.mock(StructLike.class);
    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(String.class))).thenReturn("value2");

    Assert.assertTrue(predicate.test(mockPartition));
  }

  @Test
  public void testPartitionValueNoMatch() {
    mockProperties.setProperty(TEST_ICEBERG_PARTITION_VALUES_KEY, TEST_ICEBERG_PARTITION_VALUES);
    setupMockMetadata(mockTableMetadata);

    IcebergPartitionFilterPredicate predicate = new IcebergPartitionFilterPredicate(TEST_ICEBERG_PARTITION_COLUMN,
        mockTableMetadata, mockProperties);

    StructLike mockPartition = Mockito.mock(StructLike.class);
    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(String.class))).thenReturn("value3");

    Assert.assertFalse(predicate.test(mockPartition));
  }

  private void verifyIllegalArgumentExceptionWithMessage() {
    IllegalArgumentException exception = Assert.expectThrows(IllegalArgumentException.class, () -> {
      new IcebergPartitionFilterPredicate(TEST_ICEBERG_PARTITION_COLUMN, mockTableMetadata, mockProperties);
    });
    Assert.assertTrue(exception.getMessage().startsWith(TEST_ICEBERG_PARTITION_VALUES_EXCEPTION_MESSAGE));
  }

}
