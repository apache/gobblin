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

/** Tests for {@link org.apache.gobblin.data.management.copy.iceberg.predicates.IcebergDateTimePartitionFilterPredicate} */
public class IcebergDateTimePartitionFilterPredicateTest {
  private static final String TEST_ICEBERG_PARTITION_DATETTIME = "iceberg.dataset.source.partition.datetime";
  private static final String TEST_ICEBERG_PARTITION_DATETTIME_PATTERN = TEST_ICEBERG_PARTITION_DATETTIME + ".pattern";
  private static final String TEST_ICEBERG_PARTITION_DATETTIME_STARTDATE = TEST_ICEBERG_PARTITION_DATETTIME + ".startdate";
  private static final String TEST_ICEBERG_PARTITION_DATETTIME_ENDDATE = TEST_ICEBERG_PARTITION_DATETTIME + ".enddate";
  private static final String PARTITION_COLUMN_NAME = "partitionColumn";
  private static final String PARTITION_PATTERN = "yyyy-MM-dd";
  private static final String START_DATE = "2024-01-01";
  private static final String END_DATE = "2024-12-31";
  private TableMetadata mockTableMetadata;
  private Properties mockProperties;
  private StructLike mockPartition;
  private IcebergDateTimePartitionFilterPredicate mockDateTimePartitionFilterPredicate;
  private MockedStatic<IcebergPartitionFilterPredicateUtil> icebergPartitionFilterPredicateUtilMockedStatic;

  @BeforeMethod
  public void setup() {
    mockTableMetadata = Mockito.mock(TableMetadata.class);
    icebergPartitionFilterPredicateUtilMockedStatic = Mockito.mockStatic(IcebergPartitionFilterPredicateUtil.class);
    icebergPartitionFilterPredicateUtilMockedStatic.when(
            () -> IcebergPartitionFilterPredicateUtil.getPartitionColumnIndex(Mockito.anyString(), Mockito.any(TableMetadata.class), Mockito.anyList()))
        .thenReturn(0);

    mockProperties = new Properties();
    mockProperties.setProperty(TEST_ICEBERG_PARTITION_DATETTIME_PATTERN, PARTITION_PATTERN);
    mockProperties.setProperty(TEST_ICEBERG_PARTITION_DATETTIME_STARTDATE, START_DATE);
    mockProperties.setProperty(TEST_ICEBERG_PARTITION_DATETTIME_ENDDATE, END_DATE);

    mockDateTimePartitionFilterPredicate = new IcebergDateTimePartitionFilterPredicate(
        PARTITION_COLUMN_NAME,
        mockTableMetadata,
        mockProperties
    );

    mockPartition = Mockito.mock(StructLike.class);
  }

  @AfterMethod
  public void cleanup() {
    icebergPartitionFilterPredicateUtilMockedStatic.close();
  }

  @Test
  public void testWhenPartitionIsNull() {
    Assert.assertFalse(mockDateTimePartitionFilterPredicate.test(null));
  }

  @Test
  public void testPartitionColumnNotFound() {
    icebergPartitionFilterPredicateUtilMockedStatic.when(
            () -> IcebergPartitionFilterPredicateUtil.getPartitionColumnIndex(Mockito.anyString(), Mockito.any(TableMetadata.class), Mockito.anyList()))
        .thenReturn(-1);
    verifyIllegalArgumentExceptionWithMessage("Partition column partitionColumn not found");
  }

  @Test
  public void testPartitionBeforeRange() {
    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(String.class))).thenReturn("2023-12-31");
    Assert.assertFalse(mockDateTimePartitionFilterPredicate.test(mockPartition));
  }

  @Test
  public void testPartitionWithinRange() {
    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(String.class))).thenReturn("2024-06-15");
    Assert.assertTrue(mockDateTimePartitionFilterPredicate.test(mockPartition));
  }

  @Test
  public void testPartitionOnStartDate() {
    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(String.class))).thenReturn(START_DATE);
    Assert.assertTrue(mockDateTimePartitionFilterPredicate.test(mockPartition));
  }

  @Test
  public void testPartitionOnEndDate() {
    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(String.class))).thenReturn(END_DATE);
    Assert.assertTrue(mockDateTimePartitionFilterPredicate.test(mockPartition));
  }

  @Test
  public void testPartitionAfterRange() {
    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(String.class))).thenReturn("2025-01-01");
    Assert.assertFalse(mockDateTimePartitionFilterPredicate.test(mockPartition));
  }

  @Test
  public void testPartitionValueIsBlank() {
    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(String.class))).thenReturn("");
    Assert.assertFalse(mockDateTimePartitionFilterPredicate.test(mockPartition));
  }

  @Test
  public void testPartitionValueIsNull() {
    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(String.class))).thenReturn(null);
    Assert.assertFalse(mockDateTimePartitionFilterPredicate.test(mockPartition));
  }

  @Test
  public void testMissingPartitionPattern() {
    mockProperties.remove(TEST_ICEBERG_PARTITION_DATETTIME_PATTERN);
    verifyIllegalArgumentExceptionWithMessage("DateTime Partition pattern cannot be empty");
  }

  @Test
  public void testInvalidPartitionPattern() {
    mockProperties.setProperty(TEST_ICEBERG_PARTITION_DATETTIME_PATTERN, "invalid-pattern");
    verifyIllegalArgumentExceptionWithMessage("Illegal pattern");
  }

  @Test
  public void testMissingStartDate() {
    mockProperties.remove(TEST_ICEBERG_PARTITION_DATETTIME_STARTDATE);
    verifyIllegalArgumentExceptionWithMessage("DateTime Partition start date cannot be empty");
  }

  @Test
  public void testInvalidStartDate() {
    mockProperties.setProperty(TEST_ICEBERG_PARTITION_DATETTIME_STARTDATE, "invalid-date");
    verifyIllegalArgumentExceptionWithMessage("Invalid format");
  }

  @Test
  public void testMissingEndDate() {
    mockProperties.remove(TEST_ICEBERG_PARTITION_DATETTIME_ENDDATE);
    verifyIllegalArgumentExceptionWithMessage("DateTime Partition end date cannot be empty");
  }

  @Test
  public void testInvalidEndDate() {
    mockProperties.setProperty(TEST_ICEBERG_PARTITION_DATETTIME_ENDDATE, "invalid-date");
    verifyIllegalArgumentExceptionWithMessage("Invalid format");
  }

  @Test
  public void testWithDifferentPattern() {
    mockProperties.setProperty(TEST_ICEBERG_PARTITION_DATETTIME_PATTERN, "yyyy-MM-dd-HH");
    mockProperties.setProperty(TEST_ICEBERG_PARTITION_DATETTIME_STARTDATE, "2024-10-10-10");
    mockProperties.setProperty(TEST_ICEBERG_PARTITION_DATETTIME_ENDDATE, "2024-10-10-20");

    IcebergDateTimePartitionFilterPredicate predicate = new IcebergDateTimePartitionFilterPredicate(
        PARTITION_COLUMN_NAME,
        mockTableMetadata,
        mockProperties
    );

    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(String.class))).thenReturn("2024-10-10-09");
    Assert.assertFalse(predicate.test(mockPartition));

    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(String.class))).thenReturn("2024-10-10-10");
    Assert.assertTrue(predicate.test(mockPartition));

    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(String.class))).thenReturn("2024-10-10-15");
    Assert.assertTrue(predicate.test(mockPartition));

    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(String.class))).thenReturn("2024-10-10-20");
    Assert.assertTrue(predicate.test(mockPartition));

    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(String.class))).thenReturn("2024-10-10-21");
    Assert.assertFalse(predicate.test(mockPartition));
  }

  private void verifyIllegalArgumentExceptionWithMessage(String expectedMessageStart) {
    IllegalArgumentException exception = Assert.expectThrows(IllegalArgumentException.class, () -> {
      new IcebergDateTimePartitionFilterPredicate(PARTITION_COLUMN_NAME, mockTableMetadata, mockProperties);
    });
    Assert.assertTrue(exception.getMessage().startsWith(expectedMessageStart));
  }
}