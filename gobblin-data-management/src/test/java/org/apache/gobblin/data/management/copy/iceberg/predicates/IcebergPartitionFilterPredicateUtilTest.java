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

import java.util.List;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.transforms.Transform;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

/** Tests for {@link org.apache.gobblin.data.management.copy.iceberg.predicates.IcebergPartitionFilterPredicateUtil} */
public class IcebergPartitionFilterPredicateUtilTest {
  private TableMetadata mockTableMetadata;
  private final List<String> supportedTransforms = ImmutableList.of("supported1", "supported2");

  private void setupMockData(String name, String transform) {
    mockTableMetadata = Mockito.mock(TableMetadata.class);

    PartitionSpec mockPartitionSpec = Mockito.mock(PartitionSpec.class);
    PartitionField mockPartitionField = Mockito.mock(PartitionField.class);
    Transform mockTransform = Mockito.mock(Transform.class);

    List<PartitionField> partitionFields = ImmutableList.of(mockPartitionField);

    Mockito.when(mockTableMetadata.spec()).thenReturn(mockPartitionSpec);
    Mockito.when(mockPartitionSpec.fields()).thenReturn(partitionFields);
    Mockito.when(mockPartitionField.name()).thenReturn(name);
    Mockito.when(mockPartitionField.transform()).thenReturn(mockTransform);
    Mockito.when(mockTransform.toString()).thenReturn(transform);
  }

  @Test
  public void testPartitionTransformNotSupported() {
    setupMockData("col1", "unsupported");
    IllegalArgumentException exception = Assert.expectThrows(IllegalArgumentException.class, () -> {
      IcebergPartitionFilterPredicateUtil.getPartitionColumnIndex("col1", mockTableMetadata, supportedTransforms);
    });
    Assert.assertEquals(exception.getMessage(), "Partition transform unsupported is not supported. Supported transforms are [supported1, supported2]");
  }

  @Test
  public void testPartitionTransformSupported() {
    setupMockData("col1", "supported1");
    int result = IcebergPartitionFilterPredicateUtil.getPartitionColumnIndex("col1", mockTableMetadata, supportedTransforms);
    Assert.assertEquals(result, 0);
  }

  @Test
  public void testPartitionColumnNotFound() {
    setupMockData("col", "supported1");
    int result = IcebergPartitionFilterPredicateUtil.getPartitionColumnIndex("col2", mockTableMetadata, supportedTransforms);
    Assert.assertEquals(result, -1);
  }

  @Test
  public void testPartitionColumnFoundIndex1() {
    mockTableMetadata = Mockito.mock(TableMetadata.class);
    PartitionSpec mockPartitionSpec = Mockito.mock(PartitionSpec.class);
    PartitionField mockPartitionField1 = Mockito.mock(PartitionField.class);
    PartitionField mockPartitionField2 = Mockito.mock(PartitionField.class);
    Transform mockTransform1 = Mockito.mock(Transform.class);
    Transform mockTransform2 = Mockito.mock(Transform.class);

    List<PartitionField> partitionFields = ImmutableList.of(mockPartitionField1, mockPartitionField2);

    Mockito.when(mockTableMetadata.spec()).thenReturn(mockPartitionSpec);
    Mockito.when(mockPartitionSpec.fields()).thenReturn(partitionFields);
    Mockito.when(mockPartitionField1.name()).thenReturn("col1");
    Mockito.when(mockPartitionField1.transform()).thenReturn(mockTransform1);
    Mockito.when(mockTransform1.toString()).thenReturn("supported1");
    Mockito.when(mockPartitionField2.name()).thenReturn("col2");
    Mockito.when(mockPartitionField2.transform()).thenReturn(mockTransform2);
    Mockito.when(mockTransform2.toString()).thenReturn("supported2");

    int result = IcebergPartitionFilterPredicateUtil.getPartitionColumnIndex("col2", mockTableMetadata, supportedTransforms);
    Assert.assertEquals(result, 1);
  }
}