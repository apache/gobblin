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

import org.apache.iceberg.StructLike;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests for {@link org.apache.gobblin.data.management.copy.iceberg.predicates.IcebergMatchesAnyPropNamePartitionFilterPredicate} */
public class IcebergMatchesAnyPropNamePartitionFilterPredicateTest {
  private static final String TEST_PARTITION_VALUE_1 = "value1";
  private IcebergMatchesAnyPropNamePartitionFilterPredicate predicate;

  @BeforeMethod
  public void setup() {
    predicate = new IcebergMatchesAnyPropNamePartitionFilterPredicate(0, TEST_PARTITION_VALUE_1);
  }

  @Test
  public void testPartitionValueNULL() {
    // Just mocking, so that the partition value is NULL
    Assert.assertFalse(predicate.test(Mockito.mock(StructLike.class)));
  }

  @Test
  public void testWhenPartitionIsNull() {
    Assert.assertFalse(predicate.test(null));
  }

  @Test
  public void testPartitionValueMatch() {
    StructLike mockPartition = Mockito.mock(StructLike.class);
    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(Object.class))).thenReturn("value1");
    Assert.assertTrue(predicate.test(mockPartition));
  }

  @Test
  public void testPartitionValueDoesNotMatch() {
    StructLike mockPartition = Mockito.mock(StructLike.class);
    Mockito.when(mockPartition.get(Mockito.anyInt(), Mockito.eq(Object.class))).thenReturn("<invalid_value>");
    Assert.assertFalse(predicate.test(mockPartition));
  }
}
