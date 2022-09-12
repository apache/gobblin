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
package org.apache.gobblin.data.management.copy.predicates;

import com.google.common.base.Optional;
import org.apache.gobblin.data.management.copy.hive.HivePartitionFileSet;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class ExistingPartitionSkipPredicateTest {
  ExistingPartitionSkipPredicate predicate = new ExistingPartitionSkipPredicate();

  @Test
  public void shouldSkipHiveDatasetWithExistingPartition() {
    HivePartitionFileSet fileSetWithExistingPartition = mock(HivePartitionFileSet.class);
    HivePartitionFileSet fileSetWithoutExistingPartition = mock(HivePartitionFileSet.class);
    Partition partition = mock(Partition.class);
    when(fileSetWithExistingPartition.getExistingTargetPartition()).thenReturn(Optional.of(partition));
    when(fileSetWithoutExistingPartition.getExistingTargetPartition()).thenReturn(Optional.absent());
    Assert.assertTrue(predicate.apply(fileSetWithExistingPartition));
    Assert.assertFalse(predicate.apply(fileSetWithoutExistingPartition));
  }
}
