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

import java.util.Objects;
import java.util.function.Predicate;

import org.apache.iceberg.StructLike;

/**
 * Predicate implementation for filtering Iceberg partitions based on specified partition values.
 * <p>
 * This class filters partitions by checking if the partition value matches any of the specified values.
 * </p>
 */
public class IcebergMatchesAnyPropNamePartitionFilterPredicate implements Predicate<StructLike> {
  private final int partitionColumnIndex;
  private final String partitionValue;

  /**
   * Constructs an {@code IcebergMatchesAnyPropNamePartitionFilterPredicate} with the specified parameters.
   *
   * @param partitionColumnIndex the index of the partition column in partition spec
   * @param partitionValue the partition value to match
   */
  public IcebergMatchesAnyPropNamePartitionFilterPredicate(int partitionColumnIndex, String partitionValue) {
    this.partitionColumnIndex = partitionColumnIndex;
    this.partitionValue = partitionValue;
  }

  /**
   * Check if the partition value matches any of the specified partition values.
   *
   * @param partition the partition to check
   * @return {@code true} if the partition value matches any of the specified values, otherwise {@code false}
   */
  @Override
  public boolean test(StructLike partition) {
    // Just a cautious check to avoid NPE, ideally partition shouldn't be null if table is partitioned
    if (Objects.isNull(partition)) {
      return false;
    }

    Object partitionVal = partition.get(this.partitionColumnIndex, Object.class);
    // Need this check to avoid NPE on partitionVal.toString()
    if (Objects.isNull(partitionVal)) {
      return false;
    }

    return this.partitionValue.equals(partitionVal.toString());
  }
}