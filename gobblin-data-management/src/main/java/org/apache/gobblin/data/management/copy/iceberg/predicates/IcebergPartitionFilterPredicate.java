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
import java.util.Objects;
import java.util.Properties;
import java.util.function.Predicate;

import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableMetadata;

import com.google.common.base.Splitter;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.gobblin.data.management.copy.iceberg.IcebergDatasetFinder;

/**
 * Predicate implementation for filtering Iceberg partitions based on specified partition values.
 * <p>
 * This class filters partitions by checking if the partition value matches any of the specified values.
 * </p>
 */
public class IcebergPartitionFilterPredicate implements Predicate<StructLike> {
  private static final List<String> supportedTransforms = ImmutableList.of("identity", "truncate");
  private static final String ICEBERG_PARTITION_VALUES_KEY = "partition.values";
  private final int partitionColumnIndex;
  private final List<String> partitionValues;
  private static final Splitter LIST_SPLITTER = Splitter.on(",").trimResults().omitEmptyStrings();

  /**
   * Constructs an {@code IcebergPartitionFilterPredicate} with the specified parameters.
   *
   * @param partitionColumnName the name of the partition column
   * @param tableMetadata the metadata of the Iceberg table
   * @param properties the properties containing partition configuration
   * @throws IllegalArgumentException if the partition column is not found or required properties are missing
   */
  public IcebergPartitionFilterPredicate(String partitionColumnName, TableMetadata tableMetadata,
      Properties properties) {
    this.partitionColumnIndex = IcebergPartitionFilterPredicateUtil.getPartitionColumnIndex(partitionColumnName,
        tableMetadata, supportedTransforms);
    Preconditions.checkArgument(this.partitionColumnIndex != -1,
        String.format("Partition column %s not found", partitionColumnName));

    String partitionColumnValues =
        IcebergDatasetFinder.getLocationQualifiedProperty(properties, IcebergDatasetFinder.CatalogLocation.SOURCE,
            ICEBERG_PARTITION_VALUES_KEY);;
    Preconditions.checkArgument(StringUtils.isNotBlank(partitionColumnValues),
        "Partition column values cannot be empty");

    this.partitionValues = LIST_SPLITTER.splitToList(partitionColumnValues);
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

    return this.partitionValues.contains(partitionVal.toString());
  }
}