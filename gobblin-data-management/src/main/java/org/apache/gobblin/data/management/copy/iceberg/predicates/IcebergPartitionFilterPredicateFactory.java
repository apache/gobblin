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
import java.util.function.Predicate;
import org.apache.gobblin.data.management.copy.iceberg.IcebergDatasetFinder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableMetadata;

/**
 * Factory class for creating partition filter predicates for Iceberg tables.
 */
public class IcebergPartitionFilterPredicateFactory {
  private static final String ICEBERG_PARTITION_TYPE_KEY = "partition.type";
  private static final String DATETIME_PARTITION_TYPE = "datetime";

  /**
   * Creates a filter predicate for the given partition column name, table metadata, and properties.
   *
   * @param partitionColumnName the name of the partition column
   * @param tableMetadata the metadata of the Iceberg table
   * @param properties the properties containing partition type information
   * @return a {@link Predicate} for filtering partitions
   */
  public static Predicate<StructLike> getFilterPredicate(String partitionColumnName, TableMetadata tableMetadata,
      Properties properties) {
    if (DATETIME_PARTITION_TYPE.equals(IcebergDatasetFinder.getLocationQualifiedProperty(properties,
        IcebergDatasetFinder.CatalogLocation.SOURCE, ICEBERG_PARTITION_TYPE_KEY))) {
      return new IcebergDateTimePartitionFilterPredicate(partitionColumnName, tableMetadata, properties);
    }
    return new IcebergPartitionFilterPredicate(partitionColumnName, tableMetadata, properties);
  }
}
