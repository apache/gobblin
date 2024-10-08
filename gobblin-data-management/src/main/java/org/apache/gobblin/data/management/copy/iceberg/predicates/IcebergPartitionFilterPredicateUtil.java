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
import org.apache.iceberg.TableMetadata;

/**
 * Utility class for creating and managing partition filter predicates for Iceberg tables.
 * <p>
 * This class provides methods to retrieve the index of a partition column in the table metadata
 * and ensures that the partition transform is supported.
 * </p>
 * <p>
 * Note: This class is not meant to be instantiated.
 * </p>
 */
public class IcebergPartitionFilterPredicateUtil {
  private IcebergPartitionFilterPredicateUtil() {
  }

  /**
   * Retrieves the index of the partition column from the partition spec in the table metadata.
   *
   * @param partitionColumnName the name of the partition column to find
   * @param tableMetadata the metadata of the Iceberg table
   * @param supportedTransforms a list of supported partition transforms
   * @return the index of the partition column if found, otherwise -1
   * @throws IllegalArgumentException if the partition transform is not supported
   */
  public static int getPartitionColumnIndex(
      String partitionColumnName,
      TableMetadata tableMetadata,
      List<String> supportedTransforms
  ) {
    List<PartitionField> partitionFields = tableMetadata.spec().fields();
    for (int idx = 0; idx < partitionFields.size(); idx++) {
      PartitionField partitionField = partitionFields.get(idx);
      if (partitionField.name().equals(partitionColumnName)) {
        String transform = partitionField.transform().toString().toLowerCase();
        if (!supportedTransforms.contains(transform)) {
          throw new IllegalArgumentException(
              String.format(" For ~{%s:%d}~ Partition transform %s is not supported. Supported transforms are %s",
                  partitionColumnName,
                  idx,
                  transform,
                  supportedTransforms));
        }
        return idx;
      }
    }
    return -1;
  }
}