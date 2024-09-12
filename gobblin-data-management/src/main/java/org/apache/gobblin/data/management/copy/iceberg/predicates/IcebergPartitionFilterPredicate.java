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
import java.util.function.Predicate;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableMetadata;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;


public class IcebergPartitionFilterPredicate implements Predicate<StructLike> {
  private static final List<String> allowedTransforms = ImmutableList.of("identity", "truncate");

  private int partitionColumnIndex;
  private final List<String> partitionValues;

  private static final Splitter LIST_SPLITTER = Splitter.on(",").trimResults().omitEmptyStrings();

  public IcebergPartitionFilterPredicate(String partitionColumnName, String partitionColumnValues, TableMetadata tableMetadata) {
    List<PartitionField> partitionFields = tableMetadata.spec().fields();
    for (int idx = 0; idx < partitionFields.size(); idx++) {
      PartitionField partitionField = partitionFields.get(idx);
      if (partitionField.name().equals(partitionColumnName)) {
        String transform = partitionField.transform().toString().toLowerCase();
        if (!allowedTransforms.contains(transform)) {
          throw new IllegalArgumentException(
              String.format("Partition transform %s is not supported. Supported transforms are %s", transform,
                  allowedTransforms));
        }
        this.partitionColumnIndex = idx;
        break;
      }
    }
    this.partitionValues = LIST_SPLITTER.splitToList(partitionColumnValues);
  }

  @Override
  public boolean test(StructLike partition) {
    String partitionVal = partition.get(this.partitionColumnIndex, String.class);
    return this.partitionValues.contains(partitionVal);
  }
}
