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

import java.util.Properties;

import org.apache.hadoop.hive.metastore.api.Table;

import com.google.common.base.Predicate;

import javax.annotation.Nullable;


/**
 * A predicate to check if a hive {@link Table} is of a certain type in {@link TABLE_TYPE}
 *
 * <p> Example usage: {@link org.apache.gobblin.data.management.copy.hive.HiveDatasetFinder#TABLE_FILTER}
 */
public class TableTypeFilter implements Predicate<Table> {

  public static final String FILTER_TYPE = "tableTypeFilter.type";

  private enum TABLE_TYPE {
    SNAPSHOT,
    PARTITIONED
  }

  private final TABLE_TYPE tableType;

  public TableTypeFilter(Properties props) {
    tableType = TABLE_TYPE.valueOf(
        props.getProperty(FILTER_TYPE, TABLE_TYPE.SNAPSHOT.name()).toUpperCase());
  }

  @Override
  public boolean apply(@Nullable Table input) {
    if (input == null) {
      return false;
    }

    switch (tableType) {
      case SNAPSHOT:
        return input.getPartitionKeys() == null || input.getPartitionKeys().size() == 0;
      case PARTITIONED:
        return input.getPartitionKeys() != null && input.getPartitionKeys().size() > 0;
      default:
        throw new UnsupportedOperationException("Invalid type: " + tableType);
    }
  }
}
