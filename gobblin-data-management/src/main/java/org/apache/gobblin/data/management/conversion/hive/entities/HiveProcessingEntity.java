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

package org.apache.gobblin.data.management.conversion.hive.entities;

import org.apache.gobblin.data.management.copy.hive.HiveDataset;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.base.Optional;

import lombok.Getter;


/**
 * Represents a Hive table and optionally partition.
 */
@Getter
public class HiveProcessingEntity {

  private final HiveDataset hiveDataset;
  private final Table table;
  private final Optional<Partition> partition;

  public HiveProcessingEntity(HiveDataset hiveDataset, Table table) {
    this(hiveDataset, table, Optional.absent());
  }

  public HiveProcessingEntity(HiveDataset convertibleHiveDataset, Table table,
      Optional<Partition> partition) {
    this.hiveDataset = convertibleHiveDataset;
    this.table = table;
    this.partition = partition;
  }

}
