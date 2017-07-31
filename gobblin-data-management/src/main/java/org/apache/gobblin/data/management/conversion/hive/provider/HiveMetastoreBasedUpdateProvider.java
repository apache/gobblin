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
package org.apache.gobblin.data.management.conversion.hive.provider;

import java.util.concurrent.TimeUnit;

import lombok.NoArgsConstructor;

import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

import org.apache.gobblin.annotation.Alpha;


/**
 * An update provider that uses update metadata from Hive metastore
 */
@Alpha
@NoArgsConstructor
public class HiveMetastoreBasedUpdateProvider implements HiveUnitUpdateProvider {

  @Override
  public long getUpdateTime(Partition partition) throws UpdateNotFoundException {
    // TODO if a table/partition is registered by gobblin an update time will be made available in table properties
    // Use the update time instead of create time
    return TimeUnit.MILLISECONDS.convert(partition.getTPartition().getCreateTime(), TimeUnit.SECONDS);
  }

  @Override
  public long getUpdateTime(Table table) throws UpdateNotFoundException {
    // TODO if a table/partition is registered by gobblin an update time will be made available in table properties
    // Use the update time instead of create time
    return TimeUnit.MILLISECONDS.convert(table.getTTable().getCreateTime(), TimeUnit.SECONDS);
  }

}
