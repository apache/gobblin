/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.data.management.conversion.hive.mock;

import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.collect.Maps;

import gobblin.data.management.conversion.hive.provider.HiveUnitUpdateProvider;

public class MockUpdateProvider implements HiveUnitUpdateProvider {

  private final Map<String, Long> mock_updates_map;
  private MockUpdateProvider() {
    this.mock_updates_map = Maps.newHashMap();
  }


  @Override
  public long getUpdateTime(Partition partition) {
    if (this.mock_updates_map.containsKey(partition.getCompleteName())) {
      return this.mock_updates_map.get(partition.getCompleteName());
    }
    return 0;
  }

  @Override
  public long getUpdateTime(Table table) {
    if (this.mock_updates_map.containsKey(table.getCompleteName())) {
      return this.mock_updates_map.get(table.getCompleteName());
    }
    return 0;
  }

  public void addMockUpdateTime(String partitionOrTableCompleteName, long updateTime) {
    this.mock_updates_map.put(partitionOrTableCompleteName, Long.valueOf(updateTime));
  }

  private static MockUpdateProvider INSTANCE = new MockUpdateProvider();

  public static MockUpdateProvider getInstance() {
    return INSTANCE;
  }

}
