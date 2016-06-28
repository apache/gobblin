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
package gobblin.data.management.hive;

import org.apache.hadoop.fs.Path;

import gobblin.config.client.ConfigClient;
import gobblin.data.management.copy.hive.HiveDataset;
import gobblin.data.management.copy.hive.HiveDatasetFinder.DbAndTable;

/**
 * Utility methods to for a {@link HiveDataset} to communicate with {@link ConfigClient}
 */
public class HiveConfigClientUtils {

  private static final String HIVE_DATASETS_CONFIG_PREFIX = "hive/";

  /**
   * Get the dataset uri for a hive db and table. The uri is relative to the store uri .
   * @param dbAndTable
   */
  public static String getDatasetUri(DbAndTable dbAndTable) {
    return HIVE_DATASETS_CONFIG_PREFIX + dbAndTable.getDb() + Path.SEPARATOR + dbAndTable.getTable();
  }
}
