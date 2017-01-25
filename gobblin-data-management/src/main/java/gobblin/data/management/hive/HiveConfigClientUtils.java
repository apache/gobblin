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
package gobblin.data.management.hive;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;

import gobblin.config.client.ConfigClient;
import gobblin.data.management.copy.hive.HiveDataset;

/**
 * Utility methods for a {@link HiveDataset} to communicate with {@link ConfigClient}
 */
public class HiveConfigClientUtils {

  private static final String HIVE_DATASETS_CONFIG_PREFIX = "hive" + Path.SEPARATOR;

  /**
   * Get the dataset uri for a hive db and table. The uri is relative to the store uri .
   * @param table the hive table for which a config client uri needs to be built
   */
  public static String getDatasetUri(Table table) {
    return HIVE_DATASETS_CONFIG_PREFIX + table.getDbName() + Path.SEPARATOR + table.getTableName();
  }
}
