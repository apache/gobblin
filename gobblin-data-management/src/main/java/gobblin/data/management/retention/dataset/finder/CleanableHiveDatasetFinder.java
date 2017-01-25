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
package gobblin.data.management.retention.dataset.finder;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.api.Table;

import com.typesafe.config.Config;

import gobblin.config.client.ConfigClient;
import gobblin.data.management.copy.hive.HiveDatasetFinder;
import gobblin.data.management.retention.dataset.CleanableHiveDataset;
import gobblin.data.management.retention.dataset.ConfigurableCleanableDataset;


public class CleanableHiveDatasetFinder extends HiveDatasetFinder {

  public CleanableHiveDatasetFinder(FileSystem fs, Properties properties) throws IOException {
    super(fs, setConfigPrefix(properties));
  }

  public CleanableHiveDatasetFinder(FileSystem fs, Properties properties, ConfigClient configClient) throws IOException {
    super(fs, setConfigPrefix(properties), configClient);
  }

  protected CleanableHiveDataset createHiveDataset(Table table, Config datasetConfig) throws IOException {
    return new CleanableHiveDataset(super.fs, super.clientPool, new org.apache.hadoop.hive.ql.metadata.Table(table), super.properties, datasetConfig);
  }

  private static Properties setConfigPrefix(Properties props) {
    if (!props.containsKey(HIVE_DATASET_CONFIG_PREFIX_KEY)) {
      props.setProperty(HIVE_DATASET_CONFIG_PREFIX_KEY, ConfigurableCleanableDataset.RETENTION_CONFIGURATION_KEY);
    }
    return props;
  }
}
