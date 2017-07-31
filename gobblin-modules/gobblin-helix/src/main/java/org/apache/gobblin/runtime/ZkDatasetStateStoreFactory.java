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
package org.apache.gobblin.runtime;

import com.typesafe.config.Config;
import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.DatasetStateStore;
import org.apache.gobblin.metastore.ZkStateStoreConfigurationKeys;
import org.apache.gobblin.util.ConfigUtils;

@Alias("zk")
public class ZkDatasetStateStoreFactory implements DatasetStateStore.Factory {
  @Override
  public DatasetStateStore<JobState.DatasetState> createStateStore(Config config) {

    String connectString = config.getString(ZkStateStoreConfigurationKeys.STATE_STORE_ZK_CONNECT_STRING_KEY);
    String rootDir = config.getString(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY);
    boolean compressedValues = ConfigUtils.getBoolean(config, ConfigurationKeys.STATE_STORE_COMPRESSED_VALUES_KEY,
        ConfigurationKeys.DEFAULT_STATE_STORE_COMPRESSED_VALUES);

    try {
      return new ZkDatasetStateStore(connectString, rootDir, compressedValues);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create ZkDatasetStateStore with factory", e);
    }
  }
}
