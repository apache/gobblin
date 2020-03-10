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

package org.apache.gobblin.metastore;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.gobblin.annotation.Alias;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.ConfigUtils;

@Alias("fc")
public class FileContextBasedFsStateStoreFactory implements StateStore.Factory {
  @Override
  public <T extends State> StateStore<T> createStateStore(Config config, Class<T> stateClass) {
    // Add all job configuration properties so they are picked up by Hadoop
    Configuration conf = new Configuration();
    for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
      conf.set(entry.getKey(), entry.getValue().unwrapped().toString());
    }

    try {
      String stateStoreFsUri = ConfigUtils.getString(config, ConfigurationKeys.STATE_STORE_FS_URI_KEY,
          ConfigurationKeys.LOCAL_FS_URI);
      FileSystem stateStoreFs = FileSystem.get(URI.create(stateStoreFsUri), conf);
      String stateStoreRootDir = config.getString(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY);

      return new FileContextBasedFsStateStore<T>(stateStoreFs, stateStoreRootDir, stateClass);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create FsStateStore with factory", e);
    }

  }
}
