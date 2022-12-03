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

import javax.sql.DataSource;

import com.typesafe.config.Config;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.DatasetStateStore;
import org.apache.gobblin.metastore.MysqlDataSourceFactory;

@Alias("mysql")
public class MysqlDatasetStateStoreFactory implements DatasetStateStore.Factory {
  @Override
  public DatasetStateStore<JobState.DatasetState> createStateStore(Config config) {
    String stateStoreTableName = config.hasPath(ConfigurationKeys.STATE_STORE_DB_TABLE_KEY) ?
        config.getString(ConfigurationKeys.STATE_STORE_DB_TABLE_KEY) :
        ConfigurationKeys.DEFAULT_STATE_STORE_DB_TABLE;
    boolean compressedValues = config.hasPath(ConfigurationKeys.STATE_STORE_COMPRESSED_VALUES_KEY) ?
        config.getBoolean(ConfigurationKeys.STATE_STORE_COMPRESSED_VALUES_KEY) :
        ConfigurationKeys.DEFAULT_STATE_STORE_COMPRESSED_VALUES;

    try {
      DataSource dataSource = MysqlDataSourceFactory.get(config,
          SharedResourcesBrokerFactory.getImplicitBroker());

      return new MysqlDatasetStateStore(dataSource, stateStoreTableName, compressedValues);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create MysqlDatasetStateStore with factory", e);
    }
  }
}
