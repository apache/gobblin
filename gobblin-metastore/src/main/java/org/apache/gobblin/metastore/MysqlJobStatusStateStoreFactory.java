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

import javax.sql.DataSource;

import com.typesafe.config.Config;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.ConfigUtils;


public class MysqlJobStatusStateStoreFactory extends MysqlStateStoreFactory implements DatasetStateStore.Factory {
  @Override
  public <T extends State> MysqlJobStatusStateStore<T> createStateStore(Config config, Class<T> stateClass) {
    String stateStoreTableName = ConfigUtils.getString(config, ConfigurationKeys.STATE_STORE_DB_TABLE_KEY,
        ConfigurationKeys.DEFAULT_STATE_STORE_DB_TABLE);
    boolean compressedValues = ConfigUtils.getBoolean(config, ConfigurationKeys.STATE_STORE_COMPRESSED_VALUES_KEY,
        ConfigurationKeys.DEFAULT_STATE_STORE_COMPRESSED_VALUES);

    try {
      DataSource dataSource = MysqlDataSourceFactory.get(config,
          SharedResourcesBrokerFactory.getImplicitBroker());

      return new MysqlJobStatusStateStore<>(dataSource, stateStoreTableName, compressedValues, stateClass);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create MysqlStateStore with factory", e);
    }
  }

  @Override
  public <T extends State> MysqlJobStatusStateStore createStateStore(Config config) {
    return createStateStore(config, State.class);
  }
}
