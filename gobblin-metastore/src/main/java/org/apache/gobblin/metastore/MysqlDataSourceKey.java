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

import org.apache.gobblin.broker.iface.SharedResourceKey;

import lombok.Getter;

/**
 * {@link SharedResourceKey} for requesting {@link DataSource}s from a
 * {@link org.apache.gobblin.broker.iface.SharedResourceFactory}
 */
@Getter
public class MysqlDataSourceKey implements SharedResourceKey {
  private final String dataSourceName;
  private final Config config;

  /**
   * Constructs a key for the mysql data source. The dataSourceName is used as the key.
   * @param dataSourceName an identifier for the data source
   * @param config configuration that is passed along to configure the data source
   */
  public MysqlDataSourceKey(String dataSourceName, Config config) {
    this.dataSourceName = dataSourceName;
    this.config = config;
  }

  @Override
  public String toConfigurationKey() {
    return this.dataSourceName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MysqlDataSourceKey that = (MysqlDataSourceKey) o;

    return dataSourceName == null ?
        that.dataSourceName == null : dataSourceName.equals(that.dataSourceName);
  }

  @Override
  public int hashCode() {
    return dataSourceName != null ? dataSourceName.hashCode() : 0;
  }
}
