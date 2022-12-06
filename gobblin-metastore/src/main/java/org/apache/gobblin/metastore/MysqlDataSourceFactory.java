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

import javax.sql.DataSource;

import com.typesafe.config.Config;

import org.apache.gobblin.broker.ResourceInstance;
import org.apache.gobblin.broker.iface.ConfigView;
import org.apache.gobblin.broker.iface.NotConfiguredException;
import org.apache.gobblin.broker.iface.ScopeType;
import org.apache.gobblin.broker.iface.ScopedConfigView;
import org.apache.gobblin.broker.iface.SharedResourceFactory;
import org.apache.gobblin.broker.iface.SharedResourceFactoryResponse;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;

import lombok.extern.slf4j.Slf4j;

/**
 * A {@link SharedResourceFactory} for creating {@link DataSource}s.
 *
 * The factory creates a {@link DataSource} with the config.
 */
@Slf4j
public class MysqlDataSourceFactory<S extends ScopeType<S>>
    implements SharedResourceFactory<DataSource, MysqlDataSourceKey, S> {

  // WARNING: now a misnomer, but retained for legacy compatibility, despite move from `o.a.commons.dbcp.BasicDataSource` to `HikariCP`
  public static final String FACTORY_NAME = "basicDataSource";

  /**
   * Get a {@link DataSource} based on the config
   * @param config configuration
   * @param broker broker
   * @return a {@link DataSource}
   * @throws IOException
   */
  public static <S extends ScopeType<S>> DataSource get(Config config,
      SharedResourcesBroker<S> broker) throws IOException {
    try {
      return broker.getSharedResource(new MysqlDataSourceFactory<S>(),
          new MysqlDataSourceKey(MysqlStateStore.getDataSourceId(config), config));
    } catch (NotConfiguredException nce) {
      throw new IOException(nce);
    }
  }

  @Override
  public String getName() {
    return FACTORY_NAME;
  }

  @Override
  public SharedResourceFactoryResponse<DataSource> createResource(SharedResourcesBroker<S> broker,
    ScopedConfigView<S, MysqlDataSourceKey> config) throws NotConfiguredException {
    MysqlDataSourceKey key = config.getKey();
    Config configuration = key.getConfig();

    DataSource dataSource = MysqlStateStore.newDataSource(configuration);

    return new ResourceInstance<>(dataSource);
  }

  @Override
  public S getAutoScope(SharedResourcesBroker<S> broker, ConfigView<S, MysqlDataSourceKey> config) {
    return broker.selfScope().getType().rootScope();
  }
}
