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

package org.apache.gobblin.service.modules.db;

import java.time.Duration;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariDataSource;

import javax.inject.Inject;
import javax.sql.DataSource;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import org.apache.gobblin.util.jdbc.MysqlDataSourceUtils;
import org.apache.gobblin.password.PasswordManager;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.util.ConfigUtils;


public class ServiceDatabaseProviderImpl implements ServiceDatabaseProvider {
  private static final Logger LOG = LoggerFactory.getLogger(ServiceDatabaseProviderImpl.class);

  private final Configuration configuration;
  private HikariDataSource dataSource;

  @Inject
  public ServiceDatabaseProviderImpl(Configuration configuration) {
    this.configuration = configuration;
  }

  public DataSource getDatasource() {
    ensureDataSource();
    return dataSource;
  }

  private synchronized void ensureDataSource() {
    if (dataSource != null) {
      return;
    }

    dataSource = new HikariDataSource();

    dataSource.setJdbcUrl(configuration.getUrl());
    dataSource.setUsername(configuration.getUserName());
    dataSource.setPassword(configuration.getPassword());

    // MySQL server can timeout a connection so need to validate connections before use
    final String validationQuery = MysqlDataSourceUtils.QUERY_CONNECTION_IS_VALID_AND_NOT_READONLY;
    LOG.info("setting `DataSource` validation query: '" + validationQuery + "'");
    // TODO: revisit following verification of successful connection pool migration:
    //   If your driver supports JDBC4 we strongly recommend not setting this property. This is for "legacy" drivers
    //   that do not support the JDBC4 Connection.isValid() API; see:
    //   https://github.com/brettwooldridge/HikariCP#gear-configuration-knobs-baby
    dataSource.setConnectionTestQuery(validationQuery);
    dataSource.setValidationTimeout(Duration.ofSeconds(5).toMillis());

    // To improve performance, we set a maximum connection lifetime
    // If database goes to read-only mode, then connection would not work correctly for up to configured lifetime
    dataSource.setMaxLifetime(configuration.getMaxConnectionLifetime().toMillis());
    dataSource.setIdleTimeout(Duration.ofSeconds(10).toMillis());
    dataSource.setMinimumIdle(2);
    dataSource.setMaximumPoolSize(configuration.getMaxConnections());
  }

  @Builder
  @AllArgsConstructor
  @Getter
  @NoArgsConstructor
  public static class Configuration {

    private String url;
    private String userName;
    private String password;

    @Builder.Default
    private Duration maxConnectionLifetime = Duration.ofMillis(-1);

    @Builder.Default
    private int maxConnections = 100;

    @Inject
    public Configuration(Config config) {
      this();
      Objects.requireNonNull(config, "Config cannot be null");

      url = config.getString(ServiceConfigKeys.SERVICE_DB_URL_KEY);

      PasswordManager passwordManager = PasswordManager.getInstance(ConfigUtils.configToProperties(config));

      userName = config.getString(ServiceConfigKeys.SERVICE_DB_USERNAME);
      password = passwordManager.readPassword(config.getString(ServiceConfigKeys.SERVICE_DB_PASSWORD));

      if(config.hasPath(ServiceConfigKeys.SERVICE_DB_MAX_CONNECTIONS)){
        maxConnections = config.getInt(ServiceConfigKeys.SERVICE_DB_MAX_CONNECTIONS);
      }

      if(config.hasPath(ServiceConfigKeys.SERVICE_DB_MAX_CONNECTION_LIFETIME)){
        maxConnectionLifetime = config.getDuration(ServiceConfigKeys.SERVICE_DB_MAX_CONNECTION_LIFETIME);
      }
    }
  }
}
