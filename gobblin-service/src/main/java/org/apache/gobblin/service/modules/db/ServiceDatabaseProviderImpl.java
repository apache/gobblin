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

import org.apache.commons.dbcp2.BasicDataSource;

import com.typesafe.config.Config;

import javax.inject.Inject;
import javax.sql.DataSource;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import org.apache.gobblin.password.PasswordManager;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.util.ConfigUtils;


public class ServiceDatabaseProviderImpl implements ServiceDatabaseProvider {

  private final Configuration configuration;
  private BasicDataSource dataSource;

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

    dataSource = new BasicDataSource();

    dataSource.setUrl(configuration.getUrl());
    dataSource.setUsername(configuration.getUserName());
    dataSource.setPassword(configuration.getPassword());

    // MySQL server can timeout a connection so we need to validate connections.
    // Also checking that the DB is not readonly - https://stackoverflow.com/q/39552146
    dataSource.setValidationQuery("select case when @@read_only + @@innodb_read_only = 0 then 1 else "
                                      + "(select table_name from information_schema.tables) end as `1`");
    dataSource.setValidationQueryTimeout(5);

    // To improve performance, we only check connections on creation, and set a maximum connection lifetime
    // If database goes to read-only mode, then connection would not work correctly for up to configured lifetime
    dataSource.setTestOnCreate(true);
    dataSource.setMaxConnLifetimeMillis(configuration.getMaxConnectionLifetime().toMillis());
    dataSource.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(10).toMillis());
    dataSource.setMinIdle(2);
    dataSource.setMaxTotal(configuration.getMaxConnections());
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
    private Duration maxConnectionLifetime = Duration.ofMinutes(1);

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
