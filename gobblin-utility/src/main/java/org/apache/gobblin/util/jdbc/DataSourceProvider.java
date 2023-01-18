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

package org.apache.gobblin.util.jdbc;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.gobblin.password.PasswordManager;


/**
 * A provider class for {@link javax.sql.DataSource}s.
 */
public class DataSourceProvider implements Provider<DataSource> {
  private static final Logger LOG = LoggerFactory.getLogger(DataSourceProvider.class);

  public static final String GOBBLIN_UTIL_JDBC_PREFIX = "gobblin.util.jdbc.";
  public static final String CONN_DRIVER = GOBBLIN_UTIL_JDBC_PREFIX + "conn.driver";
  public static final String CONN_URL = GOBBLIN_UTIL_JDBC_PREFIX + "conn.url";

  public static final String USERNAME = GOBBLIN_UTIL_JDBC_PREFIX + "username";
  public static final String PASSWORD = GOBBLIN_UTIL_JDBC_PREFIX + "password";
  public static final String SKIP_VALIDATION_QUERY = GOBBLIN_UTIL_JDBC_PREFIX + "skip.validation.query";
  public static final String MAX_ACTIVE_CONNS = GOBBLIN_UTIL_JDBC_PREFIX + "max.active.connections";
  public static final String DEFAULT_CONN_DRIVER = "com.mysql.jdbc.Driver";

  protected final HikariDataSource dataSource;

  @Inject
  public DataSourceProvider(@Named("dataSourceProperties") Properties properties) {
    this.dataSource = new HikariDataSource();
    this.dataSource.setDriverClassName(properties.getProperty(CONN_DRIVER, DEFAULT_CONN_DRIVER));
    // the validation query should work beyond mysql; still, to bypass for any reason, heed directive
    if (!Boolean.parseBoolean(properties.getProperty(SKIP_VALIDATION_QUERY, "false"))) {
      // MySQL server can timeout a connection so need to validate connections before use
      final String validationQuery = MysqlDataSourceUtils.QUERY_CONNECTION_IS_VALID_AND_NOT_READONLY;
      LOG.info("setting `DataSource` validation query: '" + validationQuery + "'");
      // TODO: revisit following verification of successful connection pool migration:
      //   If your driver supports JDBC4 we strongly recommend not setting this property. This is for "legacy" drivers
      //   that do not support the JDBC4 Connection.isValid() API; see:
      //   https://github.com/brettwooldridge/HikariCP#gear-configuration-knobs-baby
      this.dataSource.setConnectionTestQuery(validationQuery);
      this.dataSource.setIdleTimeout(Duration.ofSeconds(60).toMillis());
    }
    this.dataSource.setJdbcUrl(properties.getProperty(CONN_URL));
    // TODO: revisit following verification of successful connection pool migration:
    //   whereas `o.a.commons.dbcp.BasicDataSource` defaults min idle conns to 0, hikari defaults to 10.
    //   perhaps non-zero would have desirable runtime perf, but anything >0 currently fails unit tests (even 1!);
    //   (so experimenting with a higher number would first require adjusting tests)
    this.dataSource.setMinimumIdle(0);
    if (properties.containsKey(USERNAME) && properties.containsKey(PASSWORD)) {
      this.dataSource.setUsername(properties.getProperty(USERNAME));
      this.dataSource
          .setPassword(PasswordManager.getInstance(properties).readPassword(properties.getProperty(PASSWORD)));
    }
    if (properties.containsKey(MAX_ACTIVE_CONNS)) {
      this.dataSource.setMaximumPoolSize(Integer.parseInt(properties.getProperty(MAX_ACTIVE_CONNS)));
    }
  }

  public DataSourceProvider() {
    LOG.warn("Creating {} without setting validation query.\n Stacktrace of current thread {}",
        this.getClass().getSimpleName(),
        Arrays.toString(Thread.currentThread().getStackTrace()).replace(", ", "\n  at "));
    this.dataSource = new HikariDataSource();
  }

  @Override
  public DataSource get() {
    return this.dataSource;
  }
}
