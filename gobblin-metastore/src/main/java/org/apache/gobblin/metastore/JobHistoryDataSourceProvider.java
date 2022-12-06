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

import java.time.Duration;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.password.PasswordManager;
import org.apache.gobblin.util.jdbc.MysqlDataSourceUtils;


/**
 * This class extends {@link org.apache.gobblin.util.jdbc.DataSourceProvider} with its own property keys.
 */
public class JobHistoryDataSourceProvider extends org.apache.gobblin.util.jdbc.DataSourceProvider {
  private static final Logger LOG = LoggerFactory.getLogger(JobHistoryDataSourceProvider.class);

  @Inject
  public JobHistoryDataSourceProvider(@Named("dataSourceProperties") Properties properties) {
    this.dataSource.setDriverClassName(properties.getProperty(ConfigurationKeys.JOB_HISTORY_STORE_JDBC_DRIVER_KEY,
        ConfigurationKeys.DEFAULT_JOB_HISTORY_STORE_JDBC_DRIVER));

    // Set validation query to verify connection
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

    this.dataSource.setJdbcUrl(properties.getProperty(ConfigurationKeys.JOB_HISTORY_STORE_URL_KEY));
    // TODO: revisit following verification of successful connection pool migration:
    //   whereas `o.a.commons.dbcp.BasicDataSource` defaults min idle conns to 0, hikari defaults to 10.
    //   perhaps non-zero would have desirable runtime perf, but anything >0 currently fails unit tests (even 1!);
    //   (so experimenting with a higher number would first require adjusting tests)
    this.dataSource.setMinimumIdle(0);
    if (properties.containsKey(ConfigurationKeys.JOB_HISTORY_STORE_USER_KEY)
        && properties.containsKey(ConfigurationKeys.JOB_HISTORY_STORE_PASSWORD_KEY)) {
      this.dataSource.setUsername(properties.getProperty(ConfigurationKeys.JOB_HISTORY_STORE_USER_KEY));
      this.dataSource.setPassword(PasswordManager.getInstance(properties)
          .readPassword(properties.getProperty(ConfigurationKeys.JOB_HISTORY_STORE_PASSWORD_KEY)));
    }
  }
}
