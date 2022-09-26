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
    this.basicDataSource.setDriverClassName(properties.getProperty(ConfigurationKeys.JOB_HISTORY_STORE_JDBC_DRIVER_KEY,
        ConfigurationKeys.DEFAULT_JOB_HISTORY_STORE_JDBC_DRIVER));

    // Set validation query to verify connection
    if (!Boolean.parseBoolean(properties.getProperty(SKIP_VALIDATION_QUERY, "false"))) {
      // MySQL server can timeout a connection so need to validate connections before use
      final String validationQuery = MysqlDataSourceUtils.QUERY_CONNECTION_IS_VALID_AND_NOT_READONLY;
      LOG.info("setting `DataSource` validation query: '" + validationQuery + "'");
      this.basicDataSource.setValidationQuery(validationQuery);
      this.basicDataSource.setTestOnBorrow(true);
      this.basicDataSource.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(60).toMillis());
    }

    this.basicDataSource.setUrl(properties.getProperty(ConfigurationKeys.JOB_HISTORY_STORE_URL_KEY));
    if (properties.containsKey(ConfigurationKeys.JOB_HISTORY_STORE_USER_KEY)
        && properties.containsKey(ConfigurationKeys.JOB_HISTORY_STORE_PASSWORD_KEY)) {
      this.basicDataSource.setUsername(properties.getProperty(ConfigurationKeys.JOB_HISTORY_STORE_USER_KEY));
      this.basicDataSource.setPassword(PasswordManager.getInstance(properties)
          .readPassword(properties.getProperty(ConfigurationKeys.JOB_HISTORY_STORE_PASSWORD_KEY)));
    }
  }
}
