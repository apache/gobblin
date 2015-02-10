/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metastore;

import javax.sql.DataSource;
import java.util.Properties;

import org.apache.commons.dbcp.BasicDataSource;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;

import gobblin.configuration.ConfigurationKeys;


/**
 * A provider class for {@link javax.sql.DataSource}s.
 *
 * @author ynli
 */
public class DataSourceProvider implements Provider<DataSource> {

  private final BasicDataSource basicDataSource;

  @Inject
  public DataSourceProvider(@Named("dataSourceProperties") Properties properties) {
    this.basicDataSource = new BasicDataSource();
    basicDataSource.setDriverClassName(properties.getProperty(ConfigurationKeys.JOB_HISTORY_STORE_JDBC_DRIVER_KEY,
            ConfigurationKeys.DEFAULT_JOB_HISTORY_STORE_JDBC_DRIVER));
    basicDataSource.setUrl(properties.getProperty(ConfigurationKeys.JOB_HISTORY_STORE_URL_KEY));
    if (properties.containsKey(ConfigurationKeys.JOB_HISTORY_STORE_USER_KEY) && properties
        .containsKey(ConfigurationKeys.JOB_HISTORY_STORE_PASSWORD_KEY)) {
      basicDataSource.setUsername(properties.getProperty(ConfigurationKeys.JOB_HISTORY_STORE_USER_KEY));
      basicDataSource.setPassword(properties.getProperty(ConfigurationKeys.JOB_HISTORY_STORE_PASSWORD_KEY));
    }
  }

  @Override
  public DataSource get() {
    return this.basicDataSource;
  }
}
