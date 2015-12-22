/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
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

import java.util.Properties;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import gobblin.configuration.ConfigurationKeys;
import gobblin.password.PasswordManager;


/**
 * This class extends {@link gobblin.util.jdbc.DataSourceProvider} with its own property keys.
 */
public class JobHistoryDataSourceProvider extends gobblin.util.jdbc.DataSourceProvider {

  @Inject
  public JobHistoryDataSourceProvider(@Named("dataSourceProperties") Properties properties) {
    basicDataSource.setDriverClassName(properties.getProperty(ConfigurationKeys.JOB_HISTORY_STORE_JDBC_DRIVER_KEY,
        ConfigurationKeys.DEFAULT_JOB_HISTORY_STORE_JDBC_DRIVER));
    basicDataSource.setUrl(properties.getProperty(ConfigurationKeys.JOB_HISTORY_STORE_URL_KEY));
    if (properties.containsKey(ConfigurationKeys.JOB_HISTORY_STORE_USER_KEY)
        && properties.containsKey(ConfigurationKeys.JOB_HISTORY_STORE_PASSWORD_KEY)) {
      basicDataSource.setUsername(properties.getProperty(ConfigurationKeys.JOB_HISTORY_STORE_USER_KEY));
      basicDataSource.setPassword(PasswordManager.getInstance(properties).readPassword(
          properties.getProperty(ConfigurationKeys.JOB_HISTORY_STORE_PASSWORD_KEY)));
    }
  }
}
