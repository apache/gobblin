/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util.jdbc;

import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;

import gobblin.password.PasswordManager;

/**
 * A provider class for {@link javax.sql.DataSource}s.
 */
public class DataSourceProvider implements Provider<DataSource> {

  public static final String GOBBLIN_UTIL_JDBC_PREFIX = "gobblin.util.jdbc.";
  public static final String CONN_DRIVER = GOBBLIN_UTIL_JDBC_PREFIX + "conn.driver";
  public static final String CONN_URL = GOBBLIN_UTIL_JDBC_PREFIX + "conn.url";

  public static final String USERNAME = GOBBLIN_UTIL_JDBC_PREFIX + "username";
  public static final String PASSWORD = GOBBLIN_UTIL_JDBC_PREFIX + "password";
  public static final String MAX_IDLE_CONNS = GOBBLIN_UTIL_JDBC_PREFIX + "max.idle.connections";
  public static final String MAX_ACTIVE_CONNS = GOBBLIN_UTIL_JDBC_PREFIX + "max.active.connections";
  public static final String DEFAULT_CONN_DRIVER = "com.mysql.jdbc.Driver";

  protected final BasicDataSource basicDataSource;

  @Inject
  public DataSourceProvider(@Named("dataSourceProperties") Properties properties) {
    this.basicDataSource = new BasicDataSource();
    basicDataSource.setDriverClassName(properties.getProperty(CONN_DRIVER, DEFAULT_CONN_DRIVER));
    basicDataSource.setUrl(properties.getProperty(CONN_URL));
    if (properties.containsKey(USERNAME) && properties.containsKey(PASSWORD)) {
      basicDataSource.setUsername(properties.getProperty(USERNAME));
      basicDataSource.setPassword(PasswordManager.getInstance(properties)
          .readPassword(properties.getProperty(PASSWORD)));
    }
    if (properties.containsKey(MAX_IDLE_CONNS)) {
      basicDataSource.setMaxIdle(Integer.parseInt(properties.getProperty(MAX_IDLE_CONNS)));
    }
    if (properties.containsKey(MAX_ACTIVE_CONNS)) {
      basicDataSource.setMaxActive(Integer.parseInt(properties.getProperty(MAX_ACTIVE_CONNS)));
    }
  }

  public DataSourceProvider() {
    this.basicDataSource = new BasicDataSource();
  }

  @Override
  public DataSource get() {
    return this.basicDataSource;
  }
}
