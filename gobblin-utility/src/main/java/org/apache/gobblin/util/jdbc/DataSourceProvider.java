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

import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;

import org.apache.gobblin.password.PasswordManager;


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
    this.basicDataSource.setDriverClassName(properties.getProperty(CONN_DRIVER, DEFAULT_CONN_DRIVER));
    this.basicDataSource.setUrl(properties.getProperty(CONN_URL));
    if (properties.containsKey(USERNAME) && properties.containsKey(PASSWORD)) {
      this.basicDataSource.setUsername(properties.getProperty(USERNAME));
      this.basicDataSource
          .setPassword(PasswordManager.getInstance(properties).readPassword(properties.getProperty(PASSWORD)));
    }
    if (properties.containsKey(MAX_IDLE_CONNS)) {
      this.basicDataSource.setMaxIdle(Integer.parseInt(properties.getProperty(MAX_IDLE_CONNS)));
    }
    if (properties.containsKey(MAX_ACTIVE_CONNS)) {
      this.basicDataSource.setMaxActive(Integer.parseInt(properties.getProperty(MAX_ACTIVE_CONNS)));
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
