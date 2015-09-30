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

package gobblin.util.jdbc;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import gobblin.password.PasswordManager;


/**
 * Maintains a {@link com.google.common.cache.Cache} from {@link String} of connection related attributes to {@link MySqlDb}.
 */
public class MySqlDbCache {

  private static final int DEFAULT_MAX_CACHE_SIZE = 100;
  private static final Cache<String, MySqlDb> MYSQL_DB_CONNECTION_CACHE = CacheBuilder.newBuilder()
      .maximumSize(DEFAULT_MAX_CACHE_SIZE).build();

  private static final String GOBBLIN_UTIL_MYSQL_PREFIX = "gobblin.util.mysql.";
  private static final String MYSQL_CONN_DRIVER = GOBBLIN_UTIL_MYSQL_PREFIX + "conn.driver";
  private static final String MYSQL_HOST = GOBBLIN_UTIL_MYSQL_PREFIX + "host";
  private static final String MYSQL_PORT = GOBBLIN_UTIL_MYSQL_PREFIX + "port";
  private static final String MYSQL_DB = GOBBLIN_UTIL_MYSQL_PREFIX + "database";
  private static final String MYSQL_USERNAME = GOBBLIN_UTIL_MYSQL_PREFIX + "username";
  private static final String MYSQL_PASSWORD = GOBBLIN_UTIL_MYSQL_PREFIX + "password";

  private static final String MYSQL_MAX_IDLE_CONNS = GOBBLIN_UTIL_MYSQL_PREFIX + "max.idle.connections";
  private static final String MYSQL_MAX_ACTIVE_CONNS = GOBBLIN_UTIL_MYSQL_PREFIX + "max.active.connections";
  private static final String MYSQL_USE_COMPRESSION = GOBBLIN_UTIL_MYSQL_PREFIX + "useCompression";
  private static final String DEFAULT_MYSQL_MAX_IDLE_CONNS = "100";
  private static final String DEFAULT_MYSQL_MAX_ACTIVE_CONNS = "100";
  private static final String DEFAULT_MYSQL_USE_COMPRESSION = Boolean.FALSE.toString();

  /**
   * Retrieves a {@link MySqlDb} from Cache using {@link Properties}.
   */
  public static MySqlDb getMySqlDb(Properties props) throws ExecutionException {
    return getMySqlDb(props.getProperty(MYSQL_CONN_DRIVER), props.getProperty(MYSQL_HOST),
        props.getProperty(MYSQL_PORT), props.getProperty(MYSQL_DB), props.getProperty(MYSQL_USERNAME), PasswordManager
            .getInstance(props).readPassword(props.getProperty(MYSQL_PASSWORD)), Boolean.parseBoolean(props
            .getProperty(MYSQL_USE_COMPRESSION, DEFAULT_MYSQL_USE_COMPRESSION)), Integer.parseInt(props.getProperty(
            MYSQL_MAX_IDLE_CONNS, DEFAULT_MYSQL_MAX_IDLE_CONNS)), Integer.parseInt(props.getProperty(
            MYSQL_MAX_ACTIVE_CONNS, DEFAULT_MYSQL_MAX_ACTIVE_CONNS)));
  }

  /**
   * Retrieves a {@link MySqlDb} from Cache using connection attributes.
   */
  public static MySqlDb getMySqlDb(final String driver, final String host, final String port, final String database,
      final String username, final String password, final boolean useCompression, final int maxIdleConns,
      final int maxActiveConns) throws ExecutionException {
    return MYSQL_DB_CONNECTION_CACHE.get(getKeyForDbCache(driver, host, port, database, username, password),
        new Callable<MySqlDb>() {
          @Override
          public MySqlDb call() throws Exception {
            return MySqlDb.builder().driver(driver).host(host).port(port).database(database)
                .useCompression(useCompression).username(username).password(password).maxIdleConns(maxIdleConns)
                .maxActiveConns(maxActiveConns).build();
          }
        });
  }

  /**
   * Returns key constructed from {driver, host, port, database, username, password}.
   */
  private static String getKeyForDbCache(String driver, String host, String port, String database, String username,
      String password) {
    return new StringBuilder().append(driver).append(host).append(port).append(database).append(username)
        .append(password).toString();
  }
}
