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

import java.io.Closeable;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * This class provides functions to query a MySql database.
 * It contains a connection {@link JdbcDataSource} to connect to MySql database, and provides interfaces to run MySql query.
 */
@Slf4j
@Getter
public class MySqlDb implements Closeable {

  private JdbcDataSource jdbcSource;

  private String driver;
  private String host;
  private String port;
  private String database;
  private boolean useCompression;
  private String username;
  private String password;
  private int maxIdleConns;
  private int maxActiveConns;

  @Builder
  public MySqlDb(String driver, String host, String port, String database, String username, String password,
      boolean useCompression, int maxIdleConns, int maxActiveConns) {
    this.driver = driver;
    this.host = host;
    this.port = port;
    this.database = database;
    this.username = username;
    this.password = password;
    this.useCompression = useCompression;
    this.maxIdleConns = maxIdleConns;
    this.maxActiveConns = maxActiveConns;
  }

  /**
   * Get jdbcSource. If the current jdbcSource is null or closed, open a new one.
   */
  private JdbcDataSource getJdbcSource() {
    if (this.jdbcSource == null || this.jdbcSource.isClosed()) {
      this.jdbcSource =
          new JdbcDataSource(this.driver, this.getConnectionUrl(), this.username, this.password, this.maxIdleConns,
              this.maxActiveConns);
    }
    return this.jdbcSource;
  }

  private String getConnectionUrl() {
    String url = "jdbc:mysql://" + host.trim() + ":" + port + "/" + database.trim();
    if (this.useCompression) {
      return url + "?useCompression=true";
    }
    return url;
  }

  /**
   * Execute a given SQL query.
   */
  public ResultSet executeQuery(String query) throws SQLException {
    ResultSet resultSet = null;
      Statement statement = this.getJdbcSource().getConnection().createStatement();
      final boolean status = statement.execute(query);
      if (status == false) {
        log.error("Failed to execute sql:" + query);
      }
      resultSet = statement.getResultSet();
    return resultSet;
  }

  @Override
  public void close() throws IOException {
    try {
      this.jdbcSource.close();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }
}
