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

package gobblin.metastore.testing;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import com.google.common.base.Optional;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.wix.mysql.EmbeddedMysql;
import com.wix.mysql.config.MysqldConfig;
import com.wix.mysql.distribution.Version;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metastore.MetaStoreModule;
import gobblin.metastore.util.DatabaseJobHistoryStoreSchemaManager;
import gobblin.metastore.util.MySqlJdbcUrl;


class TestMetastoreDatabaseServer implements Closeable {
  private static final String INFORMATION_SCHEMA = "information_schema";
  private static final String ROOT_USER = "root";
  private static final String DROP_DATABASE_TEMPLATE = "DROP DATABASE IF EXISTS %s;";
  private static final String CREATE_DATABASE_TEMPLATE = "CREATE DATABASE %s CHARACTER SET = %s COLLATE = %s;";
  private static final String ADD_USER_TEMPLATE = "GRANT ALL ON %s.* TO '%s'@'%%';";
  private static final String USER = "testUser";
  private static final String PASSWORD = "testPassword";
  private final MysqldConfig config;
  private final EmbeddedMysql testingMySqlServer;

  TestMetastoreDatabaseServer() throws Exception {
    config = MysqldConfig.aMysqldConfig(Version.v5_6_latest)
        .withPort(chooseRandomPort())
        .withUser(USER, PASSWORD)
        .build();
    testingMySqlServer = EmbeddedMysql.anEmbeddedMysql(config).start();
  }

  public void drop(String database) throws SQLException, URISyntaxException {
    Optional<Connection> connectionOptional = Optional.absent();
    try {
      connectionOptional = getConnector(getInformationSchemaJdbcUrl());
      Connection connection = connectionOptional.get();
      executeStatement(connection, String.format(DROP_DATABASE_TEMPLATE, database));
    } finally {
      if (connectionOptional.isPresent()) {
        connectionOptional.get().close();
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (testingMySqlServer != null) {
      testingMySqlServer.stop();
    }
  }

  private int chooseRandomPort() throws IOException {
    ServerSocket socket = null;
    try {
      socket = new ServerSocket(0);
      return socket.getLocalPort();
    } finally {
      if (socket != null) {
        socket.close();
      }
    }
  }

  MySqlJdbcUrl getJdbcUrl(String database) throws URISyntaxException {
    return getBaseJdbcUrl()
        .setPath(database)
        .setUser(USER)
        .setPassword(PASSWORD)
        .setParameter("useLegacyDatetimeCode", "false")
        .setParameter("rewriteBatchedStatements", "true");
  }

  private MySqlJdbcUrl getBaseJdbcUrl() throws URISyntaxException {
    return MySqlJdbcUrl.create()
        .setHost("localhost")
        .setPort(config.getPort());
  }

  private MySqlJdbcUrl getInformationSchemaJdbcUrl() throws URISyntaxException {
    return getBaseJdbcUrl()
        .setPath(INFORMATION_SCHEMA)
        .setUser(ROOT_USER);
  }

  private Optional<Connection> getConnector(MySqlJdbcUrl jdbcUrl) throws SQLException {
    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.JOB_HISTORY_STORE_URL_KEY, jdbcUrl.toString());
    Injector injector = Guice.createInjector(new MetaStoreModule(properties));
    DataSource dataSource = injector.getInstance(DataSource.class);
    return Optional.of(dataSource.getConnection());
  }

  private void ensureDatabaseExists(String database) throws SQLException, URISyntaxException {
    Optional<Connection> connectionOptional = Optional.absent();
    try {
      connectionOptional = getConnector(getInformationSchemaJdbcUrl());
      Connection connection = connectionOptional.get();
      executeStatements(connection,
          String.format(DROP_DATABASE_TEMPLATE, database),
          String.format(CREATE_DATABASE_TEMPLATE, database,
                 config.getCharset().getCharset(), config.getCharset().getCollate()),
          String.format(ADD_USER_TEMPLATE, database, config.getUsername()));
    } finally {
      if (connectionOptional.isPresent()) {
        connectionOptional.get().close();
      }
    }
  }

  void prepareDatabase(String database, String version) throws Exception {
    // Drop/create the database
    this.ensureDatabaseExists(database);

    // Deploy the schema
    DatabaseJobHistoryStoreSchemaManager schemaManager =
        DatabaseJobHistoryStoreSchemaManager.builder()
            .setDataSource(getJdbcUrl(database).toString(), USER, PASSWORD)
            .setVersion(version)
            .build();
    schemaManager.migrate();
  }

  private void executeStatements(Connection connection, String... statements) throws SQLException {
    for (String statement : statements) {
      executeStatement(connection, statement);
    }
  }

  private void executeStatement(Connection connection, String statement) throws SQLException {
    try (PreparedStatement preparedStatement = connection.prepareStatement(statement)) {
      preparedStatement.execute();
    }
  }
}
