/* (c) 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.compaction;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.DriverManager;
import java.util.Enumeration;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;


/**
 * A class for managing Hive JDBC connection.
 *
 * @author ziliu
 */
public class HiveJdbcConnector implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(HiveJdbcConnector.class);

  private static final String HIVESERVER_VERSION = "hiveserver.version";
  private static final Set<Integer> VALID_HIVESERVER_VERSIONS = ImmutableSet.<Integer>builder().add(1).add(2).build();
  private static final String HIVESERVER_VERSION_DEFAULT = "2";
  private static final String HIVESERVER_CONNECTION_STRING = "hiveserver.connection.string";
  private static final String HIVESERVER_URL = "hiveserver.connection.string";
  private static final String HIVESERVER_USER = "hiveserver.connection.string";
  private static final String HIVESERVER_PASSWORD = "hiveserver.connection.string";
  private static final String HIVE_JDBC_DRIVER_NAME = "org.apache.hadoop.hive.jdbc.HiveDriver";
  private static final String HIVE2_JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
  private static final String HIVE_EMBEDDED_CONNECTION_STRING = "jdbc:hive://";
  private static final String HIVE2_EMBEDDED_CONNECTION_STRING = "jdbc:hive2://";
  private static final String HIVESITE_DIR = "hivesite.dir";
  private static final String HIVE_CONFIG_KEY_PREFIX = "hive.";
  private static final int MAX_OUTPUT_STMT_LENGTH = 1000;

  private final Connection conn;
  private final Statement stmt;

  public HiveJdbcConnector() throws SQLException {

    addHiveSiteDirToClasspath();
    int hiveServerVersion = getHiveServerVersion();

    try {
      Class.forName(getJdbcDriverName(hiveServerVersion));
    } catch (ClassNotFoundException e) {
      LOG.error("No suitable driver found for HiveServer " + hiveServerVersion);
      throw new RuntimeException(e);
    }

    this.conn = getHiveConnection(getHiveServerVersion());
    this.stmt = this.conn.createStatement();
    setupHive();
  }

  private int getHiveServerVersion() {
    int hiveServerVersion =
        Integer.parseInt(CompactionRunner.properties.getProperty(HIVESERVER_VERSION, HIVESERVER_VERSION_DEFAULT));
    if (!VALID_HIVESERVER_VERSIONS.contains(hiveServerVersion)) {
      String message = hiveServerVersion + " is not a valid HiveServer version.";
      LOG.error(message);
      throw new RuntimeException(message);
    }
    return hiveServerVersion;
  }

  private String getJdbcDriverName(int hiveServerVersion) {
    if (hiveServerVersion == 1) {
      return HIVE_JDBC_DRIVER_NAME;
    } else if (hiveServerVersion == 2) {
      return HIVE2_JDBC_DRIVER_NAME;
    } else {
      String message = "Cannot find a suitable driver for HiveServer " + hiveServerVersion;
      LOG.error(message);
      throw new RuntimeException(message);
    }
  }

  private Connection getHiveConnection(int hiveServerVersion) throws SQLException {
    Connection connection = null;
    if (CompactionRunner.properties.containsKey(HIVESERVER_CONNECTION_STRING)) {
      String connectionString = CompactionRunner.properties.getProperty(HIVESERVER_CONNECTION_STRING);
      connection = DriverManager.getConnection(connectionString);
    } else if (CompactionRunner.properties.containsKey(HIVESERVER_URL)) {
      String url = CompactionRunner.properties.getProperty(HIVESERVER_URL);
      String user = CompactionRunner.properties.getProperty(HIVESERVER_USER, "");
      String password = CompactionRunner.properties.getProperty(HIVESERVER_PASSWORD, "");
      connection = DriverManager.getConnection(url, user, password);
    } else if (hiveServerVersion == 1) {
      connection = DriverManager.getConnection(HIVE_EMBEDDED_CONNECTION_STRING);
    } else {
      connection = DriverManager.getConnection(HIVE2_EMBEDDED_CONNECTION_STRING);
    }
    return connection;
  }

  private void addHiveSiteDirToClasspath() {
    if (CompactionRunner.properties.containsKey(HIVESITE_DIR)) {
      String hiveSiteDir = CompactionRunner.properties.getProperty(HIVESITE_DIR);
      LOG.info("Adding " + hiveSiteDir + " to CLASSPATH");
      File f = new File(hiveSiteDir);
      try {
        URL u = f.toURI().toURL();
        URLClassLoader urlClassLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
        Class<URLClassLoader> urlClass = URLClassLoader.class;
        Method method = urlClass.getDeclaredMethod("addURL", new Class[] { URL.class });
        method.setAccessible(true);
        method.invoke(urlClassLoader, new Object[] { u });
      } catch (Exception e) {
        LOG.error("Unable to add hive.site.dir to CLASSPATH");
        throw new RuntimeException(e);
      }
    }
  }

  private void setupHive() throws SQLException {
    Enumeration<?> enumeration = CompactionRunner.properties.propertyNames();
    while (enumeration.hasMoreElements()) {
      String propertyName = (String) enumeration.nextElement();
      if (propertyName.startsWith(HIVE_CONFIG_KEY_PREFIX)) {
        String str = "set " + propertyName + "=" + CompactionRunner.properties.getProperty(propertyName);
        LOG.info(str);
        stmt.execute(str);
      }
    }
  }

  public void executeStatements(String... statements) throws SQLException {
    for (String statement : statements) {
      LOG.info("RUNNING STATEMENT: " + choppedStatement(statement));
      stmt.execute(statement);
    }
  }

  private String choppedStatement(String statement) {
    if (statement.length() <= MAX_OUTPUT_STMT_LENGTH) {
      return statement;
    } else {
      return statement.substring(0, MAX_OUTPUT_STMT_LENGTH) + "...... ("
          + (statement.length() - MAX_OUTPUT_STMT_LENGTH) + " characters ommitted)";
    }
  }

  @Override
  public void close() throws IOException {
    if (stmt != null) {
      try {
        stmt.close();
      } catch (SQLException e) {
        throw new IOException(e);
      }
    }

    if (conn != null) {
      try {
        conn.close();
      } catch (SQLException e) {
        throw new IOException(e);
      }
    }
  }
}
