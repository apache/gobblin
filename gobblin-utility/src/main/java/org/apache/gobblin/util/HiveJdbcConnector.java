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

package org.apache.gobblin.util;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import org.apache.gobblin.password.PasswordManager;


/**
 * A class for managing a Hive JDBC connection.
 *
 * @author Ziyang Liu
 */
public class HiveJdbcConnector implements Closeable {

  public static final String HIVESERVER_VERSION = "hiveserver.version";
  public static final String DEFAULT_HIVESERVER_VERSION = "2";
  public static final String HIVESERVER_CONNECTION_STRING = "hiveserver.connection.string";
  public static final String HIVESERVER_URL = "hiveserver.url";
  public static final String HIVESERVER_USER = "hiveserver.user";
  public static final String HIVESERVER_PASSWORD = "hiveserver.password";
  public static final String HIVESITE_DIR = "hivesite.dir";
  public static final String HIVE_EXECUTION_SIMULATE = "hive.execution.simulate";

  private static final String HIVE_JDBC_DRIVER_NAME = "org.apache.hadoop.hive.jdbc.HiveDriver";
  private static final String HIVE2_JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
  private static final String HIVE_EMBEDDED_CONNECTION_STRING = "jdbc:hive://";
  private static final String HIVE2_EMBEDDED_CONNECTION_STRING = "jdbc:hive2://";
  private static final String HIVE_CONFIG_KEY_PREFIX = "hive.";
  private static final int MAX_OUTPUT_STMT_LENGTH = 1000;

  private static final Logger LOG = LoggerFactory.getLogger(HiveJdbcConnector.class);

  // Connection to the Hive DB
  private Connection conn;

  private int hiveServerVersion;

  private boolean isSimulate;

  private HiveJdbcConnector() {
    this(false);
  }

  private HiveJdbcConnector(boolean isSimulate) {
    this.isSimulate = isSimulate;
  }

  /**
   * Create a new {@link HiveJdbcConnector} using the specified Hive server version.
   * @param hiveServerVersion is the Hive server version to use
   * @return a HiveJdbcConnector with the specified hiveServerVersion
   * @throws SQLException
   */
  @SuppressWarnings("resource")
  public static HiveJdbcConnector newEmbeddedConnector(int hiveServerVersion) throws SQLException {
    return new HiveJdbcConnector().withHiveServerVersion(hiveServerVersion).withHiveEmbeddedConnection();
  }

  /**
   * Create a new {@link HiveJdbcConnector} based on the configs in a {@link Properties} object
   * @param compactRunProps contains the configuration keys to construct the {@link HiveJdbcConnector}
   * @throws SQLException if there is a problem setting up the JDBC connection
   * @return
   */
  public static HiveJdbcConnector newConnectorWithProps(Properties compactRunProps) throws SQLException {

    boolean isSimulate = Boolean.valueOf(compactRunProps.getProperty(HIVE_EXECUTION_SIMULATE));

    HiveJdbcConnector hiveJdbcConnector = new HiveJdbcConnector(isSimulate);

    // Set the Hive Server type
    hiveJdbcConnector.withHiveServerVersion(
        Integer.parseInt(compactRunProps.getProperty(HIVESERVER_VERSION, DEFAULT_HIVESERVER_VERSION)));

    // Add the Hive Site Dir to the classpath
    if (compactRunProps.containsKey(HIVESITE_DIR)) {
      HiveJdbcConnector.addHiveSiteDirToClasspath(compactRunProps.getProperty(HIVESITE_DIR));
    }

    // Set and create the Hive JDBC connection
    if (compactRunProps.containsKey(HIVESERVER_CONNECTION_STRING)) {
      hiveJdbcConnector.withHiveConnectionFromUrl(compactRunProps.getProperty(HIVESERVER_CONNECTION_STRING));
    } else if (compactRunProps.containsKey(HIVESERVER_URL)) {
      hiveJdbcConnector.withHiveConnectionFromUrlUserPassword(compactRunProps.getProperty(HIVESERVER_URL),
          compactRunProps.getProperty(HIVESERVER_USER),
          PasswordManager.getInstance(compactRunProps).readPassword(compactRunProps.getProperty(HIVESERVER_PASSWORD)));
    } else {
      hiveJdbcConnector.withHiveEmbeddedConnection();
    }

    // Set Hive properties
    hiveJdbcConnector.setHiveProperties(compactRunProps);

    return hiveJdbcConnector;
  }

  /**
   * Set the {@link HiveJdbcConnector#hiveServerVersion}. The hiveServerVersion must be either 1 or 2.
   * @param hiveServerVersion
   * @return
   */
  private HiveJdbcConnector withHiveServerVersion(int hiveServerVersion) {
    try {
      if (hiveServerVersion == 1) {
        Class.forName(HIVE_JDBC_DRIVER_NAME);
      } else if (hiveServerVersion == 2) {
        Class.forName(HIVE2_JDBC_DRIVER_NAME);
      } else {
        throw new RuntimeException(hiveServerVersion + " is not a valid HiveServer version.");
      }
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Cannot find a suitable driver of HiveServer version " + hiveServerVersion + ".", e);
    }
    this.hiveServerVersion = hiveServerVersion;
    return this;
  }

  /**
   * Set the {@link HiveJdbcConnector#conn} based on the URL given to the method
   * @param hiveServerUrl is the URL to connect to
   * @throws SQLException if there is a problem connectiong to the URL
   * @return
   */
  private HiveJdbcConnector withHiveConnectionFromUrl(String hiveServerUrl) throws SQLException {
    this.conn = DriverManager.getConnection(hiveServerUrl);
    return this;
  }

  /**
   * Set the {@link HiveJdbcConnector#conn} based on the URL, user, and password given to the method
   * @param hiveServerUrl is the URL to connect to
   * @param hiveServerUser is the username to connect with
   * @param hiveServerPassword is the password to authenticate with when connecting to the URL
   * @throws SQLException if there is a problem connecting to the URL
   * @return
   */
  private HiveJdbcConnector withHiveConnectionFromUrlUserPassword(String hiveServerUrl, String hiveServerUser,
      String hiveServerPassword) throws SQLException {
    this.conn = DriverManager.getConnection(hiveServerUrl, hiveServerUser, hiveServerPassword);
    return this;
  }

  /**
   * Set the {@link HiveJdbcConnector#conn} based on {@link HiveJdbcConnector#hiveServerVersion}
   * @throws SQLException if there is a problem connection to the URL
   * @return
   */
  private HiveJdbcConnector withHiveEmbeddedConnection() throws SQLException {
    if (this.hiveServerVersion == 1) {
      this.conn = DriverManager.getConnection(HIVE_EMBEDDED_CONNECTION_STRING);
    } else {
      this.conn = DriverManager.getConnection(HIVE2_EMBEDDED_CONNECTION_STRING);
    }
    return this;
  }

  /**
   * Helper method to add the directory containing the hive-site.xml file to the classpath
   * @param hiveSiteDir is the path to to the folder containing the hive-site.xml file
   */
  private static void addHiveSiteDirToClasspath(String hiveSiteDir) {
    LOG.info("Adding " + hiveSiteDir + " to CLASSPATH");
    File f = new File(hiveSiteDir);
    try {
      URL u = f.toURI().toURL();
      URLClassLoader urlClassLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
      Class<URLClassLoader> urlClass = URLClassLoader.class;
      Method method = urlClass.getDeclaredMethod("addURL", new Class[] { URL.class });
      method.setAccessible(true);
      method.invoke(urlClassLoader, new Object[] { u });
    } catch (ReflectiveOperationException | IOException e) {
      throw new RuntimeException("Unable to add hive.site.dir to CLASSPATH", e);
    }
  }

  /**
   * Helper method that executes a series of "set ?=?" queries for the Hive connection in {@link HiveJdbcConnector#conn}.
   * @param props specifies which set methods to run. For example, if the config contains "hive.mapred.min.split.size=100"
   * then "set mapred.min.split.size=100" will be executed.
   * @throws SQLException is thrown if there is a problem executing the "set" queries
   */
  private void setHiveProperties(Properties props) throws SQLException {
    Preconditions.checkNotNull(this.conn, "The Hive connection must be set before any queries can be run");

    try (PreparedStatement preparedStatement = this.conn.prepareStatement("set ?=?")) {
      Enumeration<?> enumeration = props.propertyNames();
      while (enumeration.hasMoreElements()) {
        String propertyName = (String) enumeration.nextElement();

        if (propertyName.startsWith(HIVE_CONFIG_KEY_PREFIX)) {
          preparedStatement.setString(1, propertyName);
          preparedStatement.setString(2, props.getProperty(propertyName));
          preparedStatement.execute();
        }
      }
    }
  }

  /***
   * Executes the given SQL statements.
   *
   * @param statements SQL statements to be executed.
   * @throws SQLException if any issue in executing any statement.
   */
  public void executeStatements(String... statements) throws SQLException {
    Preconditions.checkNotNull(this.conn, "The Hive connection must be set before any queries can be run");

    for (String statement : statements) {
      if (isSimulate) {
        LOG.info("[SIMULATE MODE] STATEMENT NOT RUN: " + choppedStatementNoLineChange(statement));
      } else {
        LOG.info("RUNNING STATEMENT: " + choppedStatementNoLineChange(statement));
        try (Statement stmt = this.conn.createStatement()) {
          try {
            stmt.execute(statement);
          } catch (SQLException sqe) {
            LOG.error("Failed statement: " + choppedStatementNoLineChange(statement));
            throw sqe;
          }
        }
      }
    }
  }

  // Chopped statements with all line-changing character being removed for saving space of log.
  static String choppedStatementNoLineChange(String statement) {
    // \r\n needs to be the first element in the pipe.
    statement = statement.replaceAll("\\r\\n|\\r|\\n", " ");
    if (statement.length() <= MAX_OUTPUT_STMT_LENGTH) {
      return statement;
    }
    return statement.substring(0, MAX_OUTPUT_STMT_LENGTH) + "...... (" + (statement.length() - MAX_OUTPUT_STMT_LENGTH)
        + " characters omitted)";
  }

  public Connection getConnection() {
    return this.conn;
  }

  @Override
  public void close() throws IOException {

    if (this.conn != null) {
      try {
        this.conn.close();
      } catch (SQLException e) {
        LOG.error("Failed to close JDBC connection", e);
      }
    }
  }
}
