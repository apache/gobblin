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

package gobblin.hive.util;

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
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class for managing a Hive JDBC connection.
 *
 * @author ziliu
 */
public class HiveJdbcConnector implements Closeable {

  public static final String HIVESERVER_VERSION = "hiveserver.version";
  public static final int DEFAULT_HIVESERVER_VERSION = 2;
  public static final String HIVESERVER_CONNECTION_STRING = "hiveserver.connection.string";
  public static final String HIVESERVER_URL = "hiveserver.connection.string";
  public static final String HIVESERVER_USER = "hiveserver.connection.string";
  public static final String HIVESERVER_PASSWORD = "hiveserver.connection.string";
  public static final String HIVESITE_DIR = "hivesite.dir";

  private static final String HIVE_JDBC_DRIVER_NAME = "org.apache.hadoop.hive.jdbc.HiveDriver";
  private static final String HIVE2_JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
  private static final String HIVE_EMBEDDED_CONNECTION_STRING = "jdbc:hive://";
  private static final String HIVE2_EMBEDDED_CONNECTION_STRING = "jdbc:hive2://";
  private static final String HIVE_CONFIG_KEY_PREFIX = "hive.";
  private static final int MAX_OUTPUT_STMT_LENGTH = 1000;

  private static final Logger LOG = LoggerFactory.getLogger(HiveJdbcConnector.class);

  private Connection conn;
  private Statement stmt;

  private int hiveServerVersion = DEFAULT_HIVESERVER_VERSION;

  public HiveJdbcConnector() {
  }

  /**
   * Set the Hive server version. Before it sets the internal version to use, it checks if the version number is valid,
   * and if the driver class corresponding to the driver version is present on the classpath.
   * @param hiveServerVersion is the Hive server version to use
   * @return a HiveJdbcConnector with the specified hiveServerVersion
   */
  public HiveJdbcConnector withHiveServerVersion(int hiveServerVersion) {
    if (hiveServerVersion == 1) {
      if (!checkIfClassOnClasspath(HIVE_JDBC_DRIVER_NAME)) {
        throw new RuntimeException("Cannot find a suitable driver for HiveServer " + hiveServerVersion);
      }
    } else if (hiveServerVersion == 2) {
      if (!checkIfClassOnClasspath(HIVE2_JDBC_DRIVER_NAME)) {
        throw new RuntimeException("Cannot find a suitable driver for HiveServer " + hiveServerVersion);
      }
    } else {
      throw new RuntimeException(hiveServerVersion + " is not a valid HiveServer version.");
    }
    this.hiveServerVersion = hiveServerVersion;
    return this;
  }

  /**
   * Helper method to check if a class is on the current classpath
   * @param className the name of the class to check
   * @return true if the specified class specified by className is on the classpath, false otherwise
   */
  private boolean checkIfClassOnClasspath(String className) {
    try {
      Class.forName(className);
    } catch (ClassNotFoundException e) {
      return false;
    }
    return true;
  }

  /**
   * Set the {@link HiveJdbcConnector#conn} based on the URL given to the method
   * @param hiveServerUrl is the URL to connect to
   * @throws SQLException if there is a problem connectiong to the URL
   */
  public void setHiveConnectionFromUrl(String hiveServerUrl) throws SQLException {
    this.conn = DriverManager.getConnection(hiveServerUrl);
    this.stmt = this.conn.createStatement();
  }

  /**
   * Set the {@link HiveJdbcConnector#conn} based on the URL, user, and password given to the method
   * @param hiveServerUrl is the URL to connect to
   * @param hiveServerUser is the username to connect with
   * @param hiveServerPassword is the password to authenticate with when connecting to the URL
   * @throws SQLException if there is a problem connecting to the URL
   */
  public void setHiveConnectionFromUrlUserPassword(String hiveServerUrl, String hiveServerUser,
      String hiveServerPassword) throws SQLException {
    this.conn = DriverManager.getConnection(hiveServerUrl, hiveServerUser, hiveServerPassword);
    this.stmt = this.conn.createStatement();
  }

  /**
   * Set the {@link HiveJdbcConnector#conn} based on {@link HiveJdbcConnector#hiveServerVersion}
   * @throws SQLException if there is a problem connection to the URL
   */
  public void setHiveEmbeddedConnection() throws SQLException {
    if (this.hiveServerVersion == 1) {
      this.conn = DriverManager.getConnection(HIVE_EMBEDDED_CONNECTION_STRING);
    } else {
      this.conn = DriverManager.getConnection(HIVE2_EMBEDDED_CONNECTION_STRING);
    }
    this.stmt = this.conn.createStatement();
  }

  public void addHiveSiteDirToClasspath(String hiveSiteDir) {
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
      throw new RuntimeException("Unable to add hive.site.dir to CLASSPATH");
    }
  }

  public void setHiveProperties(Properties props) throws SQLException {
    Enumeration<?> enumeration = props.propertyNames();
    while (enumeration.hasMoreElements()) {
      String propertyName = (String) enumeration.nextElement();
      if (propertyName.startsWith(HIVE_CONFIG_KEY_PREFIX)) {
        String str = "set " + propertyName + "=" + props.getProperty(propertyName);
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
