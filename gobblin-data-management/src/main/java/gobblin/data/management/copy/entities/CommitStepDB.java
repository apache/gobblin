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

package gobblin.data.management.copy.entities;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.NoSuchElementException;
import java.util.Properties;

import org.apache.commons.io.IOUtils;

import com.google.common.base.Charsets;

import gobblin.commit.CommitStep;
import gobblin.data.management.copy.CopyEntity;

import lombok.extern.slf4j.Slf4j;


/**
 * A DB that stores {@link CommitStep}s. Implementation is transparent to the user, but currently uses Derby.
 */
@Slf4j
public class CommitStepDB implements Closeable {

  public static final String COMMIT_STEP_DB_NAME = "gobblinCommitStepDB";
  private static final String protocol = "jdbc:derby:";
  private static final String ALREADY_EXISTS_STATE = "X0Y32";

  private static final String INSERT_STEP_STATEMENT = "INSERT INTO steps VALUES (?, ?)";
  private static final String GET_STEP_STATEMENT = "SELECT step FROM steps WHERE step_key = ?";

  static {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          dropTable();
          try {
            DriverManager.getConnection(protocol + COMMIT_STEP_DB_NAME + ";shutdown=true", new Properties());
          } catch (SQLException se) {
            if (((se.getErrorCode() == 45000)
                && ("08006".equals(se.getSQLState())))) {
              // we got the expected exception
              System.out.println("Derby shut down normally");
            } else {
              throw se;
            }
          }
          DriverManager.getConnection(protocol + COMMIT_STEP_DB_NAME + ";drop=true", new Properties());
        } catch (SQLException sqle) {
          System.out.println("Error dropping commit steps DB");
          printSQLException(sqle);
        }
      }
    });
  }

  private final Connection conn;

  public CommitStepDB() throws IOException {
    try {
      String driver = "org.apache.derby.jdbc.EmbeddedDriver";
      Class.forName(driver);
    } catch (ReflectiveOperationException roe) {
      throw new RuntimeException("Derby is not in the classpath", roe);
    }

    try {
      this.conn = DriverManager.getConnection(protocol + COMMIT_STEP_DB_NAME + ";create=true", new Properties());
      ensureTableExists();
    } catch (SQLException se) {
      throw new IOException(se);
    }
  }

  private void ensureTableExists() throws SQLException {
    try (Statement statement = this.conn.createStatement()) {
      statement.execute("CREATE TABLE steps(step_key VARCHAR(200) NOT NULL, step CLOB(500K), PRIMARY KEY (step_key))");
    } catch (SQLException sqle) {
      if (!sqle.getSQLState().equals(ALREADY_EXISTS_STATE)) {
        throw sqle;
      }
    }
  }

  private static void dropTable() throws SQLException {
    try (Connection conn = DriverManager.getConnection(protocol + COMMIT_STEP_DB_NAME + ";create=true", new Properties());
        Statement statement = conn.createStatement()) {
      statement.execute("DROP TABLE steps");
      conn.commit();
    } catch (SQLException sqle) {
      if (!sqle.getSQLState().equals(ALREADY_EXISTS_STATE)) {
        throw sqle;
      }
    }
  }

  /**
   * Put a {@link CommitStep} into the DB.
   * @param key key to indentify the {@link CommitStep}
   * @param step {@link CommitStep} to store.
   * @throws IOException for SQL errors.
   * @throws IllegalStateException if a {@link CommitStep} with that key already exists.
   */
  public void put(String key, CommitStep step) throws IOException {
    String serializedStep = CopyEntity.GSON.toJson(step);
    try (PreparedStatement statement = this.conn.prepareStatement(INSERT_STEP_STATEMENT)) {
      statement.setString(1, key);
      statement.setAsciiStream(2, new ByteArrayInputStream(serializedStep.getBytes(Charsets.UTF_8)));
      statement.execute();
      this.conn.commit();
    } catch (SQLIntegrityConstraintViolationException se) {
      String existingStep = getRaw(key);
      if (!serializedStep.equals(existingStep)) {
        throw new IllegalStateException(
            String.format("A step with key %s is already present in the commit step DB.", key));
      }
    } catch (SQLException se) {
      throw new IOException(se);
    }
  }

  /**
   * Get the {@link CommitStep} stored under this key.
   * @param key
   * @return {@link CommitStep} stored under input key.
   * @throws NoSuchElementException if no {@link CommitStep} with that key exists.
   * @throws IOException for SQL errors.
   */
  public CommitStep get(String key) throws IOException {
    return CopyEntity.GSON.fromJson(getRaw(key), CommitStep.class);
  }

  private String getRaw(String key) throws IOException {
    try (PreparedStatement statement = this.conn.prepareStatement(GET_STEP_STATEMENT)) {
      statement.setString(1, key);
      ResultSet rs = statement.executeQuery();
      this.conn.commit();

      if (!rs.next()) {
        throw new NoSuchElementException("No step with key " + key);
      }

      InputStream is = rs.getAsciiStream(1);
      return IOUtils.toString(is, Charsets.UTF_8);
    } catch (SQLException se) {
      throw new IOException(se);
    }
  }

  /**
   * Closes the connection to the DB.
   * @throws IOException
   */
  @Override
  public void close()
      throws IOException {
    try {
      this.conn.close();
    } catch (SQLException sqle) {
      log.error("Failed to close SQL connection.");
      printSQLException(sqle);
    }
  }

  /**
   * Prints details of an SQLException chain to <code>System.err</code>.
   * Details included are SQL State, Error code, Exception message.
   *
   * @param e the SQLException from which to print details.
   */
  public static void printSQLException(SQLException e) {
    // Unwraps the entire exception chain to unveil the real cause of the
    // Exception.
    while (e != null) {
      System.err.println("\n----- SQLException -----");
      System.err.println("  SQL State:  " + e.getSQLState());
      System.err.println("  Error Code: " + e.getErrorCode());
      System.err.println("  Message:    " + e.getMessage());
      // for stack traces, refer to derby.log or uncomment this:
      //e.printStackTrace(System.err);
      e = e.getNextException();
    }
  }

}
