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

package org.apache.gobblin.writer.commands;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.converter.jdbc.JdbcEntryData;
import org.apache.gobblin.converter.jdbc.JdbcType;


/**
 * The implementation of JdbcWriterCommands for MySQL.
 */
public class MySqlWriterCommands implements JdbcWriterCommands {
  private static final Logger LOG = LoggerFactory.getLogger(MySqlWriterCommands.class);

  private static final String CREATE_TABLE_SQL_FORMAT = "CREATE TABLE %s.%s LIKE %s.%s";
  private static final String SELECT_SQL_FORMAT = "SELECT COUNT(*) FROM %s.%s";
  private static final String TRUNCATE_TABLE_FORMAT = "TRUNCATE TABLE %s.%s";
  private static final String DROP_TABLE_SQL_FORMAT = "DROP TABLE %s.%s";
  private static final String INFORMATION_SCHEMA_SELECT_SQL_PSTMT =
      "SELECT column_name, column_type FROM information_schema.columns WHERE table_schema = ? AND table_name = ?";
  private static final String COPY_INSERT_STATEMENT_FORMAT = "INSERT INTO %s.%s SELECT * FROM %s.%s";
  private static final String COPY_REPLACE_STATEMENT_FORMAT = "REPLACE INTO %s.%s SELECT * FROM %s.%s";
  private static final String DELETE_STATEMENT_FORMAT = "DELETE FROM %s.%s";
  private static final String SHOW_GRANTS_SQL = "SHOW GRANTS FOR CURRENT_USER()";

  private final JdbcBufferedInserter jdbcBufferedWriter;
  private final Connection conn;
  private final boolean overwriteRecords;

  public MySqlWriterCommands(State state, Connection conn, boolean overwriteRecords) {
    this.conn = conn;
    this.jdbcBufferedWriter = new MySqlBufferedInserter(state, conn, overwriteRecords);
    this.overwriteRecords = overwriteRecords;
  }

  @Override
  public void setConnectionParameters(Properties properties, Connection conn) throws SQLException {
  }

  @Override
  public void insert(String databaseName, String table, JdbcEntryData jdbcEntryData) throws SQLException {
    this.jdbcBufferedWriter.insert(databaseName, table, jdbcEntryData);
  }

  @Override
  public void flush() throws SQLException {
    this.jdbcBufferedWriter.flush();
  }

  @Override
  public void createTableStructure(String databaseName, String fromStructure, String targetTableName) throws SQLException {
    String sql = String.format(CREATE_TABLE_SQL_FORMAT, databaseName, targetTableName,
                                                        databaseName, fromStructure);
    execute(sql);
  }

  @Override
  public boolean isEmpty(String database, String table) throws SQLException {
    String sql = String.format(SELECT_SQL_FORMAT, database, table);
    try (PreparedStatement pstmt = this.conn.prepareStatement(sql); ResultSet resultSet = pstmt.executeQuery();) {
      if (!resultSet.next()) {
        throw new RuntimeException("Should have received at least one row from SQL " + pstmt);
      }
      return 0 == resultSet.getInt(1);
    }
  }

  @Override
  public void truncate(String database, String table) throws SQLException {
    String sql = String.format(TRUNCATE_TABLE_FORMAT, database, table);
    execute(sql);
  }

  @Override
  public void deleteAll(String database, String table) throws SQLException {
    String deleteSql = String.format(DELETE_STATEMENT_FORMAT, database, table);
    execute(deleteSql);
  }

  @Override
  public void drop(String database, String table) throws SQLException {
    LOG.info("Dropping table " + table);
    String sql = String.format(DROP_TABLE_SQL_FORMAT, database, table);
    execute(sql);
  }

  /**
   * Best-effort DROP-privilege check via {@code SHOW GRANTS FOR CURRENT_USER()}. Fails open: any
   * error or inconclusive parse returns {@code true} so a check failure never blocks a run that is
   * actually permitted. Only returns {@code false} when no grant covering DROP on {@code database}
   * is found.
   *
   * <p>Caveats: {@code SHOW GRANTS FOR CURRENT_USER()} may not expand privileges that come from
   * non-default (inactive) roles, and grant-string parsing is necessarily heuristic. This is meant
   * to catch the common case (e.g. a CREATE-but-not-DROP role) and avoid leaving orphan staging
   * tables, not to be a complete authorization model.
   */
  @Override
  public boolean hasDropPrivilege(String database) throws SQLException {
    try (PreparedStatement pstmt = this.conn.prepareStatement(SHOW_GRANTS_SQL);
        ResultSet rs = pstmt.executeQuery()) {
      while (rs.next()) {
        if (grantCoversDrop(rs.getString(1), database)) {
          return true;
        }
      }
      LOG.warn("No grant covering DROP on database '" + database + "' found for current user.");
      return false;
    } catch (SQLException e) {
      // Fail open: never block a legitimately-permitted run because the check itself failed.
      LOG.warn("Could not verify DROP privilege on database '" + database + "'; proceeding.", e);
      return true;
    }
  }

  /**
   * Returns true if a single {@code SHOW GRANTS} line grants DROP (or ALL PRIVILEGES) on a scope
   * that covers {@code database} (either {@code *.*} or {@code `database`.*}).
   */
  @VisibleForTesting
  static boolean grantCoversDrop(String grantLine, String database) {
    if (grantLine == null) {
      return false;
    }
    String g = grantLine.toUpperCase();
    int onIdx = g.indexOf(" ON ");
    int toIdx = g.indexOf(" TO ");
    if (onIdx < 0 || toIdx < 0 || toIdx < onIdx) {
      return false;
    }
    String privileges = g.substring(0, onIdx);
    boolean grantsDrop = privileges.contains("ALL PRIVILEGES")
        || privileges.matches(".*\\bDROP\\b.*");
    if (!grantsDrop) {
      return false;
    }
    // Scope looks like "`DB`.*" or "*.*" or "`DB`.`TABLE`"; strip backticks and compare the schema.
    String scope = g.substring(onIdx + 4, toIdx).trim().replace("`", "");
    String schema = scope.substring(0, scope.indexOf('.') < 0 ? scope.length() : scope.indexOf('.'));
    return schema.equals("*") || schema.equals(database.toUpperCase());
  }

  /**
   * https://dev.mysql.com/doc/connector-j/en/connector-j-reference-type-conversions.html
   * {@inheritDoc}
   * @see org.apache.gobblin.writer.commands.JdbcWriterCommands#retrieveDateColumns(java.sql.Connection, java.lang.String)
   */
  @Override
  public Map<String, JdbcType> retrieveDateColumns(String database, String table) throws SQLException {
    Map<String, JdbcType> targetDataTypes = ImmutableMap.<String, JdbcType> builder()
                                                        .put("DATE", JdbcType.DATE)
                                                        .put("DATETIME", JdbcType.TIMESTAMP)
                                                        .put("TIME", JdbcType.TIME)
                                                        .put("TIMESTAMP", JdbcType.TIMESTAMP)
                                                        .build();

    ImmutableMap.Builder<String, JdbcType> dateColumnsBuilder = ImmutableMap.builder();
    try (PreparedStatement pstmt = this.conn.prepareStatement(INFORMATION_SCHEMA_SELECT_SQL_PSTMT)) {
      pstmt.setString(1, database);
      pstmt.setString(2, table);
      LOG.info("Retrieving column type information from SQL: " + pstmt);
      try (ResultSet rs = pstmt.executeQuery()) {
        if (!rs.next()) {
          throw new IllegalArgumentException("No result from information_schema.columns");
        }
        do {
          String type = rs.getString("column_type").toUpperCase();
          JdbcType convertedType = targetDataTypes.get(type);
          if (convertedType != null) {
            dateColumnsBuilder.put(rs.getString("column_name"), convertedType);
          }
        } while (rs.next());
      }
    }
    return dateColumnsBuilder.build();
  }

  @Override
  public void copyTable(String databaseName, String from, String to) throws SQLException {
    // Chooses between INSERT and REPLACE logic based on the job configurations
    String sql = String
        .format(this.overwriteRecords ? COPY_REPLACE_STATEMENT_FORMAT : COPY_INSERT_STATEMENT_FORMAT, databaseName,
            to, databaseName, from);
    execute(sql);
  }

  private void execute(String sql) throws SQLException {
    LOG.info("Executing SQL " + sql);
    try (PreparedStatement pstmt = this.conn.prepareStatement(sql)) {
      pstmt.execute();
    }
  }

  @Override
  public String toString() {
    return String.format("MySqlWriterCommands [bufferedWriter=%s]", this.jdbcBufferedWriter);
  }
}
