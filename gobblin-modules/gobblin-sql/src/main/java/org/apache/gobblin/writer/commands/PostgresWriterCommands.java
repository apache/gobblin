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

import com.google.common.collect.ImmutableMap;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.converter.jdbc.JdbcEntryData;
import org.apache.gobblin.converter.jdbc.JdbcType;


/**
 * The implementation of JdbcWriterCommands for Postgres.
 */
@Slf4j
public class PostgresWriterCommands implements JdbcWriterCommands {

  private static final String CREATE_TABLE_SQL_FORMAT = "CREATE TABLE %s.%s (LIKE %s.%s)";
  private static final String SELECT_SQL_FORMAT = "SELECT COUNT(*) FROM %s.%s";
  private static final String TRUNCATE_TABLE_FORMAT = "TRUNCATE TABLE %s.%s";
  private static final String DROP_TABLE_SQL_FORMAT = "DROP TABLE %s.%s";
  private static final String INFORMATION_SCHEMA_SELECT_SQL_PSTMT =
      "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = ? AND table_name = ?";
  private static final String COPY_INSERT_STATEMENT_FORMAT = "INSERT INTO %s.%s SELECT * FROM %s.%s";
  private static final String DELETE_STATEMENT_FORMAT = "DELETE FROM %s.%s";

  private final JdbcBufferedInserter jdbcBufferedWriter;
  private final Connection conn;

  public PostgresWriterCommands(State state, Connection conn, boolean overwriteRecords) throws UnsupportedOperationException {
    if (overwriteRecords) {
      throw new IllegalArgumentException("Replace existing records is not supported in PostgresWriterCommands");
    }

    this.conn = conn;
    this.jdbcBufferedWriter = new PostgresBufferedInserter(state, conn);
  }

  @Override
  public void setConnectionParameters(Properties properties, Connection conn)
      throws SQLException {
  }

  @Override
  public void insert(String databaseName, String table, JdbcEntryData jdbcEntryData)
      throws SQLException {
    this.jdbcBufferedWriter.insert(databaseName, table, jdbcEntryData);
  }

  @Override
  public void flush()
      throws SQLException {
    this.jdbcBufferedWriter.flush();
  }

  @Override
  public void createTableStructure(String databaseName, String fromStructure, String targetTableName)
      throws SQLException {
    String sql = String.format(CREATE_TABLE_SQL_FORMAT, databaseName, targetTableName, databaseName, fromStructure);
    execute(sql);
  }

  @Override
  public boolean isEmpty(String database, String table)
      throws SQLException {
    String sql = String.format(SELECT_SQL_FORMAT, database, table);
    try (PreparedStatement pstmt = this.conn.prepareStatement(sql); ResultSet resultSet = pstmt.executeQuery();) {
      if (!resultSet.first()) {
        throw new RuntimeException("Should have received at least one row from SQL " + pstmt);
      }
      return 0 == resultSet.getInt(1);
    }
  }

  @Override
  public void truncate(String database, String table)
      throws SQLException {
    String sql = String.format(TRUNCATE_TABLE_FORMAT, database, table);
    execute(sql);
  }

  @Override
  public void deleteAll(String database, String table)
      throws SQLException {
    String deleteSql = String.format(DELETE_STATEMENT_FORMAT, database, table);
    execute(deleteSql);
  }

  @Override
  public void drop(String database, String table)
      throws SQLException {
    log.info("Dropping table " + table);
    String sql = String.format(DROP_TABLE_SQL_FORMAT, database, table);
    execute(sql);
  }

  /**
   * https://documentation.progress.com/output/DataDirect/DataDirectCloud/index.html#page/queries/postgresql-data-types.html
   * {@inheritDoc}
   * @see org.apache.gobblin.writer.commands.JdbcWriterCommands#retrieveDateColumns(java.sql.Connection, java.lang.String)
   */
  @Override
  public Map<String, JdbcType> retrieveDateColumns(String database, String table)
      throws SQLException {
    Map<String, JdbcType> targetDataTypes =
        ImmutableMap.<String, JdbcType>builder().put("DATE", JdbcType.DATE).put("TIME WITH TIME ZONE", JdbcType.TIME)
            .put("TIME WITHOUT TIME ZONE", JdbcType.TIME).put("TIMESTAMP WITH TIME ZONE", JdbcType.TIMESTAMP)
            .put("TIMESTAMP WITHOUT TIME ZONE", JdbcType.TIMESTAMP).build();

    ImmutableMap.Builder<String, JdbcType> dateColumnsBuilder = ImmutableMap.builder();

    try (PreparedStatement pstmt = this.conn
        .prepareStatement(INFORMATION_SCHEMA_SELECT_SQL_PSTMT, ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY)) {
      pstmt.setString(1, database);
      pstmt.setString(2, table);
      log.info("Retrieving column type information from SQL: " + pstmt);
      try (ResultSet rs = pstmt.executeQuery()) {
        if (!rs.first()) {
          throw new IllegalArgumentException("No result from information_schema.columns");
        }
        do {
          String type = rs.getString("data_type").toUpperCase();
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
  public void copyTable(String databaseName, String from, String to)
      throws SQLException {
    String sql = String.format(COPY_INSERT_STATEMENT_FORMAT, databaseName, to, databaseName, from);
    execute(sql);
  }

  private void execute(String sql)
      throws SQLException {
    log.info("Executing SQL " + sql);
    try (PreparedStatement pstmt = this.conn.prepareStatement(sql)) {
      pstmt.execute();
    }
  }

  @Override
  public String toString() {
    return String.format("PostgresWriterCommands [bufferedWriter=%s]", this.jdbcBufferedWriter);
  }
}
