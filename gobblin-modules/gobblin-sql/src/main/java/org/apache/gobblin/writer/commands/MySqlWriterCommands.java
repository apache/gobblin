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

import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.converter.jdbc.JdbcEntryData;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;


/**
 * The implementation of JdbcWriterCommands for MySQL.
 */
public class MySqlWriterCommands implements JdbcWriterCommands, JdbcUpdater {
  private static final Logger LOG = LoggerFactory.getLogger(MySqlWriterCommands.class);
  private static final JdbcExpressionGenerator JDBC_EXPRESSION_GENERATOR = new JdbcExpressionGenerator("`", "'");

  private static final String CREATE_TABLE_SQL_FORMAT = "CREATE TABLE %s.%s LIKE %s.%s";
  private static final String SELECT_SQL_FORMAT = "SELECT COUNT(*) FROM %s.%s";
  private static final String TRUNCATE_TABLE_FORMAT = "TRUNCATE TABLE %s.%s";
  private static final String DROP_TABLE_SQL_FORMAT = "DROP TABLE %s.%s";
  private static final String INFORMATION_SCHEMA_SELECT_SQL_PSTMT =
      "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = ? AND table_name = ?";
  private static final String RETRIEVE_KEYS_PSTMT = "SHOW KEYS FROM `%s`.`%s` WHERE Key_name = 'PRIMARY'";
  private static final String COPY_INSERT_STATEMENT_FORMAT = "INSERT INTO %s.%s SELECT * FROM %s.%s";
  private static final String DELETE_RECORD_STATEMENT_FORMAT = "DELETE FROM `%s`.`%s` WHERE %s";
  private static final String UPDATE_RECORD_STATEMENT_FORMAT = "UPDATE `%s`.`%s` SET %s WHERE %s";
  private static final String DELETE_STATEMENT_FORMAT = "DELETE FROM %s.%s";

  private final JdbcBufferedInserter jdbcBufferedWriter;
  private final Connection conn;

  public MySqlWriterCommands(State state, Connection conn) {
    this.conn = conn;
    this.jdbcBufferedWriter = new MySqlBufferedInserter(state, conn);
  }

  @Override
  public void dispatch(String databaseName, String table, JdbcEntryData jdbcEntryData) throws SQLException {
    switch (jdbcEntryData.getOperation()) {
      case INSERT:
        insert(databaseName, table, jdbcEntryData);
        break;
      case DELETE:
        delete(databaseName, table, jdbcEntryData);
        break;
      case UPDATE:
        update(databaseName, table, jdbcEntryData);
        break;
      case UPSERT:
        upsert(databaseName, table, jdbcEntryData);
        break;
    }
  }

  @Override
  public void setConnectionParameters(Properties properties, Connection conn) throws SQLException {
    // MySQL writer always uses one single transaction
    this.conn.setAutoCommit(false);
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
  public void update(String databaseName, String table, JdbcEntryData jdbcEntryData) throws SQLException {
    String updateExpression = String.format(UPDATE_RECORD_STATEMENT_FORMAT, databaseName, table,
        JDBC_EXPRESSION_GENERATOR.generateNonPrimaryKeyUpdateStatement(jdbcEntryData),
        JDBC_EXPRESSION_GENERATOR.generatePrimaryKeyFilter(jdbcEntryData));
    execute(updateExpression, false);
  }

  @Override
  public void delete(String databaseName, String table, JdbcEntryData jdbcEntryData) throws SQLException {
    String deleteExpression = String.format(DELETE_RECORD_STATEMENT_FORMAT, databaseName, table,
        JDBC_EXPRESSION_GENERATOR.generatePrimaryKeyFilter(jdbcEntryData));
    execute(deleteExpression, false);
  }

  @Override
  public void upsert(String databaseName, String table, JdbcEntryData jdbcEntryData) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createTableStructure(String databaseName, String fromStructure, String targetTableName) throws SQLException {
    String sql = String.format(CREATE_TABLE_SQL_FORMAT, databaseName, targetTableName,
                                                        databaseName, fromStructure);
    execute(sql, true);
  }

  @Override
  public boolean isEmpty(String database, String table) throws SQLException {
    String sql = String.format(SELECT_SQL_FORMAT, database, table);
    try (PreparedStatement pstmt = this.conn.prepareStatement(sql); ResultSet resultSet = pstmt.executeQuery();) {
      if (!resultSet.first()) {
        throw new RuntimeException("Should have received at least one row from SQL " + pstmt);
      }
      return 0 == resultSet.getInt(1);
    }
  }

  @Override
  public void truncate(String database, String table) throws SQLException {
    String sql = String.format(TRUNCATE_TABLE_FORMAT, database, table);
    execute(sql, true);
  }

  @Override
  public void deleteAll(String database, String table) throws SQLException {
    String deleteSql = String.format(DELETE_STATEMENT_FORMAT, database, table);
    execute(deleteSql, true);
  }

  @Override
  public void drop(String database, String table) throws SQLException {
    LOG.info("Dropping table " + table);
    String sql = String.format(DROP_TABLE_SQL_FORMAT, database, table);
    execute(sql, true);
  }

  /**
   * https://dev.mysql.com/doc/connector-j/en/connector-j-reference-type-conversions.html
   * {@inheritDoc}
   * @see org.apache.gobblin.writer.commands.JdbcWriterCommands#retrieveColumnTypes(String, String)
   */
  @Override
  public Map<String, JDBCType> retrieveColumnTypes(String database, String table) throws SQLException {
    DatabaseMetaData metaData = this.conn.getMetaData();

    ImmutableMap.Builder<String, JDBCType> columnsBuilder = ImmutableMap.builder();

    try (ResultSet rsTmp = metaData.getColumns("privacy", null, "users", null)) {
      while (rsTmp.next()) {
        columnsBuilder.put(rsTmp.getString("COLUMN_NAME"), JDBCType.values()[rsTmp.getInt("DATA_TYPE")]);
      }
    }

    return columnsBuilder.build();
  }

  @Override
  public List<String> retrievePrimaryKeys(String database, String table) throws SQLException {
    DatabaseMetaData metaData = this.conn.getMetaData();

    List<String> primaryKeys = new ArrayList<>();

    try (ResultSet rsTmp = metaData.getPrimaryKeys("privacy", null, "users")) {
      while (rsTmp.next()) {
        int idx = rsTmp.getInt("KEY_SEQ") - 1;
        String col = rsTmp.getString("COLUMN_NAME");
        if (idx < primaryKeys.size()) {
          primaryKeys.set(idx, col);
        } else {
          for (int i = primaryKeys.size(); i < idx; i ++) {
            primaryKeys.add(null);
          }
          primaryKeys.add(col);
        }
      }
    }

    return primaryKeys;
  }

  @Override
  public void copyTable(String databaseName, String from, String to) throws SQLException {
    String sql = String.format(COPY_INSERT_STATEMENT_FORMAT, databaseName, to, databaseName, from);
    execute(sql, true);
  }

  private void execute(String sql, boolean logAtInfoLevel) throws SQLException {
    if (logAtInfoLevel) {
      LOG.info("Executing SQL " + sql);
    } else {
      LOG.debug("Executing SQL {}", sql);
    }
    try (PreparedStatement pstmt = this.conn.prepareStatement(sql)) {
      pstmt.execute();
    }
  }

  @Override
  public String toString() {
    return String.format("MySqlWriterCommands [bufferedWriter=%s]", this.jdbcBufferedWriter);
  }
}
