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

package gobblin.writer.commands;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.converter.jdbc.JdbcType;
import gobblin.source.extractor.JobCommitPolicy;
import gobblin.converter.jdbc.JdbcEntryData;

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
 * The implementation of JdbcWriterCommands for Teradata.
 * It is assumed that the final output table is of type MULTISET without primary index.
 * If primary index need to be defined, it is advised to adapt the staging table creation
 * ({@link #CREATE_TABLE_SQL_FORMAT}) to avoid unnecessary redistribution of data among the AMPs during
 * the insertion to the final table.
 *
 * @author Lorand Bendig
 *
 */
public class TeradataWriterCommands implements JdbcWriterCommands {
  private static final Logger LOG = LoggerFactory.getLogger(TeradataWriterCommands.class);

  private static final String CREATE_TABLE_SQL_FORMAT =
      "CREATE MULTISET TABLE %s.%s AS (SELECT * FROM %s.%s) WITH NO DATA NO PRIMARY INDEX";
  private static final String SELECT_SQL_FORMAT = "SELECT COUNT(*) FROM %s.%s";
  private static final String TRUNCATE_TABLE_FORMAT = "DELETE FROM %s.%s ALL";
  private static final String DROP_TABLE_SQL_FORMAT = "DROP TABLE %s.%s";
  private static final String DBC_COLUMNS_SELECT_SQL_PSTMT =
      "SELECT columnName, columnType FROM dbc.columns WHERE databasename = ? AND tablename = ?";
  private static final String COPY_INSERT_STATEMENT_FORMAT = "INSERT INTO %s.%s SELECT * FROM %s.%s";
  private static final String DELETE_STATEMENT_FORMAT = "DELETE FROM %s.%s";

  private final JdbcBufferedInserter jdbcBufferedWriter;
  private final Connection conn;

  public TeradataWriterCommands(State state, Connection conn) {
    this.conn = conn;
    this.jdbcBufferedWriter = new TeradataBufferedInserter(state, conn);
  }

  @Override
  public void setConnectionParameters(Properties properties, Connection conn) throws SQLException {
    // If staging tables are skipped i.e task level and partial commits are allowed to the target table,
    // then transaction handling will be managed by the JDBC driver to avoid deadlocks in the database.
    boolean jobCommitPolicyIsFull =
        JobCommitPolicy.COMMIT_ON_FULL_SUCCESS.equals(JobCommitPolicy.getCommitPolicy(properties));
    boolean publishDataAtJobLevel =
        Boolean.parseBoolean(properties.getProperty(ConfigurationKeys.PUBLISH_DATA_AT_JOB_LEVEL,
            String.valueOf(ConfigurationKeys.DEFAULT_PUBLISH_DATA_AT_JOB_LEVEL)));
    if (jobCommitPolicyIsFull || publishDataAtJobLevel) {
      this.conn.setAutoCommit(false);
    }
    else {
      LOG.info("Writing without staging tables, transactions are handled by the driver");
    }
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
  public void createTableStructure(String databaseName, String fromStructure, String targetTableName)
      throws SQLException {
    String sql = String.format(CREATE_TABLE_SQL_FORMAT, databaseName, targetTableName, databaseName, fromStructure);
    execute(sql);
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
   * {@inheritDoc}
   * @see gobblin.writer.commands.JdbcWriterCommands#retrieveDateColumns(java.sql.Connection, java.lang.String)
   */
  @Override
  public Map<String, JdbcType> retrieveDateColumns(String database, String table) throws SQLException {
    Map<String, JdbcType> targetDataTypes = ImmutableMap.<String, JdbcType> builder()
                                                        .put("AT", JdbcType.TIME)
                                                        .put("DA", JdbcType.DATE)
                                                        .put("TS", JdbcType.TIMESTAMP)
                                                        .build();

    ImmutableMap.Builder<String, JdbcType> dateColumnsBuilder = ImmutableMap.builder();
    try (PreparedStatement pstmt = this.conn.prepareStatement(DBC_COLUMNS_SELECT_SQL_PSTMT,
        ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
      pstmt.setString(1, database);
      pstmt.setString(2, table);
      LOG.info("Retrieving column type information from SQL: " + pstmt);
      try (ResultSet rs = pstmt.executeQuery()) {
        if (!rs.first()) {
          throw new IllegalArgumentException("No result from information_schema.columns");
        }
        do {
          String type = rs.getString("columnType").toUpperCase();
          JdbcType convertedType = targetDataTypes.get(type);
          if (convertedType != null) {
            dateColumnsBuilder.put(rs.getString("columnName"), convertedType);
          }
        } while (rs.next());
      }
    }
    return dateColumnsBuilder.build();
  }

  @Override
  public void copyTable(String databaseName, String from, String to) throws SQLException {
    String sql = String.format(COPY_INSERT_STATEMENT_FORMAT, databaseName, to, databaseName, from);
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
    return String.format("TeradataWriterCommands [bufferedWriter=%s]", this.jdbcBufferedWriter);
  }
}
