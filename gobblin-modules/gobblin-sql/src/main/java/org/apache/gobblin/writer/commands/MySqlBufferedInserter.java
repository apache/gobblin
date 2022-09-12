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
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.converter.jdbc.JdbcEntryData;
import org.apache.gobblin.converter.jdbc.JdbcEntryDatum;
import lombok.ToString;


/**
 * The implementation of JdbcBufferedInserter for MySQL.
 * This purpose of buffered insert is mainly for performance reason and the implementation is based on the
 * reference manual https://dev.mysql.com/doc/refman/8.0/en/
 *
 * This class supports two types of insertions for MySQL 1) standard insertion - only supports records with unique
 * primary keys and fails on attempted insertion of a duplicate record 2) replace insertion - inserts new records as
 * normal but allows for value overwrites for duplicate inserts (by primary key)
 *
 * Note that replacement occurs at 'record-level', so if there are duplicates in the same input then they will replace
 * each other in a non-deterministic order.
 */
@ToString
public class MySqlBufferedInserter extends BaseJdbcBufferedInserter {

  private static final Logger LOG = LoggerFactory.getLogger(MySqlBufferedInserter.class);

  protected static final String REPLACE_STATEMENT_PREFIX_FORMAT = "REPLACE INTO %s.%s (%s) VALUES ";

  private final int maxParamSize;

  private final boolean overwriteRecords;

  public MySqlBufferedInserter(State state, Connection conn, boolean overwriteRecords) {
    super(state, conn);
    this.maxParamSize = state.getPropAsInt(WRITER_JDBC_MAX_PARAM_SIZE, DEFAULT_WRITER_JDBC_MAX_PARAM_SIZE);
    this.overwriteRecords = overwriteRecords;
  }

  @Override
  protected boolean insertBatch(PreparedStatement pstmt) throws SQLException {
    int i = 0;
    pstmt.clearParameters();
    for (JdbcEntryData pendingEntry : MySqlBufferedInserter.this.pendingInserts) {
      for (JdbcEntryDatum datum : pendingEntry) {
        pstmt.setObject(++i, datum.getVal());
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Executing SQL " + pstmt);
    }
    return pstmt.execute();
  }

  @Override
  protected String createPrepareStatementStr(int batchSize) {
    final String VALUE_FORMAT = "(%s)";
    StringBuilder sb = new StringBuilder(this.insertStmtPrefix);
    String values =
        String.format(VALUE_FORMAT, JOINER_ON_COMMA.useForNull("?").join(new String[this.columnNames.size()]));
    sb.append(values);
    for (int i = 1; i < batchSize; i++) {
      sb.append(',').append(values);
    }
    return sb.append(';').toString();
  }

  @Override
  protected void initializeBatch(String databaseName, String table)
      throws SQLException {
    int actualBatchSize = Math.min(this.batchSize, this.maxParamSize / this.columnNames.size());
    if (this.batchSize != actualBatchSize) {
      LOG.info("Changing batch size from " + this.batchSize + " to " + actualBatchSize
          + " due to # of params limitation " + this.maxParamSize + " , # of columns: " + this.columnNames.size());
    }
    this.batchSize = actualBatchSize;
    super.initializeBatch(databaseName, table);
  }

  @Override
  /**
   * Use separate insertion statement if data overwrites are allowed
   */
  protected String createInsertStatementStr(String databaseName, String table) {
    return String.format(this.overwriteRecords ? REPLACE_STATEMENT_PREFIX_FORMAT : INSERT_STATEMENT_PREFIX_FORMAT, databaseName, table, JOINER_ON_COMMA.join(this.columnNames));
  }
}