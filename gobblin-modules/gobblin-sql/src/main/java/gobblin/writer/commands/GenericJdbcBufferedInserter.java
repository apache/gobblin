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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.State;
import gobblin.converter.jdbc.JdbcEntryData;
import gobblin.converter.jdbc.JdbcEntryDatum;


public class GenericJdbcBufferedInserter extends BaseJdbcBufferedInserter {

  private static final Logger LOG = LoggerFactory.getLogger(GenericJdbcBufferedInserter.class);

  private static final String INSERT_STATEMENT_PREFIX_FORMAT =
      BaseJdbcBufferedInserter.INSERT_STATEMENT_PREFIX_FORMAT + " (%s)";

  private final int maxParamSize;
  private int currBatchSize;

  public GenericJdbcBufferedInserter(State state, Connection conn) {
    super(state, conn);
    this.maxParamSize = state.getPropAsInt(WRITER_JDBC_MAX_PARAM_SIZE, DEFAULT_WRITER_JDBC_MAX_PARAM_SIZE);
  }

  @Override
  protected boolean insertBatch(PreparedStatement pstmt) throws SQLException {
    GenericJdbcBufferedInserter.this.insertPstmtForFixedBatch.executeBatch();
    return true;
  }

  @Override
  public void insert(String databaseName, String table, JdbcEntryData jdbcEntryData) throws SQLException {
    if (this.insertPstmtForFixedBatch == null) {
      for (JdbcEntryDatum datum : jdbcEntryData) {
        this.columnNames.add(datum.getColumnName());
      }
      initializeBatch(databaseName, table);
    }
    int i = 0;
    for (JdbcEntryDatum datum : jdbcEntryData) {
      this.insertPstmtForFixedBatch.setObject(++i, datum.getVal());
    }
    this.insertPstmtForFixedBatch.addBatch();
    this.currBatchSize++;

    if (this.currBatchSize >= this.batchSize) {
      executeBatchInsert(this.insertPstmtForFixedBatch);
    }
  }

  @Override
  protected void initializeBatch(String databaseName, String table) throws SQLException {
    int actualBatchSize = Math.min(this.batchSize, this.maxParamSize / this.columnNames.size());
    if (this.batchSize != actualBatchSize) {
      LOG.info("Changing batch size from " + this.batchSize + " to " + actualBatchSize
          + " due to # of params limitation " + this.maxParamSize + " , # of columns: " + this.columnNames.size());
    }
    this.batchSize = actualBatchSize;
    super.initializeBatch(databaseName, table);
  }

  @Override
  protected String createInsertStatementStr(String databaseName, String table) {
    return String.format(INSERT_STATEMENT_PREFIX_FORMAT, databaseName, table,
        JOINER_ON_COMMA.join(columnNames), JOINER_ON_COMMA.useForNull("?").join(new String[columnNames.size()]));
  }

  @Override
  protected void resetBatch() {
    try {
      this.insertPstmtForFixedBatch.clearBatch();
      this.insertPstmtForFixedBatch.clearParameters();
      this.currBatchSize = 0;
    }
    catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void flush() throws SQLException {
    if (this.currBatchSize > 0) {
      insertBatch(this.insertPstmtForFixedBatch);
    }
  }

  @Override
  protected String createPrepareStatementStr(int batchSize) {
    return null;
  }

}