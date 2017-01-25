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

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import gobblin.configuration.State;
import gobblin.converter.jdbc.JdbcEntryData;
import gobblin.converter.jdbc.JdbcEntryDatum;
import lombok.ToString;


/**
 * The implementation of JdbcBufferedInserter for Teradata.
 * Writing is done by executing {@link JdbcBufferedInserter#WRITER_JDBC_INSERT_BATCH_SIZE} sized batch inserts.
 *
 * @author Lorand Bendig
 *
 */
@ToString
public class TeradataBufferedInserter extends BaseJdbcBufferedInserter {

  private static final Logger LOG = LoggerFactory.getLogger(TeradataBufferedInserter.class);

  private Map<Integer, Integer> columnPosSqlTypes;

  public TeradataBufferedInserter(State state, Connection conn) {
    super(state, conn);
  }

  @Override
  protected boolean insertBatch(PreparedStatement pstmt) throws SQLException {
    for (JdbcEntryData pendingEntry : TeradataBufferedInserter.this.pendingInserts) {
      int i = 1;
      for (JdbcEntryDatum datum : pendingEntry) {
        Object value = datum.getVal();
        if (value != null) {
          pstmt.setObject(i, value);
        } else {
          // Column type is needed for null value insertion
          pstmt.setNull(i, columnPosSqlTypes.get(i));
        }
        i++;
      }
      pstmt.addBatch();
      pstmt.clearParameters();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Executing SQL " + pstmt);
    }
    int[] execStatus = pstmt.executeBatch();
    // Check status explicitly if driver continues batch insertion upon failure
    for (int status : execStatus) {
      if (status == Statement.EXECUTE_FAILED) {
        throw new BatchUpdateException("Batch insert failed.", execStatus);
      }
    }
    return true;
  }

  @Override
  protected String createPrepareStatementStr(int batchSize) {
    final String VALUE_FORMAT = "(%s)";
    StringBuilder sb = new StringBuilder(this.insertStmtPrefix);
    String values =
        String.format(VALUE_FORMAT, JOINER_ON_COMMA.useForNull("?").join(new String[this.columnNames.size()]));
    sb.append(values);
    return sb.append(';').toString();
  }

  @Override
  protected void initializeBatch(String databaseName, String table)
      throws SQLException {
    super.initializeBatch(databaseName, table);
    this.columnPosSqlTypes = getColumnPosSqlTypes();
  }

  /**
   * Creates a mapping between column positions and their data types
   * @return A map containing the position of the columns along with their data type as value
   */
  private Map<Integer, Integer> getColumnPosSqlTypes() {
    try {
      final Map<Integer, Integer> columnPosSqlTypes = Maps.newHashMap();
      ParameterMetaData pMetaData = this.insertPstmtForFixedBatch.getParameterMetaData();
      for (int i = 1; i <= pMetaData.getParameterCount(); i++) {
        columnPosSqlTypes.put(i, pMetaData.getParameterType(i));
      }
      return columnPosSqlTypes;
    } catch (SQLException e) {
      throw new RuntimeException("Cannot retrieve columns types for batch insert", e);
    }
  }

}