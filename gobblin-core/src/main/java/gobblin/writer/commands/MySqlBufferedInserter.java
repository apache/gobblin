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

package gobblin.writer.commands;

import gobblin.configuration.State;
import gobblin.converter.jdbc.JdbcEntryData;
import gobblin.converter.jdbc.JdbcEntryDatum;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import lombok.ToString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;


/**
 * The implementation of JdbcBufferedInserter for MySQL.
 * This purpose of buffered insert is mainly for performance reason and the implementation is based on the
 * reference manual http://dev.mysql.com/doc/refman/5.0/en/insert-speed.html
 */
@ToString
public class MySqlBufferedInserter implements JdbcBufferedInserter {
  private static final Logger LOG = LoggerFactory.getLogger(MySqlBufferedInserter.class);

  private static final String INSERT_STATEMENT_PREFIX_FORMAT = "INSERT INTO %s.%s (%s) VALUES ";
  private static final Joiner JOINER_ON_COMMA = Joiner.on(',');

  private List<JdbcEntryData> pendingInserts;
  private List<String> columnNames;
  private String insertStmtPrefix;
  private PreparedStatement insertPstmtForFixedBatch;
  private Retryer<Boolean> retryer;

  private int batchSize;
  private final int maxParamSize;
  private final Connection conn;

  public MySqlBufferedInserter(State state, Connection conn) {
    this.conn = conn;
    this.batchSize = state.getPropAsInt(WRITER_JDBC_INSERT_BATCH_SIZE, DEFAULT_WRITER_JDBC_INSERT_BATCH_SIZE);
    if (this.batchSize < 1) {
      throw new IllegalArgumentException(WRITER_JDBC_INSERT_BATCH_SIZE + " should be a positive number");
    }
    this.maxParamSize = state.getPropAsInt(WRITER_JDBC_MAX_PARAM_SIZE, DEFAULT_WRITER_JDBC_MAX_PARAM_SIZE);
  }

  /**
   * Inserts entry into buffer. If current # of entries filled batch size or it overflowed the buffer, it will call underlying JDBC to actually insert it.
   * {@inheritDoc}
   * @see gobblin.writer.commands.JdbcBufferedInserter#insert(java.sql.Connection, java.lang.String, java.lang.String, gobblin.converter.jdbc.JdbcEntryData)
   */
  @Override
  public void insert(String databaseName, String table, JdbcEntryData jdbcEntryData) throws SQLException {
    if (this.columnNames == null) {
      initializeForBatch(this.conn, databaseName, table, jdbcEntryData);
    }
    this.pendingInserts.add(jdbcEntryData);

    if (this.pendingInserts.size() == this.batchSize) {
      insertBatch(this.insertPstmtForFixedBatch); //reuse pre-computed Preparedstatement.
      return;
    }
  }

  private void insertBatch(final PreparedStatement pstmt) {
    Callable<Boolean> insertCall = new Callable<Boolean>() { //Need a Callable interface to be wrapped by Retryer.
      @Override
      public Boolean call() throws Exception {
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
    };

    try {
      this.retryer.wrap(insertCall).call();
    } catch (Exception e) {
      throw new RuntimeException("Failed to insert.", e);
    }
    resetBatch();
  }

  /**
   * Initializes variables for batch insert and pre-compute PreparedStatement based on requested batch size and parameter size.
   * @param conn
   * @param databaseName
   * @param table
   * @param jdbcEntryData
   * @throws SQLException
   */
  private void initializeForBatch(Connection conn, String databaseName, String table, JdbcEntryData jdbcEntryData)
      throws SQLException {
    this.columnNames = Lists.newArrayList();
    for (JdbcEntryDatum datum : jdbcEntryData) {
      this.columnNames.add(datum.getColumnName());
    }
    this.pendingInserts = Lists.newArrayList();

    this.insertStmtPrefix =
        String.format(INSERT_STATEMENT_PREFIX_FORMAT, databaseName, table, JOINER_ON_COMMA.join(this.columnNames));
    int actualBatchSize = Math.min(this.batchSize, this.maxParamSize / this.columnNames.size());
    if (this.batchSize != actualBatchSize) {
      LOG.info("Changing batch size from " + this.batchSize + " to " + actualBatchSize
          + " due to # of params limitation " + this.maxParamSize + " , # of columns: " + this.columnNames.size());
    }
    this.batchSize = actualBatchSize;
    this.insertPstmtForFixedBatch =
        conn.prepareStatement(createPrepareStatementStr(this.insertStmtPrefix, this.batchSize));
    if (this.batchSize == 1) {
      LOG.info("Initialized for insert " + this);
    } else {
      LOG.info("Initialized for batch insert " + this);
    }

    //retry after 2, 4, 8, 16... sec, max 30 sec delay
    this.retryer = RetryerBuilder.<Boolean> newBuilder().retryIfException()
        .withWaitStrategy(WaitStrategies.exponentialWait(1000, 30, TimeUnit.SECONDS))
        .withStopStrategy(StopStrategies.stopAfterAttempt(5)).build();
  }

  private void resetBatch() {
    this.pendingInserts.clear();
  }

  private String createPrepareStatementStr(String insertStmtPrefix, int batchSize) {
    final String VALUE_FORMAT = "(%s)";

    StringBuilder sb = new StringBuilder(insertStmtPrefix);
    String values =
        String.format(VALUE_FORMAT, JOINER_ON_COMMA.useForNull("?").join(new String[this.columnNames.size()]));
    sb.append(values);
    for (int i = 1; i < batchSize; i++) {
      sb.append(',').append(values);
    }
    return sb.append(';').toString();
  }

  @Override
  public void flush() throws SQLException {
    if (this.pendingInserts == null || this.pendingInserts.isEmpty()) {
      return;
    }
    try (PreparedStatement pstmt =
        this.conn.prepareStatement(createPrepareStatementStr(this.insertStmtPrefix, this.pendingInserts.size()));) {
      insertBatch(pstmt);
    }
  }
}
