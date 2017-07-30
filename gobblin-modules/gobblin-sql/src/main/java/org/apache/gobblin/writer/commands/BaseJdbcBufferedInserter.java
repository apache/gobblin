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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import gobblin.configuration.State;
import gobblin.converter.jdbc.JdbcEntryData;
import gobblin.converter.jdbc.JdbcEntryDatum;
import lombok.ToString;


/**
 * Base implementation of JdbcBufferedInserter.
 * Concrete DB specific implementations are expected to subclass this class.
 *
 */
@ToString
public abstract class BaseJdbcBufferedInserter implements JdbcBufferedInserter {

  private static final Logger LOG = LoggerFactory.getLogger(BaseJdbcBufferedInserter.class);

  protected static final String INSERT_STATEMENT_PREFIX_FORMAT = "INSERT INTO %s.%s (%s) VALUES ";
  protected static final Joiner JOINER_ON_COMMA = Joiner.on(',');

  protected final Connection conn;

  // Rows that are inserted at once in one batch cycle
  protected final List<JdbcEntryData> pendingInserts = Lists.newArrayList();

  protected final List<String> columnNames = Lists.newArrayList();

  protected int batchSize;
  protected String insertStmtPrefix;
  protected PreparedStatement insertPstmtForFixedBatch;
  private final Retryer<Boolean> retryer;

  public BaseJdbcBufferedInserter(State state, Connection conn) {
    this.conn = conn;
    this.batchSize = state.getPropAsInt(WRITER_JDBC_INSERT_BATCH_SIZE, DEFAULT_WRITER_JDBC_INSERT_BATCH_SIZE);
    if (this.batchSize < 1) {
      throw new IllegalArgumentException(WRITER_JDBC_INSERT_BATCH_SIZE + " should be a positive number");
    }

    int maxWait = state.getPropAsInt(WRITER_JDBC_INSERT_RETRY_TIMEOUT, DEFAULT_WRITER_JDBC_INSERT_RETRY_TIMEOUT);
    int maxAttempts =
        state.getPropAsInt(WRITER_JDBC_INSERT_RETRY_MAX_ATTEMPT, DEFAULT_WRITER_JDBC_INSERT_RETRY_MAX_ATTEMPT);

    //retry after 2, 4, 8, 16... sec, allow at most maxWait sec delay
    this.retryer = RetryerBuilder.<Boolean> newBuilder().retryIfException()
        .withWaitStrategy(WaitStrategies.exponentialWait(1000, maxWait, TimeUnit.SECONDS))
        .withStopStrategy(StopStrategies.stopAfterAttempt(maxAttempts)).build();
  }

  /**
   * Adds all the records from {@link #pendingInserts} to the PreparedStatement and executes the
   * batch insert.
   *
   * @param pstmt PreparedStatement object
   * @return true if the insert was successful
   */
  protected abstract boolean insertBatch(final PreparedStatement pstmt) throws SQLException;

  /**
   * Constructs the SQL insert statement for the batch inserts, using the {@link #INSERT_STATEMENT_PREFIX_FORMAT}
   *
   * @param batchSize size of one batch
   * @return the constructed SQL string for batch inserts
   */
  protected abstract String createPrepareStatementStr(int batchSize);

  /**
   * <p>
   *   Inserts entry into buffer. If current # of entries filled batch size or it overflowed the buffer,
   *   it will call underlying JDBC to actually insert it.
   * </p>
   *
   * {@inheritDoc}
   * @see gobblin.writer.commands.JdbcBufferedInserter#insert(java.lang.String, java.lang.String, gobblin.converter.jdbc.JdbcEntryData)
   */
  @Override
  public void insert(String databaseName, String table, JdbcEntryData jdbcEntryData) throws SQLException {
    if (this.columnNames.isEmpty()) {
      for (JdbcEntryDatum datum : jdbcEntryData) {
        this.columnNames.add(datum.getColumnName());
      }
      initializeBatch(databaseName, table);
    }
    this.pendingInserts.add(jdbcEntryData);

    if (this.pendingInserts.size() == this.batchSize) {
      executeBatchInsert(this.insertPstmtForFixedBatch); // Reuse pre-computed Preparedstatement.
    }
  }

  /**
   * Initializes variables for batch insert and pre-compute PreparedStatement based on requested batch size and parameter size.
   * @param databaseName
   * @param table
   * @throws SQLException
   */
  protected void initializeBatch(String databaseName, String table)
      throws SQLException {
    this.insertStmtPrefix = createInsertStatementStr(databaseName, table);
    this.insertPstmtForFixedBatch =
        this.conn.prepareStatement(createPrepareStatementStr(this.batchSize));
    LOG.info(String.format("Initialized for %s insert " + this, (this.batchSize > 1) ? "batch" : ""));
  }

  /**
   * Submits the user defined {@link #insertBatch(PreparedStatement)} call to the {@link Retryer} which takes care
   * of resubmitting the records according to {@link #WRITER_JDBC_INSERT_RETRY_TIMEOUT} and {@link #WRITER_JDBC_INSERT_RETRY_MAX_ATTEMPT}
   * when failure happens.
   *
   * @param pstmt PreparedStatement object
   */
  protected void executeBatchInsert(final PreparedStatement pstmt) {
    try {
      // Need a Callable interface to be wrapped by Retryer.
      this.retryer.wrap(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return insertBatch(pstmt);
        }
      }).call();
    } catch (Exception e) {
      throw new RuntimeException("Failed to insert.", e);
    }
    resetBatch();
  }

  /**
   * Resets the list of rows after the batch insert
   */
  protected void resetBatch() {
    this.pendingInserts.clear();
  }

  /**
   * Populates the placeholders and constructs the prefix of batch insert statement
   * @param databaseName name of the database
   * @param table name of the table
   * @return {@link #INSERT_STATEMENT_PREFIX_FORMAT} with all its resolved placeholders
   */
  protected String createInsertStatementStr(String databaseName, String table) {
    return String.format(INSERT_STATEMENT_PREFIX_FORMAT, databaseName, table, JOINER_ON_COMMA.join(this.columnNames));
  }

  @Override
  public void flush() throws SQLException {
    if (this.pendingInserts == null || this.pendingInserts.isEmpty()) {
      return;
    }
    try (PreparedStatement pstmt = this.conn.prepareStatement(createPrepareStatementStr(this.pendingInserts.size()));) {
      insertBatch(pstmt);
    }
  }

}