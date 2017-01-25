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

package gobblin.writer;

import gobblin.publisher.JdbcPublisher;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.jdbc.DataSourceBuilder;
import gobblin.writer.commands.JdbcWriterCommands;
import gobblin.writer.commands.JdbcWriterCommandsFactory;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.converter.jdbc.JdbcEntryData;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;


/**
 * Uses JDBC to persist data in task level.
 * For interaction with JDBC underlying RDBMS, it uses JdbcWriterCommands.
 *
 * JdbcWriter will open single transaction for it’s simplicity on failure handling.
 *
 * Scalability issue may come with long transaction can be overcome by increasing partition which will make transaction short.
 * (Data with 200M record was tested with single transaction and it had no problem in MySQL 5.6.)
 *
 * Having one transaction per writer has its tradeoffs:
 *   Pro: Simple on failure handling as you can just simply execute rollback on failure. Basically, it will revert back to previous state so that the job can retry the task.
 *   Con: It can lead up to long lived transaction and it can face scalability issue. (Not enough disk space for transaction log, number of record limit on one transaction (200M for Postgre sql), etc)
 *
 * During the design meeting, we’ve discussed that long transaction could be a problem. One suggestion came out during the meeting was commit periodically.
 * This will address long transaction problem, but we also discussed it would be hard on failure handling.
 * Currently, Gobblin does task level retry on failure and there were three options we’ve discussed.
 * (There was no silver bullet solution from the meeting.) Note that these are all with committing periodically.
 * Revert to previous state: For writer, this will be delete the record it wrote.
 *     For JdbcWriter, it could use it’s own staging table or could share staging table with other writer.
 *     As staging table can be passed by user where we don’t have control of, not able to add partition information, it is hard to revert back to previous state for all cases.
 * Ignore duplicate: The idea is to use Upsert to perform insert or update.
 *     As it needs to check the current existence in the dataset, it is expected to show performance degradation.
 *     Also, possibility of duplicate entry was also discussed.
 * Water mark: In order to use water mark in task level, writer needs to send same order when retried which is not guaranteed.
 */
public class JdbcWriter implements DataWriter<JdbcEntryData> {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcWriter.class);

  private final Connection conn;
  private final State state;
  private final JdbcWriterCommands commands;
  private final String databaseName;
  private final String tableName;

  private boolean failed;
  private long recordWrittenCount;

  public JdbcWriter(JdbcWriterBuilder builder) {
    this.state = builder.destination.getProperties();
    this.state.setProp(ConfigurationKeys.FORK_BRANCH_ID_KEY, Integer.toString(builder.branch));

    String databaseTableKey = ForkOperatorUtils.getPropertyNameForBranch(JdbcPublisher.JDBC_PUBLISHER_DATABASE_NAME,
        builder.branches, builder.branch);
    this.databaseName = Preconditions.checkNotNull(this.state.getProp(databaseTableKey),
        "Staging table is missing with key " + databaseTableKey);

    String stagingTableKey = ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_TABLE,
        builder.branches, builder.branch);
    this.tableName = Preconditions.checkNotNull(this.state.getProp(stagingTableKey),
        "Staging table is missing with key " + stagingTableKey);
    try {
      this.conn = createConnection();
      this.commands = new JdbcWriterCommandsFactory().newInstance(this.state, this.conn);
      this.commands.setConnectionParameters(this.state.getProperties(), this.conn);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  public JdbcWriter(JdbcWriterCommands commands, State state, String databaseName, String table, Connection conn) {
    this.commands = commands;
    this.state = state;
    this.databaseName = databaseName;
    this.tableName = table;
    this.conn = conn;
  }

  private Connection createConnection() throws SQLException {
    DataSource dataSource = DataSourceBuilder.builder().url(this.state.getProp(JdbcPublisher.JDBC_PUBLISHER_URL))
        .driver(this.state.getProp(JdbcPublisher.JDBC_PUBLISHER_DRIVER))
        .userName(this.state.getProp(JdbcPublisher.JDBC_PUBLISHER_USERNAME))
        .passWord(this.state.getProp(JdbcPublisher.JDBC_PUBLISHER_PASSWORD))
        .cryptoKeyLocation(this.state.getProp(JdbcPublisher.JDBC_PUBLISHER_ENCRYPTION_KEY_LOC)).maxActiveConnections(1)
        .maxIdleConnections(1).state(this.state).build();

    return dataSource.getConnection();
  }

  /**
   * Invokes JdbcWriterCommands.insert
   * {@inheritDoc}
   * @see gobblin.writer.DataWriter#write(java.lang.Object)
   */
  @Override
  public void write(JdbcEntryData record) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Writing " + record);
    }
    try {
      this.commands.insert(this.databaseName, this.tableName, record);
      this.recordWrittenCount++;
    } catch (Exception e) {
      this.failed = true;
      throw new RuntimeException(e);
    }
  }

  /**
   * Flushes JdbcWriterCommands and commit.
   * {@inheritDoc}
   * @see gobblin.writer.DataWriter#commit()
   */
  @Override
  public void commit() throws IOException {
    try {
      LOG.info("Flushing pending insert.");
      this.commands.flush();
      LOG.info("Commiting transaction.");
      this.conn.commit();
    } catch (Exception e) {
      this.failed = true;
      throw new RuntimeException(e);
    }
  }

  /**
   * Staging table is needed by publisher and won't be cleaned here.
   * {@inheritDoc}
   * @see gobblin.writer.DataWriter#cleanup()
   */
  @Override
  public void cleanup() throws IOException {}

  /**
   * If there's a failure, it will execute roll back.
   * {@inheritDoc}
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() throws IOException {
    try {
      try {
        if (this.failed && this.conn != null) {
          this.conn.rollback();
        }
      } finally {
        if (this.conn != null) {
          this.conn.close();
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long recordsWritten() {
    return this.recordWrittenCount;
  }

  /**
   * This is not supported for JDBC writer.
   * {@inheritDoc}
   * @see gobblin.writer.DataWriter#bytesWritten()
   */
  @Override
  public long bytesWritten() throws IOException {
    return -1L;
  }
}
