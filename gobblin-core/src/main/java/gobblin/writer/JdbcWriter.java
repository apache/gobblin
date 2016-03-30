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

package gobblin.writer;

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
    this.state.appendToListProp(ConfigurationKeys.FORK_BRANCH_ID_KEY, Integer.toString(builder.branch));

    String databaseTableKey = ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.JDBC_PUBLISHER_DATABASE_NAME,
                                                                         builder.branches,
                                                                         builder.branch);
    this.databaseName = Preconditions.checkNotNull(state.getProp(databaseTableKey), "Staging table is missing with key " + databaseTableKey);

    String stagingTableKey = ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_TABLE,
                                                                        builder.branches,
                                                                        builder.branch);
    this.tableName = Preconditions.checkNotNull(state.getProp(stagingTableKey), "Staging table is missing with key " + stagingTableKey);
    this.commands = new JdbcWriterCommandsFactory().newInstance(state);
    try {
      this.conn = createConnection();
      conn.setAutoCommit(false);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  public JdbcWriter(JdbcWriterCommands commands, State state, String databaseName, String table, Connection conn) throws SQLException {
    this.commands = commands;
    this.state = state;
    this.databaseName = databaseName;
    this.tableName = table;
    this.conn = conn;
  }

  private Connection createConnection() throws SQLException {
    DataSource dataSource = DataSourceBuilder.builder()
                                             .url(state.getProp(ConfigurationKeys.JDBC_PUBLISHER_URL))
                                             .driver(state.getProp(ConfigurationKeys.JDBC_PUBLISHER_DRIVER))
                                             .userName(state.getProp(ConfigurationKeys.JDBC_PUBLISHER_USERNAME))
                                             .passWord(state.getProp(ConfigurationKeys.JDBC_PUBLISHER_PASSWORD))
                                             .maxActiveConnections(1)
                                             .maxIdleConnections(1)
                                             .state(state)
                                             .build();

    return dataSource.getConnection();
  }

  /**
   * Invokes JdbcWriterCommands.insert
   * {@inheritDoc}
   * @see gobblin.writer.DataWriter#write(java.lang.Object)
   */
  @Override
  public void write(JdbcEntryData record) throws IOException {
    if(LOG.isDebugEnabled()) {
      LOG.debug("Writing " + record);
    }
    try {
      commands.insert(conn, databaseName, tableName, record);
      recordWrittenCount++;
    } catch (Exception e) {
      failed = true;
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
      commands.flush(conn);
      LOG.info("Commiting transaction.");
      conn.commit();
    } catch (Exception e) {
      failed = true;
      throw new RuntimeException(e);
    }
  }

  /**
   * Staging table is needed by publisher and won't be cleaned here.
   * {@inheritDoc}
   * @see gobblin.writer.DataWriter#cleanup()
   */
  @Override
  public void cleanup() throws IOException {
  }

  /**
   * If there's a failure, it will execute roll back.
   * {@inheritDoc}
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() throws IOException {
    try {
      try {
        if (failed && conn != null) {
          conn.rollback();
        }
      } finally {
        if(conn != null) {
          conn.close();
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long recordsWritten() {
    return recordWrittenCount;
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
