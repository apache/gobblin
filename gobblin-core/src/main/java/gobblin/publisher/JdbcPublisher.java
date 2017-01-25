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

package gobblin.publisher;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.JobCommitPolicy;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.jdbc.DataSourceBuilder;
import gobblin.writer.commands.JdbcWriterCommands;
import gobblin.writer.commands.JdbcWriterCommandsFactory;


/**
 * Publishes data into JDBC RDBMS. Expects all the data has been already in staging table.
 */
public class JdbcPublisher extends DataPublisher {
  public static final String JDBC_PUBLISHER_PREFIX = "jdbc.publisher.";
  public static final String JDBC_PUBLISHER_DATABASE_NAME = JDBC_PUBLISHER_PREFIX + "database_name";
  public static final String JDBC_PUBLISHER_FINAL_TABLE_NAME = JDBC_PUBLISHER_PREFIX + "table_name";
  public static final String JDBC_PUBLISHER_REPLACE_FINAL_TABLE = JDBC_PUBLISHER_PREFIX + "replace_table";
  public static final String JDBC_PUBLISHER_USERNAME = JDBC_PUBLISHER_PREFIX + "username";
  public static final String JDBC_PUBLISHER_PASSWORD = JDBC_PUBLISHER_PREFIX + "password";
  public static final String JDBC_PUBLISHER_ENCRYPTION_KEY_LOC = JDBC_PUBLISHER_PREFIX + "encrypt_key_loc";
  public static final String JDBC_PUBLISHER_URL = JDBC_PUBLISHER_PREFIX + "url";
  public static final String JDBC_PUBLISHER_TIMEOUT = JDBC_PUBLISHER_PREFIX + "timeout";
  public static final String JDBC_PUBLISHER_DRIVER = JDBC_PUBLISHER_PREFIX + "driver";

  private static final Logger LOG = LoggerFactory.getLogger(JdbcPublisher.class);
  private final JdbcWriterCommandsFactory jdbcWriterCommandsFactory;

  /**
   * Expects all data is in staging table ready to be published. To validate this, it checks COMMIT_ON_FULL_SUCCESS and PUBLISH_DATA_AT_JOB_LEVEL
   * @param state
   * @param jdbcWriterCommandsFactory
   * @param conn
   */
  @VisibleForTesting
  public JdbcPublisher(State state, JdbcWriterCommandsFactory jdbcWriterCommandsFactory) {
    super(state);
    this.jdbcWriterCommandsFactory = jdbcWriterCommandsFactory;
    validate(getState());
  }

  public JdbcPublisher(State state) {
    this(state, new JdbcWriterCommandsFactory());
    validate(getState());
  }

  /**
   * @param state
   * @throws IllegalArgumentException If job commit policy is not COMMIT_ON_FULL_SUCCESS or is not on PUBLISH_DATA_AT_JOB_LEVEL
   */
  private void validate(State state) {
    JobCommitPolicy jobCommitPolicy = JobCommitPolicy.getCommitPolicy(this.getState().getProperties());
    if (JobCommitPolicy.COMMIT_ON_FULL_SUCCESS != jobCommitPolicy) {
      throw new IllegalArgumentException(this.getClass().getSimpleName()
          + " won't publish as already commited by task. Job commit policy " + jobCommitPolicy);
    }

    if (!state.getPropAsBoolean(ConfigurationKeys.PUBLISH_DATA_AT_JOB_LEVEL,
        ConfigurationKeys.DEFAULT_PUBLISH_DATA_AT_JOB_LEVEL)) {
      throw new IllegalArgumentException(this.getClass().getSimpleName() + " won't publish as "
          + ConfigurationKeys.PUBLISH_DATA_AT_JOB_LEVEL + " is set as false");
    }
  }

  @VisibleForTesting
  public Connection createConnection() {
    DataSource dataSource = DataSourceBuilder.builder().url(this.state.getProp(JDBC_PUBLISHER_URL))
        .driver(this.state.getProp(JDBC_PUBLISHER_DRIVER)).userName(this.state.getProp(JDBC_PUBLISHER_USERNAME))
        .passWord(this.state.getProp(JDBC_PUBLISHER_PASSWORD))
        .cryptoKeyLocation(this.state.getProp(JDBC_PUBLISHER_ENCRYPTION_KEY_LOC)).maxActiveConnections(1)
        .maxIdleConnections(1).state(this.state).build();
    try {
      return dataSource.getConnection();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {}

  @Override
  public void initialize() throws IOException {}

  /**
   * 1. Truncate destination table if requested
   * 2. Move data from staging to destination
   * 3. Update Workunit state
   *
   * TODO: Research on running this in parallel. While testing publishing it in parallel, it turns out delete all from the table locks the table
   * so that copying table threads wait until transaction lock times out and throwing exception(MySQL). Is there a way to avoid this?
   *
   * {@inheritDoc}
   * @see gobblin.publisher.DataPublisher#publishData(java.util.Collection)
   */
  @Override
  public void publishData(Collection<? extends WorkUnitState> states) throws IOException {
    LOG.info("Start publishing data");
    int branches = this.state.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1);
    Set<String> emptiedDestTables = Sets.newHashSet();

    final Connection conn = createConnection();
    final JdbcWriterCommands commands = this.jdbcWriterCommandsFactory.newInstance(this.state, conn);
    try {
      conn.setAutoCommit(false);

      for (int i = 0; i < branches; i++) {
        final String destinationTable = this.state
            .getProp(ForkOperatorUtils.getPropertyNameForBranch(JDBC_PUBLISHER_FINAL_TABLE_NAME, branches, i));
        final String databaseName =
            this.state.getProp(ForkOperatorUtils.getPropertyNameForBranch(JDBC_PUBLISHER_DATABASE_NAME, branches, i));
        Preconditions.checkNotNull(destinationTable);

        if (this.state.getPropAsBoolean(
            ForkOperatorUtils.getPropertyNameForBranch(JDBC_PUBLISHER_REPLACE_FINAL_TABLE, branches, i), false)
            && !emptiedDestTables.contains(destinationTable)) {
          LOG.info("Deleting table " + destinationTable);
          commands.deleteAll(databaseName, destinationTable);
          emptiedDestTables.add(destinationTable);
        }

        Map<String, List<WorkUnitState>> stagingTables = getStagingTables(states, branches, i);
        for (Map.Entry<String, List<WorkUnitState>> entry : stagingTables.entrySet()) {
          String stagingTable = entry.getKey();
          LOG.info("Copying data from staging table " + stagingTable + " into destination table " + destinationTable);
          commands.copyTable(databaseName, stagingTable, destinationTable);
          for (WorkUnitState workUnitState : entry.getValue()) {
            workUnitState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
          }
        }
      }
      LOG.info("Commit publish data");
      conn.commit();
    } catch (Exception e) {
      try {
        LOG.error("Failed publishing. Rolling back.");
        conn.rollback();
      } catch (SQLException se) {
        LOG.error("Failed rolling back.", se);
      }
      throw new RuntimeException("Failed publishing", e);
    } finally {
      try {
        conn.close();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static Map<String, List<WorkUnitState>> getStagingTables(Collection<? extends WorkUnitState> states,
      int branches, int i) {
    Map<String, List<WorkUnitState>> stagingTables = Maps.newHashMap();
    for (WorkUnitState workUnitState : states) {
      String stagingTableKey =
          ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_TABLE, branches, i);
      String stagingTable = Preconditions.checkNotNull(workUnitState.getProp(stagingTableKey));
      List<WorkUnitState> existing = stagingTables.get(stagingTable);
      if (existing == null) {
        existing = Lists.newArrayList();
        stagingTables.put(stagingTable, existing);
      }
      existing.add(workUnitState);
    }
    return stagingTables;
  }

  @Override
  public void publishMetadata(Collection<? extends WorkUnitState> states) throws IOException {}
}
