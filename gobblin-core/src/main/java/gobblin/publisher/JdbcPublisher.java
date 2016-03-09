package gobblin.publisher;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.JobCommitPolicy;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.jdbc.DataSourceBuilder;

public class JdbcPublisher extends DataPublisher {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcPublisher.class);

  private static final String INSERT_STATEMENT_FORMAT = "INSERT INTO %s SELECT * FROM %s";
  private static final String DELETE_STATEMENT_FORMAT = "DELETE FROM %s";
  private Connection conn;
  private boolean failed;


  public JdbcPublisher(State state) {
    super(state);
    validate(getState());
    try {
      conn = createConnection();
      conn.setAutoCommit(false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void validate(State state) {
    JobCommitPolicy jobCommitPolicy = JobCommitPolicy.getCommitPolicy(this.getState().getProperties());
    if (JobCommitPolicy.COMMIT_ON_FULL_SUCCESS != jobCommitPolicy) {
      throw new IllegalArgumentException(this.getClass().getSimpleName() + " won't publish as already commited by task. Job commit policy " + jobCommitPolicy);
    }

    if (!state.getPropAsBoolean(ConfigurationKeys.PUBLISH_DATA_AT_JOB_LEVEL, ConfigurationKeys.DEFAULT_PUBLISH_DATA_AT_JOB_LEVEL)) {
      throw new IllegalArgumentException(this.getClass().getSimpleName() + " won't publish as " + ConfigurationKeys.PUBLISH_DATA_AT_JOB_LEVEL + " is set as false");
    }
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

  @Override
  public void close() throws IOException {
    try {
      if (failed) {
        LOG.info("Rolling back transaction as failed publishing.");
        conn.rollback();
      } else {
        conn.commit();
      }
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void initialize() throws IOException {
  }

  /**
   * 1. Truncate destination table if requested
   * 2. Move data from staging to destination
   * 3. Update workunit state
   *
   * TODO Parallel support
   * {@inheritDoc}
   * @see gobblin.publisher.DataPublisher#publishData(java.util.Collection)
   */
  @Override
  public void publishData(Collection<? extends WorkUnitState> states) throws IOException {
    LOG.info("Start publishing data");
    LOG.info("WorkUnitStates: " + states);
    int branches = state.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1);
    Set<String> truncatedDestinationTables = Sets.newHashSet();
    for (int i = 0; i < branches; i++) {

      String destinationTable = state.getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.JDBC_PUBLISHER_FINAL_TABLE_NAME, branches, i));
      Objects.requireNonNull(destinationTable);

      if(state.getPropAsBoolean(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.JDBC_PUBLISHER_REPLACE_FINAL_TABLE, branches, i), false)
         && !truncatedDestinationTables.contains(destinationTable)) {
        String deleteSql = String.format(DELETE_STATEMENT_FORMAT, destinationTable);
        LOG.info("Deleting table " + destinationTable + " , SQL: " + deleteSql);
        try {
          conn.prepareStatement(deleteSql).execute();
          truncatedDestinationTables.add(destinationTable);
        } catch (SQLException e) {
          for (WorkUnitState workUnitState : states) {
            workUnitState.setWorkingState(WorkUnitState.WorkingState.FAILED);
          }
          throw new RuntimeException("Failed to delete destination table " + destinationTable, e);
        }
      }
      Map<String, List<WorkUnitState>> stagingTables = getStagingTables(states, branches, i);

      for (Map.Entry<String, List<WorkUnitState>> entry : stagingTables.entrySet()) {
        String stagingTable = entry.getKey();
        String sql = String.format(INSERT_STATEMENT_FORMAT, destinationTable, stagingTable);
        LOG.info("Copying data from staging table + " + stagingTable + " into destination table " + destinationTable + " , SQL: " + sql);
        try {
          conn.prepareStatement(sql).execute();
          for (WorkUnitState workUnitState : entry.getValue()) {
            workUnitState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
          }
        } catch (Exception e) {
          LOG.error("Failed copying data from staging to destination table. SQL: " + sql, e);
          failed = true;
          for (WorkUnitState workUnitState : entry.getValue()) {
            workUnitState.setWorkingState(WorkUnitState.WorkingState.FAILED);
          }
        }
      }
    }
  }

  private Map<String, List<WorkUnitState>> getStagingTables(Collection<? extends WorkUnitState> states, int branches, int i) {
    Map<String, List<WorkUnitState>> stagingTables = Maps.newHashMap();
    for (WorkUnitState workUnitState : states) {
      String stagingTableKey =
          ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_TABLE, branches, i);
      String stagingTable = Objects.requireNonNull(workUnitState.getProp(stagingTableKey));
      List<WorkUnitState> existing = stagingTables.get(stagingTable);
      if(existing == null) {
        existing = Lists.newArrayList();
        stagingTables.put(stagingTable, existing);
      }
      existing.add(workUnitState);
    }
    return stagingTables;
  }

  @Override
  public void publishMetadata(Collection<? extends WorkUnitState> states) throws IOException {
  }
}
