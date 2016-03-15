package gobblin.writer.initializer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.jdbc.DataSourceBuilder;
import gobblin.writer.commands.JdbcWriterCommands;
import gobblin.writer.commands.JdbcWriterCommandsFactory;
import gobblin.source.extractor.JobCommitPolicy;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class JdbcWriterInitializer implements WriterInitializer {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcWriterInitializer.class);
  private static final String STAGING_TABLE_FORMAT = "stage_%s_%d";
  private static final int NAMING_STAGING_TABLE_TRIAL = 10;
  private static final Random RANDOM = new Random();

  private final int branches;
  private final int branchId;
  private final State state;
  private final Collection<WorkUnit> workUnits;
  private final JdbcWriterCommands commands;
  private String userCreatedStagingTable;
  private String createdStagingTable;

  public JdbcWriterInitializer(State state, Collection<WorkUnit> workUnits) {
    this(state, workUnits, 1, 0);
  }

  public JdbcWriterInitializer(State state, Collection<WorkUnit> workUnits, int branches, int branchId) {
    validateInput(state);
    this.state = state;
    this.workUnits = Lists.newArrayList(workUnits);
    this.branches = branches;
    this.branchId = branchId;
    this.commands = JdbcWriterCommandsFactory.newInstance(state);
  }

  @Override
  public void close() {
    LOG.info("Closing " + this.getClass().getSimpleName());
    try (Connection conn = createConnection()) {
      if(createdStagingTable != null) {
        LOG.info("Dropping staging table " + createdStagingTable);
        commands.drop(conn, createdStagingTable);
      }

      if(userCreatedStagingTable != null) {
        LOG.info("Truncating staging table " + userCreatedStagingTable);
        commands.drop(conn, userCreatedStagingTable);
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to close", e);
    }
  }

  /**
   * Creating JDBC connection using publisher's connection information. It is OK to use publisher's information
   * as JdbcWriter is coupled with JdbcPublisher.
   *
   * @return JDBC Connection
   * @throws SQLException
   */
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

  private String createStagingTable(Connection conn) throws SQLException {
    String destTableKey = ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.JDBC_PUBLISHER_FINAL_TABLE_NAME, branches, branchId);
    String destinationTable = state.getProp(destTableKey);
    if(StringUtils.isEmpty(destinationTable)) {
      throw new IllegalArgumentException(ConfigurationKeys.JDBC_PUBLISHER_FINAL_TABLE_NAME + " is required for " + this.getClass().getSimpleName() + " for branch " + branchId);
    }

    String stagingTable = null;
    for (int i = 0; i < NAMING_STAGING_TABLE_TRIAL; i++) {
      String tmp = String.format(STAGING_TABLE_FORMAT, destinationTable, System.nanoTime());
      LOG.info("Check if staging table " + tmp + " exists.");
      ResultSet res = conn.getMetaData().getTables(null, null, tmp, new String[] {"TABLE"});
      if (!res.next()) {
        LOG.info("Staging table " + tmp + " does not exist. Creating.");
        try {
          commands.createTableStructure(conn, destinationTable, tmp);
          LOG.info("Test if staging table can be dropped. Test by dropping and Creating staging table.");
          commands.drop(conn, tmp);
          commands.createTableStructure(conn, destinationTable, tmp);
          stagingTable = tmp;
          break;
        } catch (SQLException e) {
          LOG.warn("Failed to create table. Retrying up to " + NAMING_STAGING_TABLE_TRIAL + " times", e);
        }
      } else {
        LOG.info("Staging table " + tmp + " exists.");
      }
      try {
        TimeUnit.MILLISECONDS.sleep(RANDOM.nextInt(1000));
      } catch (InterruptedException e) {
        LOG.info("Sleep has been interrupted.", e);
      }
    }

    if (!StringUtils.isEmpty(stagingTable)) {
     return stagingTable;
    }
    throw new RuntimeException("Failed to create staging table");
  }

  private String getProp(State state, String key, int branches, int branchId) {
    String forkedKey = ForkOperatorUtils.getPropertyNameForBranch(key, branches, branchId);
    return state.getProp(forkedKey);
  }

  private boolean getPropAsBoolean(State state, String key, int branches, int branchId) {
    return Boolean.parseBoolean(getProp(state, key, branches, branchId));
  }

  /**
   * Initializes AvroFileJdbcSource for Writer that needs to be happen in single threaded environment.
   * On each branch:
   * 1. Check if user chose to skip the staging table
   * 1.1. If user chose to skip the staging table, and user decided to replace final table, truncate final table.
   * 2. (User didn't choose to skip the staging table.) Check if user passed the staging table.
   * 2.1 If user hasn't passed the staging table:
   * 2.1.1 Check if user has permission to drop the table
   * 2.1.2 Create staging table with unique name.
   * 2.2 If user passed the staging table, use it.
   * 3. Confirm if staging table is empty.
   * 4. Update Workunit state with staging table information.
   * @param state
   */
  @Override
  public void initialize() {
    try (Connection conn = createConnection()) {
      JobCommitPolicy jobCommitPolicy = JobCommitPolicy.getCommitPolicy(state);
      boolean isSkipStaging = !JobCommitPolicy.COMMIT_ON_FULL_SUCCESS.equals(JobCommitPolicy.getCommitPolicy(state));
      if(isSkipStaging) {
        LOG.info("Writer will write directly to destination table as JobCommitPolicy is " + jobCommitPolicy);
      }
      int branches = state.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1);

      Queue<String> stagingTables = new LinkedList<>();
      final String publishTable = getProp(state, ConfigurationKeys.JDBC_PUBLISHER_FINAL_TABLE_NAME, branches, branchId);
      for (int i = 0; i < workUnits.size(); i++) {
        if (isSkipStaging) {
          LOG.info("User chose to skip staing table on branch " + branchId);
          stagingTables.add(publishTable);

          if (getPropAsBoolean(state, ConfigurationKeys.JDBC_PUBLISHER_REPLACE_FINAL_TABLE, branches, branchId)) {
            LOG.info("User chose to replace final table " + publishTable + " on branch " + branchId);
            commands.truncate(conn, publishTable);
          }
          continue;
        }

        String stagingTableKey =
            ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_TABLE, branches, branchId);
        String stagingTable = state.getProp(stagingTableKey);
        if (!StringUtils.isEmpty(stagingTable)) {
          LOG.info("Staging table for branch " + branchId + " from user: " + stagingTable);
          if (!commands.isEmpty(conn, stagingTable)) {
            LOG.error("Staging table " + stagingTable + " is not empty. Failing.");
            throw new IllegalArgumentException("Staging table " + stagingTable + " should be empty.");
          }

          stagingTables.add(stagingTable);
          userCreatedStagingTable = stagingTable;
          continue;
        }

        LOG.info("Staging table has not been passed from user for branch " + branchId + ". Creating.");
        stagingTable = createStagingTable(conn);
        stagingTables.add(stagingTable);
        createdStagingTable = stagingTable;
        LOG.info("Staging table " + stagingTable + " has been created for branchId " + branchId);
      }

      //Update work unit states
      for (WorkUnit wu : workUnits) {
          String stagingTable = stagingTables.remove();
          String stagingTableKey = ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_TABLE, branches, branchId);
          LOG.info("Update workunit state " + stagingTableKey + " , " + stagingTable);
          wu.setProp(stagingTableKey, stagingTable);
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed with SQL", e);
    }
  }

  /**
   * 1. User should not define same destination table across branches.
   * 2. User should not define same staging table across branches.
   * 3. If commit policy is not full, Gobblin will try to write into final table even there's a failure. This will let Gobblin to write in task level.
   *    However, publish data at job level is true, it contradicts with the behavior of Gobblin writing in task level. Thus, validate publish data at job level is false if commit policy is not full.
   * @param state
   */
  private void validateInput(State state) {
    int branches = state.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1);
    Set<String> publishTables = Sets.newHashSet();

    for (int branchId = 0; branchId < branches; branchId++) {
      String publishTable = Objects.requireNonNull(getProp(state, ConfigurationKeys.JDBC_PUBLISHER_FINAL_TABLE_NAME, branches, branchId),
          ConfigurationKeys.JDBC_PUBLISHER_FINAL_TABLE_NAME + " should not be null.");
      if(publishTables.contains(publishTable)) {
        throw new IllegalArgumentException("Duplicate " + ConfigurationKeys.JDBC_PUBLISHER_FINAL_TABLE_NAME + " is not allowed across branches");
      }
      publishTables.add(publishTable);
    }

    Set<String> stagingTables = Sets.newHashSet();
    for (int branchId = 0; branchId < branches; branchId++) {
      String stagingTable = getProp(state, ConfigurationKeys.WRITER_STAGING_TABLE, branches, branchId);
      if(!StringUtils.isEmpty(stagingTable) && stagingTables.contains(stagingTable)) {
        throw new IllegalArgumentException("Duplicate " + ConfigurationKeys.WRITER_STAGING_TABLE + " is not allowed across branches");
      }
      stagingTables.add(stagingTable);
    }

    JobCommitPolicy policy = JobCommitPolicy.getCommitPolicy(state);
    boolean isPublishJobLevel = state.getPropAsBoolean(ConfigurationKeys.PUBLISH_DATA_AT_JOB_LEVEL, ConfigurationKeys.DEFAULT_PUBLISH_DATA_AT_JOB_LEVEL);
    if(!JobCommitPolicy.COMMIT_ON_FULL_SUCCESS.equals(policy) && isPublishJobLevel) {
      throw new IllegalArgumentException("Cannot publish on job level when commit policy is " + policy + " To skip staging table set " + ConfigurationKeys.PUBLISH_DATA_AT_JOB_LEVEL + " to false");
    }
  }
}
