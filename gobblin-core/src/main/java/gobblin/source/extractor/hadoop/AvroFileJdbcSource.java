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

package gobblin.source.extractor.hadoop;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.source.Source;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.JobCommitPolicy;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.jdbc.DataSourceBuilder;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 *
 */
public class AvroFileJdbcSource implements Source<Schema, GenericRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(AvroFileJdbcSource.class);

  private Source<Schema, GenericRecord> avroFileSource;
  private Set<String> createdStagingTables;
  private Set<String> passedStagingTables;

  public AvroFileJdbcSource() {
    this.avroFileSource = new AvroFileSource();
    this.createdStagingTables = Sets.newHashSet();
    this.passedStagingTables = Sets.newHashSet();
  }

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    List<WorkUnit> workUnits = avroFileSource.getWorkunits(state);
    if(workUnits == null || workUnits.isEmpty()) {
      LOG.info("No work unit");
      return workUnits;
    }
    initializeForWriterPublisher(state, workUnits);
    return workUnits;
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
  private void initializeForWriterPublisher(SourceState state, List<WorkUnit> workUnits) {
    validateInput(state);
    try (Connection conn = createConnection(state)) {
      JobCommitPolicy jobCommitPolicy = JobCommitPolicy.getCommitPolicy(state);
      boolean isSkipStaging = !JobCommitPolicy.COMMIT_ON_FULL_SUCCESS.equals(JobCommitPolicy.getCommitPolicy(state));
      if(isSkipStaging) {
        LOG.info("Writer will write directly to destination table as JobCommitPolicy is " + jobCommitPolicy);
      }
      int branches = state.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1);

      Map<Integer, Queue<String>> stagingTables = Maps.newHashMap();
      for (int branchId = 0; branchId < branches; branchId++) {
        final String publishTable = getProp(state, ConfigurationKeys.JDBC_PUBLISHER_FINAL_TABLE_NAME, branches, branchId);
        for(int i = 0; i < workUnits.size(); i++) {
          Queue<String> stagingTablesForBranch = stagingTables.get(branchId);
          if (stagingTablesForBranch == null) {
            stagingTablesForBranch = Lists.newLinkedList();
            stagingTables.put(branchId, stagingTablesForBranch);
          }

          if(isSkipStaging) {
            LOG.info("User chose to skip staing table on branch " + branchId);
            stagingTablesForBranch.add(publishTable);

            if (getPropAsBoolean(state, ConfigurationKeys.JDBC_PUBLISHER_REPLACE_FINAL_TABLE, branches, branchId)) {
              LOG.info("User chose to replace final table " + publishTable + " on branch " + branchId);
              truncate(conn, publishTable);
            }
            continue;
          }

          String stagingTableKey = ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_TABLE, branches, 1);
          String stagingTable = state.getProp(stagingTableKey);
          if(!StringUtils.isEmpty(stagingTable)) {
            LOG.info("Staging table for branch " + branchId + " from user: " + stagingTable);
            if (!isEmpty(conn, stagingTable)) {
              LOG.error("Staging table " + stagingTable + " is not empty. Failing.");
              throw new IllegalArgumentException("Staging table " + stagingTable + " should be empty.");
            }

            stagingTablesForBranch.add(stagingTable);
            passedStagingTables.add(stagingTable);
            continue;
          }

          LOG.info("Staging table has not been passed from user for branch " + branchId + ". Creating.");
          stagingTable = createStagingTable(conn, state, branches, branchId);
          stagingTablesForBranch.add(stagingTable);
          createdStagingTables.add(stagingTable);
          LOG.info("Staging table " + stagingTable + " has been created for branchId " + branchId);
        }
      }

      //Update work unit states
      for (WorkUnit wu : workUnits) {
        for (int branchId = 0; branchId < branches; branchId++) {
          Queue<String> stagingTablesForBranch = Objects.requireNonNull(stagingTables.get(branchId));
          String stagingTableKey = ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_TABLE, branches, 1);
          String stagingTable = stagingTablesForBranch.remove();
          LOG.info("Update workunit state " + stagingTableKey + " , " + stagingTable);
          wu.setProp(stagingTableKey, stagingTable);
        }
      }

//      for (int branchId = 0; branchId < branches; branchId++) {
//        final String publishTable = getProp(state, ConfigurationKeys.JDBC_PUBLISHER_FINAL_TABLE_NAME, branches, branchId);
//        if(isSkipStaging
//          && getPropAsBoolean(state, ConfigurationKeys.JDBC_PUBLISHER_REPLACE_FINAL_TABLE, branches, branchId)) {
//          LOG.info("User chose to replace final table " + publishTable + " on branch " + branchId);
//          truncate(conn, publishTable);
//        }
//
//        for (WorkUnit wu : workUnits) {
//          if(isSkipStaging) {
//            String stagingTableKey = ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_TABLE, branches, 1);
//            LOG.info("Skipping staging table for branch " + branchId + ". Update workunit state " + stagingTableKey + " , " + publishTable);
//            wu.setProp(stagingTableKey, publishTable);
//            continue;
//          }
//
//          String stagingTableKey = ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_TABLE, branches, 1);
//          String stagingTable = state.getProp(stagingTableKey);
//          if(!StringUtils.isEmpty(stagingTable)) {
//            LOG.info("Staging table for branch " + branchId + " from user: " + stagingTable);
//            passedStagingTables.add(stagingTable);
//          } else {
//            LOG.info("Staging table has not been passed from user for branch " + branchId + ". Creating.");
//            stagingTable = createStagingTable(conn, state, branches, branchId);
//            createdStagingTables.add(stagingTable);
//            LOG.info("Staging table " + stagingTable + " has been created for branchId " + branchId);
//          }
//
//          if(!isEmpty(conn, stagingTable)) {
//            LOG.error("Staging table " + stagingTable + " is not empty. Failing.");
//            throw new IllegalArgumentException("Staging table " + stagingTable + " should be empty.");
//          }
//          LOG.info("Update workunit state " + stagingTableKey + " , " + stagingTable);
//          wu.setProp(stagingTableKey, stagingTable);
//        }
//      }
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
  private void validateInput(SourceState state) {
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

  private void truncate(Connection conn, String publishTable) throws SQLException {
    final String TRUNCATE_TABLE_FORMAT = "TRUNCATE TABLE %s";
    String sql = String.format(TRUNCATE_TABLE_FORMAT, publishTable);
    LOG.info("Truncating table " + publishTable + " , SQL: " + sql);
    conn.prepareStatement(sql).execute();
  }

  private void dropTable(Connection conn, String table) throws SQLException {
    LOG.info("Dropping table " + table);
    final String DROP_TABLE_SQL_FORMAT = "DROP TABLE %s";
    String sql = String.format(DROP_TABLE_SQL_FORMAT, table);

    PreparedStatement pstmt = conn.prepareStatement(sql);
    LOG.info("Executing SQL " + pstmt);
    pstmt.execute();
  }

  private boolean isEmpty(Connection conn, String table) throws SQLException {
    final String SELECT_SQL_FORMAT = "SELECT COUNT(*) FROM %s";
    String sql = String.format(SELECT_SQL_FORMAT, table);
    PreparedStatement pstmt = conn.prepareStatement(sql);
    ResultSet resultSet = pstmt.executeQuery();
    if(!resultSet.first()) {
      throw new RuntimeException("Should have received at least one row from SQL " + pstmt);
    }
    return 0 == resultSet.getInt(1);
  }

  private Connection createConnection(SourceState state) throws SQLException {
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

  private String createStagingTable(Connection conn, SourceState state, int branches, int branchId) throws SQLException {
    final String STAGING_TABLE_FORMAT = "stage_%s_%d";
    final String CREATE_TABLE_SQL_FORMAT = "CREATE TABLE %s SELECT * FROM %s WHERE 1=2";
    final int NAMING_STAGING_TABLE_COUNT = 10;
    final Random r = new Random();
    String destTableKey = ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.JDBC_PUBLISHER_FINAL_TABLE_NAME, branches, branchId);
    String destinationTable = state.getProp(destTableKey);
    if(StringUtils.isEmpty(destinationTable)) {
      throw new IllegalArgumentException(ConfigurationKeys.JDBC_PUBLISHER_FINAL_TABLE_NAME + " is required for " + this.getClass().getSimpleName() + " for branch " + branchId);
    }

    String stagingTable = null;
    for (int i = 0; i < NAMING_STAGING_TABLE_COUNT; i++) {
      String tmp = String.format(STAGING_TABLE_FORMAT, destinationTable, System.nanoTime());
      LOG.info("Check if staging table " + tmp + " exists.");
      ResultSet res = conn.getMetaData().getTables(null, null, tmp, new String[] {"TABLE"});
      if (!res.next()) {
        LOG.info("Staging table " + tmp + " does not exist. Creating.");
        String sql = String.format(CREATE_TABLE_SQL_FORMAT, tmp, destinationTable);
        PreparedStatement createPstmt = conn.prepareStatement(sql);
        LOG.info("Executing SQL " + createPstmt);
        try {
          createPstmt.execute();
          LOG.info("Test if staging table can be dropped. Test by dropping and Creating staging table.");
          dropTable(conn, tmp);
          LOG.info("Creating again by executing SQL " + createPstmt);
          createPstmt.execute();
          stagingTable = tmp;
          break;
        } catch (SQLException e) {
          LOG.warn("Failed to create table using SQL " + createPstmt, e);
        }
      } else {
        LOG.info("Staging table " + tmp + " exists.");
      }
      try {
        TimeUnit.MILLISECONDS.sleep(r.nextInt(1000));
      } catch (InterruptedException e) {
        LOG.info("Sleep has been interrupted.", e);
      }
    }

    if (!StringUtils.isEmpty(stagingTable)) {
     return stagingTable;
    }
    throw new RuntimeException("Failed to create staging table");
  }

  @Override
  public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state) throws IOException {
    return avroFileSource.getExtractor(state);
  }

  @Override
  public void shutdown(SourceState state) {
    avroFileSource.shutdown(state);

    if(!createdStagingTables.isEmpty()) {
      Set<String> tablesToDrop = Sets.newHashSet(createdStagingTables);
      Throwable t = null;
      try (Connection conn = createConnection(state)) {
        LOG.info("Dropping staging table(s) " + createdStagingTables);
        Iterator<String> it = tablesToDrop.iterator();
        while (it.hasNext()) {
          String createdStagingTable = it.next();
          try {
            dropTable(conn, createdStagingTable);
            it.remove();
          } catch (Exception e) {
            LOG.error("Failed to drop staging table " + createdStagingTable);
            t = e;
          }
        }
      } catch (SQLException e) {
        throw new RuntimeException("Failed to dropping table(s) " + tablesToDrop, e);
      }
      if (t != null) {
        throw new RuntimeException("Failed to dropping table(s) " + tablesToDrop, t);
      }

    }

    if(!passedStagingTables.isEmpty()) {
      Set<String> tablesToTruncate = Sets.newHashSet(passedStagingTables);
      LOG.info("Truncating staging table(s) " + passedStagingTables);
      Throwable t = null;
      try (Connection conn = createConnection(state)) {
        Iterator<String> it = tablesToTruncate.iterator();
        while (it.hasNext()) {
          String passedStagingTable = it.next();
          try {
            truncate(conn, passedStagingTable);
            it.remove();
          } catch (Exception e) {
            LOG.error("Failed to truncate staging table " + passedStagingTable);
            t = e;
          }
        }
      } catch (SQLException e) {
        throw new RuntimeException("Failed to truncating table(s) " + tablesToTruncate, e);
      }
      if (t != null) {
        throw new RuntimeException("Failed to truncating table(s) " + tablesToTruncate, t);
      }
    }
  }

  private String getProp(SourceState state, String key, int branches, int branchId) {
    String forkedKey = ForkOperatorUtils.getPropertyNameForBranch(key, branches, branchId);
    return state.getProp(forkedKey);
  }

  private boolean getPropAsBoolean(SourceState state, String key, int branches, int branchId) {
    return Boolean.parseBoolean(getProp(state, key, branches, branchId));
  }
}
