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

package org.apache.gobblin.runtime.api;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Timestamp;
import javax.sql.DataSource;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.MysqlDataSourceFactory;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.util.ConfigUtils;


/**
 * MySQL based implementation of the {@link MultiActiveLeaseArbiter} which uses a MySQL store to resolve ownership of
 * a flow event amongst multiple competing participants. A MySQL table is used to store flow identifying information as
 * well as the flow action associated with it. It uses two additional values of the `event_timestamp` and
 * `lease_acquisition_timestamp` to indicate an active lease, expired lease, and state of no longer leasing. The table
 * schema is as follows:
 * [flow_group | flow_name | flow_execution_id | flow_action | event_timestamp | lease_acquisition_timestamp]
 * (----------------------primary key------------------------)
 * We also maintain another table in the database with two constants that allow us to coordinate between participants and
 * ensure they are using the same values to base their coordination off of.
 * [epsilon | linger]
 * `epsilon` - time within we consider to timestamps to be the same, to account for between-host clock drift
 * `linger` - minimum time to occur before another host may attempt a lease on a flow event. It should be much greater
 *            than epsilon and encapsulate executor communication latency including retry attempts
 *
 * The `event_timestamp` is the time of the flow_action event request.
 * --- Note ---
 * We only use the participant's local event_timestamp internally to identify the particular flow_action event, but
 * after interacting with the database utilize the CURRENT_TIMESTAMP of the database to insert or keep
 * track of our event. This is to avoid any discrepancies due to clock drift between participants as well as
 * variation in local time and database time for future comparisons.
 * ---Event consolidation---
 * Note that for the sake of simplification, we only allow one event associated with a particular flow's flow_action
 * (ie: only one LAUNCH for example of flow FOO, but there can be a LAUNCH, KILL, & RESUME for flow FOO at once) during
 * the time it takes to execute the flow action. In most cases, the execution time should be so negligible that this
 * event consolidation of duplicate flow action requests is not noticed and even during executor downtime this behavior
 * is acceptable as the user generally expects a timely execution of the most recent request rather than one execution
 * per request.
 *
 * The `lease_acquisition_timestamp` is the time a host acquired ownership of this flow action, and it is valid for
 * `linger` period of time after which it expires and any host can re-attempt ownership. In most cases, the original
 * host should actually complete its work while having the lease and then mark the flow action as NULL to indicate no
 * further leasing should be done for the event.
 */
@Slf4j
public class MysqlMultiActiveLeaseArbiter implements MultiActiveLeaseArbiter {
  /** `j.u.Function` variant for an operation that may @throw IOException or SQLException: preserves method signature checked exceptions */
  @FunctionalInterface
  protected interface CheckedFunction<T, R> {
    R apply(T t) throws IOException, SQLException;
  }

  protected final DataSource dataSource;
  private final String leaseArbiterTableName;
  private final String constantsTableName;
  private final int epsilon;
  private final int linger;
  private String thisTableGetInfoStatement;
  private String thisTableSelectAfterInsertStatement;
  private String thisTableAcquireLeaseIfMatchingAllStatement;
  private String thisTableAcquireLeaseIfFinishedStatement;

  // TODO: define retention on this table
  private static final String CREATE_LEASE_ARBITER_TABLE_STATEMENT = "CREATE TABLE IF NOT EXISTS %s ("
      + "flow_group varchar(" + ServiceConfigKeys.MAX_FLOW_GROUP_LENGTH + ") NOT NULL, flow_name varchar("
      + ServiceConfigKeys.MAX_FLOW_GROUP_LENGTH + ") NOT NULL, " + "flow_execution_id varchar("
      + ServiceConfigKeys.MAX_FLOW_EXECUTION_ID_LENGTH + ") NOT NULL, flow_action varchar(100) NOT NULL, "
      + "event_timestamp TIMESTAMP, "
      + "lease_acquisition_timestamp TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP, "
      + "PRIMARY KEY (flow_group,flow_name,flow_execution_id,flow_action))";
  private static final String CREATE_CONSTANTS_TABLE_STATEMENT = "CREATE TABLE IF NOT EXISTS %s "
      + "(primary_key INT, epsilon INT, linger INT, PRIMARY KEY (primary_key))";
  // Only insert epsilon and linger values from config if this table does not contain a pre-existing values already.
  private static final String UPSERT_CONSTANTS_TABLE_STATEMENT = "INSERT INTO %s (primary_key, epsilon, linger) "
      + "VALUES(1, ?, ?) ON DUPLICATE KEY UPDATE epsilon=VALUES(epsilon), linger=VALUES(linger)";
  protected static final String WHERE_CLAUSE_TO_MATCH_KEY = "WHERE flow_group=? AND flow_name=? AND flow_execution_id=?"
      + " AND flow_action=?";
  protected static final String WHERE_CLAUSE_TO_MATCH_ROW = WHERE_CLAUSE_TO_MATCH_KEY
      + " AND event_timestamp=? AND lease_acquisition_timestamp=?";
  protected static final String SELECT_AFTER_INSERT_STATEMENT = "SELECT event_timestamp, lease_acquisition_timestamp, "
    + "linger FROM %s, %s " + WHERE_CLAUSE_TO_MATCH_KEY;
  // Does a cross join between the two tables to have epsilon and linger values available. Returns the following values:
  // event_timestamp, lease_acquisition_timestamp, isWithinEpsilon (boolean if current time in db is within epsilon of
  // event_timestamp), leaseValidityStatus (1 if lease has not expired, 2 if expired, 3 if column is NULL or no longer
  // leasing)
  protected static final String GET_EVENT_INFO_STATEMENT = "SELECT event_timestamp, lease_acquisition_timestamp, "
      + "TIMESTAMPDIFF(microsecond, event_timestamp, CURRENT_TIMESTAMP) / 1000 <= epsilon as is_within_epsilon, CASE "
      + "WHEN CURRENT_TIMESTAMP < DATE_ADD(lease_acquisition_timestamp, INTERVAL linger*1000 MICROSECOND) then 1 "
      + "WHEN CURRENT_TIMESTAMP >= DATE_ADD(lease_acquisition_timestamp, INTERVAL linger*1000 MICROSECOND) then 2 "
      + "ELSE 3 END as lease_validity_status, linger, CURRENT_TIMESTAMP FROM %s, %s " + WHERE_CLAUSE_TO_MATCH_KEY;
  // Insert or update row to acquire lease if values have not changed since the previous read
  // Need to define three separate statements to handle cases where row does not exist or has null values to check
  protected static final String ACQUIRE_LEASE_IF_NEW_ROW_STATEMENT = "INSERT INTO %s (flow_group, flow_name, "
      + "flow_execution_id, flow_action, event_timestamp, lease_acquisition_timestamp) VALUES(?, ?, ?, ?, "
      + "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)";
  protected static final String CONDITIONALLY_ACQUIRE_LEASE_IF_FINISHED_LEASING_STATEMENT = "UPDATE %s "
      + "SET event_timestamp=CURRENT_TIMESTAMP, lease_acquisition_timestamp=CURRENT_TIMESTAMP "
      + WHERE_CLAUSE_TO_MATCH_KEY + " AND event_timestamp=? AND lease_acquisition_timestamp is NULL";
  protected static final String CONDITIONALLY_ACQUIRE_LEASE_IF_MATCHING_ALL_COLS_STATEMENT = "UPDATE %s "
      + "SET event_timestamp=CURRENT_TIMESTAMP, lease_acquisition_timestamp=CURRENT_TIMESTAMP "
      + WHERE_CLAUSE_TO_MATCH_ROW;
  // Complete lease acquisition if values have not changed since lease was acquired
  protected static final String CONDITIONALLY_COMPLETE_LEASE_STATEMENT = "UPDATE %s SET "
      + "lease_acquisition_timestamp = NULL " + WHERE_CLAUSE_TO_MATCH_ROW;

  @Inject
  public MysqlMultiActiveLeaseArbiter(Config config) throws IOException {
    if (config.hasPath(ConfigurationKeys.MYSQL_LEASE_ARBITER_PREFIX)) {
      config = config.getConfig(ConfigurationKeys.MYSQL_LEASE_ARBITER_PREFIX).withFallback(config);
    } else {
      throw new IOException(String.format("Please specify the config for MysqlMultiActiveLeaseArbiter using prefix %s "
          + "before all properties", ConfigurationKeys.MYSQL_LEASE_ARBITER_PREFIX));
    }

    this.leaseArbiterTableName = ConfigUtils.getString(config, ConfigurationKeys.SCHEDULER_LEASE_DETERMINATION_STORE_DB_TABLE_KEY,
        ConfigurationKeys.DEFAULT_SCHEDULER_LEASE_DETERMINATION_STORE_DB_TABLE);
    this.constantsTableName = ConfigUtils.getString(config, ConfigurationKeys.MULTI_ACTIVE_SCHEDULER_CONSTANTS_DB_TABLE_KEY,
        ConfigurationKeys.DEFAULT_MULTI_ACTIVE_SCHEDULER_CONSTANTS_DB_TABLE);
    this.epsilon = ConfigUtils.getInt(config, ConfigurationKeys.SCHEDULER_EVENT_EPSILON_MILLIS_KEY,
        ConfigurationKeys.DEFAULT_SCHEDULER_EVENT_EPSILON_MILLIS);
    this.linger = ConfigUtils.getInt(config, ConfigurationKeys.SCHEDULER_EVENT_LINGER_MILLIS_KEY,
        ConfigurationKeys.DEFAULT_SCHEDULER_EVENT_LINGER_MILLIS);
    this.thisTableGetInfoStatement = String.format(GET_EVENT_INFO_STATEMENT, this.leaseArbiterTableName,
        this.constantsTableName);
    this.thisTableSelectAfterInsertStatement = String.format(SELECT_AFTER_INSERT_STATEMENT, this.leaseArbiterTableName,
        this.constantsTableName);
    this.thisTableAcquireLeaseIfMatchingAllStatement =
        String.format(CONDITIONALLY_ACQUIRE_LEASE_IF_MATCHING_ALL_COLS_STATEMENT, this.leaseArbiterTableName);
    this.thisTableAcquireLeaseIfFinishedStatement =
        String.format(CONDITIONALLY_ACQUIRE_LEASE_IF_FINISHED_LEASING_STATEMENT, this.leaseArbiterTableName);
    this.dataSource = MysqlDataSourceFactory.get(config, SharedResourcesBrokerFactory.getImplicitBroker());
    String createArbiterStatement = String.format(
        CREATE_LEASE_ARBITER_TABLE_STATEMENT, leaseArbiterTableName);
    try (Connection connection = dataSource.getConnection();
        PreparedStatement createStatement = connection.prepareStatement(createArbiterStatement)) {
      createStatement.executeUpdate();
      connection.commit();
    } catch (SQLException e) {
      throw new IOException("Table creation failure for " + leaseArbiterTableName, e);
    }
    initializeConstantsTable();

    log.info("MysqlMultiActiveLeaseArbiter initialized");
  }

  // Initialize Constants table if needed and insert row into it if one does not exist
  private void initializeConstantsTable() throws IOException {
    String createConstantsStatement = String.format(CREATE_CONSTANTS_TABLE_STATEMENT, this.constantsTableName);
    withPreparedStatement(createConstantsStatement, createStatement -> createStatement.executeUpdate(), true);

    String insertConstantsStatement = String.format(UPSERT_CONSTANTS_TABLE_STATEMENT, this.constantsTableName);
    withPreparedStatement(insertConstantsStatement, insertStatement -> {
      int i = 0;
      insertStatement.setInt(++i, epsilon);
      insertStatement.setInt(++i, linger);
      return insertStatement.executeUpdate();
    }, true);
  }

  @Override
  public LeaseAttemptStatus tryAcquireLease(DagActionStore.DagAction flowAction, long eventTimeMillis)
      throws IOException {
    log.info("Multi-active scheduler about to handle trigger event: [{}, triggerEventTimestamp: {}]", flowAction,
        eventTimeMillis);
    // Query lease arbiter table about this flow action
    Optional<GetEventInfoResult> getResult = getExistingEventInfo(flowAction);

    try {
      if (!getResult.isPresent()) {
        log.debug("tryAcquireLease for [{}, eventTimestamp: {}] - CASE 1: no existing row for this flow action, then go"
                + " ahead and insert", flowAction, eventTimeMillis);
        int numRowsUpdated = attemptLeaseIfNewRow(flowAction);
       return evaluateStatusAfterLeaseAttempt(numRowsUpdated, flowAction, Optional.absent());
      }

      // Extract values from result set
      Timestamp dbEventTimestamp = getResult.get().getDbEventTimestamp();
      Timestamp dbLeaseAcquisitionTimestamp = getResult.get().getDbLeaseAcquisitionTimestamp();
      boolean isWithinEpsilon = getResult.get().isWithinEpsilon();
      int leaseValidityStatus = getResult.get().getLeaseValidityStatus();
      // Used to calculate minimum amount of time until a participant should check whether a lease expired
      int dbLinger = getResult.get().getDbLinger();
      Timestamp dbCurrentTimestamp = getResult.get().getDbCurrentTimestamp();

      log.info("Multi-active arbiter replacing local trigger event timestamp [{}, triggerEventTimestamp: {}] with "
          + "database eventTimestamp {}", flowAction, eventTimeMillis, dbCurrentTimestamp.getTime());

      // Lease is valid
      if (leaseValidityStatus == 1) {
        if (isWithinEpsilon) {
          log.debug("tryAcquireLease for [{}, eventTimestamp: {}] - CASE 2: Same event, lease is valid", flowAction,
              dbCurrentTimestamp.getTime());
          // Utilize db timestamp for reminder
          return new LeasedToAnotherStatus(flowAction, dbEventTimestamp.getTime(),
              dbLeaseAcquisitionTimestamp.getTime() + dbLinger - dbCurrentTimestamp.getTime());
        }
        log.debug("tryAcquireLease for [{}, eventTimestamp: {}] - CASE 3: Distinct event, lease is valid", flowAction,
            dbCurrentTimestamp.getTime());
        // Utilize db lease acquisition timestamp for wait time
        return new LeasedToAnotherStatus(flowAction, dbCurrentTimestamp.getTime(),
            dbLeaseAcquisitionTimestamp.getTime() + dbLinger  - dbCurrentTimestamp.getTime());
      }
      else if (leaseValidityStatus == 2) {
        log.debug("tryAcquireLease for [{}, eventTimestamp: {}] - CASE 4: Lease is out of date (regardless of whether "
            + "same or distinct event)", flowAction, dbCurrentTimestamp.getTime());
        if (isWithinEpsilon) {
          log.warn("Lease should not be out of date for the same trigger event since epsilon << linger for flowAction"
                  + " {}, db eventTimestamp {}, db leaseAcquisitionTimestamp {}, linger {}", flowAction,
              dbEventTimestamp, dbLeaseAcquisitionTimestamp, dbLinger);
        }
        // Use our event to acquire lease, check for previous db eventTimestamp and leaseAcquisitionTimestamp
        int numRowsUpdated = attemptLeaseIfExistingRow(thisTableAcquireLeaseIfMatchingAllStatement, flowAction,
            true,true, dbEventTimestamp, dbLeaseAcquisitionTimestamp);
        return evaluateStatusAfterLeaseAttempt(numRowsUpdated, flowAction, Optional.of(dbCurrentTimestamp));
      } // No longer leasing this event
        if (isWithinEpsilon) {
          log.debug("tryAcquireLease for [{}, eventTimestamp: {}] - CASE 5: Same event, no longer leasing event in db: "
              + "terminate", flowAction, dbCurrentTimestamp.getTime());
          return new NoLongerLeasingStatus();
        }
        log.debug("tryAcquireLease for [{}, eventTimestamp: {}] - CASE 6: Distinct event, no longer leasing event in "
            + "db", flowAction, dbCurrentTimestamp.getTime());
        // Use our event to acquire lease, check for previous db eventTimestamp and NULL leaseAcquisitionTimestamp
        int numRowsUpdated = attemptLeaseIfExistingRow(thisTableAcquireLeaseIfFinishedStatement, flowAction,
            true, false, dbEventTimestamp, null);
        return evaluateStatusAfterLeaseAttempt(numRowsUpdated, flowAction, Optional.of(dbCurrentTimestamp));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Checks leaseArbiterTable for an existing entry for this flow action and event time
   */
  protected Optional<GetEventInfoResult> getExistingEventInfo(DagActionStore.DagAction flowAction) throws IOException {
    return withPreparedStatement(thisTableGetInfoStatement,
        getInfoStatement -> {
          int i = 0;
          getInfoStatement.setString(++i, flowAction.getFlowGroup());
          getInfoStatement.setString(++i, flowAction.getFlowName());
          getInfoStatement.setString(++i, flowAction.getFlowExecutionId());
          getInfoStatement.setString(++i, flowAction.getFlowActionType().toString());
          ResultSet resultSet = getInfoStatement.executeQuery();
          try {
            if (!resultSet.next()) {
              return Optional.<GetEventInfoResult>absent();
            }
            return Optional.of(createGetInfoResult(resultSet));
          } finally {
            if (resultSet !=  null) {
              resultSet.close();
            }
          }
        }, true);
  }

  protected GetEventInfoResult createGetInfoResult(ResultSet resultSet) throws IOException {
    try {
      // Extract values from result set
      Timestamp dbEventTimestamp = resultSet.getTimestamp("event_timestamp");
      Timestamp dbLeaseAcquisitionTimestamp = resultSet.getTimestamp("lease_acquisition_timestamp");
      boolean withinEpsilon = resultSet.getBoolean("is_within_epsilon");
      int leaseValidityStatus = resultSet.getInt("lease_validity_status");
      int dbLinger = resultSet.getInt("linger");
      Timestamp dbCurrentTimestamp = resultSet.getTimestamp("CURRENT_TIMESTAMP");
      return new GetEventInfoResult(dbEventTimestamp, dbLeaseAcquisitionTimestamp, withinEpsilon, leaseValidityStatus,
          dbLinger, dbCurrentTimestamp);
    } catch (SQLException e) {
      throw new IOException(e);
    } finally {
      if (resultSet != null) {
        try {
          resultSet.close();
        } catch (SQLException e) {
          throw new IOException(e);
        }
      }
    }
  }

  /**
   * Called by participant to try to acquire lease for a flow action that does not have an attempt in progress or in
   * near past for it.
   * @return int corresponding to number of rows updated by INSERT statement to acquire lease
   */
  protected int attemptLeaseIfNewRow(DagActionStore.DagAction flowAction) throws IOException {
    String formattedAcquireLeaseNewRowStatement =
        String.format(ACQUIRE_LEASE_IF_NEW_ROW_STATEMENT, this.leaseArbiterTableName);
    return withPreparedStatement(formattedAcquireLeaseNewRowStatement,
        insertStatement -> {
          completeInsertPreparedStatement(insertStatement, flowAction);
          try {
            return insertStatement.executeUpdate();
          } catch (SQLIntegrityConstraintViolationException e) {
            if (!e.getMessage().contains("Duplicate entry")) {
              throw e;
            }
            return 0;
          }
        }, true);
  }

  /**
   * Called by participant to try to acquire lease for a flow action that has an existing, completed, or expired lease
   * attempt for the flow action in the table.
   * @return int corresponding to number of rows updated by INSERT statement to acquire lease
   */
  protected int attemptLeaseIfExistingRow(String acquireLeaseStatement, DagActionStore.DagAction flowAction,
      boolean needEventTimeCheck, boolean needLeaseAcquisition, Timestamp dbEventTimestamp,
      Timestamp dbLeaseAcquisitionTimestamp) throws IOException {
    return withPreparedStatement(acquireLeaseStatement,
        insertStatement -> {
          completeUpdatePreparedStatement(insertStatement, flowAction, needEventTimeCheck,
              needLeaseAcquisition, dbEventTimestamp, dbLeaseAcquisitionTimestamp);
          return insertStatement.executeUpdate();
        }, true);
  }

  /**
   * Checks leaseArbiter table for a row corresponding to this flow action to determine if the lease acquisition attempt
   * was successful or not.
   */
  protected SelectInfoResult getRowInfo(DagActionStore.DagAction flowAction) throws IOException {
    return withPreparedStatement(thisTableSelectAfterInsertStatement,
        selectStatement -> {
          completeWhereClauseMatchingKeyPreparedStatement(selectStatement, flowAction);
          ResultSet resultSet = selectStatement.executeQuery();
          try {
            return createSelectInfoResult(resultSet);
          } finally {
            if (resultSet !=  null) {
              resultSet.close();
            }
          }
        }, true);
  }
  protected static SelectInfoResult createSelectInfoResult(ResultSet resultSet) throws IOException {
      try {
        if (!resultSet.next()) {
          throw new IOException("Expected num rows and lease_acquisition_timestamp returned from query but received nothing, so "
              + "providing empty result to lease evaluation code");
        }
        long eventTimeMillis = resultSet.getTimestamp(1).getTime();
        long leaseAcquisitionTimeMillis = resultSet.getTimestamp(2).getTime();
        int dbLinger = resultSet.getInt(3);
        return new SelectInfoResult(eventTimeMillis, leaseAcquisitionTimeMillis, dbLinger);
      } catch (SQLException e) {
        throw new IOException(e);
      } finally {
        if (resultSet != null) {
          try {
            resultSet.close();
          } catch (SQLException e) {
            throw new IOException(e);
          }
        }
      }
  }

  /**
   * Parse result of attempted insert/update to obtain a lease for a
   * {@link org.apache.gobblin.runtime.api.DagActionStore.DagAction} event by selecting values corresponding to that
   * event from the table to return the corresponding status based on successful insert/update or not.
   * @throws SQLException
   * @throws IOException
   */
  protected LeaseAttemptStatus evaluateStatusAfterLeaseAttempt(int numRowsUpdated,
      DagActionStore.DagAction flowAction, Optional<Timestamp> dbCurrentTimestamp)
      throws SQLException, IOException {
    // Fetch values in row after attempted insert
    SelectInfoResult selectInfoResult = getRowInfo(flowAction);
    if (numRowsUpdated == 1) {
      log.debug("Obtained lease for [{}, eventTimestamp: {}] successfully!", flowAction,
          selectInfoResult.eventTimeMillis);
      return new LeaseObtainedStatus(flowAction, selectInfoResult.eventTimeMillis,
          selectInfoResult.getLeaseAcquisitionTimeMillis());
    }
    log.debug("Another participant acquired lease in between for [{}, eventTimestamp: {}] - num rows updated: ",
        flowAction, selectInfoResult.eventTimeMillis, numRowsUpdated);
    // Another participant acquired lease in between
    return new LeasedToAnotherStatus(flowAction, selectInfoResult.getEventTimeMillis(),
        selectInfoResult.getLeaseAcquisitionTimeMillis() + selectInfoResult.getDbLinger()
            - (dbCurrentTimestamp.isPresent() ? dbCurrentTimestamp.get().getTime() : System.currentTimeMillis()));
  }

  /**
   * Complete the INSERT statement for a new flow action lease where the flow action is not present in the table
   * @param statement
   * @param flowAction
   * @throws SQLException
   */
  protected static void completeInsertPreparedStatement(PreparedStatement statement,
      DagActionStore.DagAction flowAction) throws SQLException {
    int i = 0;
    // Values to set in new row
    statement.setString(++i, flowAction.getFlowGroup());
    statement.setString(++i, flowAction.getFlowName());
    statement.setString(++i, flowAction.getFlowExecutionId());
    statement.setString(++i, flowAction.getFlowActionType().toString());
  }

  /**
   * Complete the WHERE clause to match a flow action in a select statement
   * @param statement
   * @param flowAction
   * @throws SQLException
   */
  protected static void completeWhereClauseMatchingKeyPreparedStatement(PreparedStatement statement,
      DagActionStore.DagAction flowAction) throws SQLException {
    int i = 0;
    statement.setString(++i, flowAction.getFlowGroup());
    statement.setString(++i, flowAction.getFlowName());
    statement.setString(++i, flowAction.getFlowExecutionId());
    statement.setString(++i, flowAction.getFlowActionType().toString());
  }

  /**
   * Complete the UPDATE prepared statements for a flow action that already exists in the table that needs to be
   * updated.
   * @param statement
   * @param flowAction
   * @param needEventTimeCheck true if need to compare `originalEventTimestamp` with db event_timestamp
   * @param needLeaseAcquisitionTimeCheck true if need to compare `originalLeaseAcquisitionTimestamp` with db one
   * @param originalEventTimestamp value to compare to db one, null if not needed
   * @param originalLeaseAcquisitionTimestamp value to compare to db one, null if not needed
   * @throws SQLException
   */
  protected static void completeUpdatePreparedStatement(PreparedStatement statement,
      DagActionStore.DagAction flowAction, boolean needEventTimeCheck, boolean needLeaseAcquisitionTimeCheck,
      Timestamp originalEventTimestamp, Timestamp originalLeaseAcquisitionTimestamp) throws SQLException {
    int i = 0;
    // Values to check if existing row matches previous read
    statement.setString(++i, flowAction.getFlowGroup());
    statement.setString(++i, flowAction.getFlowName());
    statement.setString(++i, flowAction.getFlowExecutionId());
    statement.setString(++i, flowAction.getFlowActionType().toString());
    // Values that may be needed depending on the insert statement
    if (needEventTimeCheck) {
      statement.setTimestamp(++i, originalEventTimestamp);
    }
    if (needLeaseAcquisitionTimeCheck) {
      statement.setTimestamp(++i, originalLeaseAcquisitionTimestamp);
    }
  }

  @Override
  public boolean recordLeaseSuccess(LeaseObtainedStatus status)
      throws IOException {
    DagActionStore.DagAction flowAction = status.getFlowAction();
    String flowGroup = flowAction.getFlowGroup();
    String flowName = flowAction.getFlowName();
    String flowExecutionId = flowAction.getFlowExecutionId();
    DagActionStore.FlowActionType flowActionType = flowAction.getFlowActionType();
    return withPreparedStatement(String.format(CONDITIONALLY_COMPLETE_LEASE_STATEMENT, leaseArbiterTableName),
        updateStatement -> {
          int i = 0;
          updateStatement.setString(++i, flowGroup);
          updateStatement.setString(++i, flowName);
          updateStatement.setString(++i, flowExecutionId);
          updateStatement.setString(++i, flowActionType.toString());
          updateStatement.setTimestamp(++i, new Timestamp(status.getEventTimestamp()));
          updateStatement.setTimestamp(++i, new Timestamp(status.getLeaseAcquisitionTimestamp()));
          int numRowsUpdated = updateStatement.executeUpdate();
          if (numRowsUpdated == 0) {
            log.info("Multi-active lease arbiter lease attempt: [{}, eventTimestamp: {}] - FAILED to complete because "
                + "lease expired or event cleaned up before host completed required actions", flowAction,
                status.getEventTimestamp());
            return false;
          }
          if( numRowsUpdated == 1) {
            log.info("Multi-active lease arbiter lease attempt: [{}, eventTimestamp: {}] - COMPLETED, no longer leasing"
                    + " this event after this.", flowAction, status.getEventTimestamp());
            return true;
          };
          throw new IOException(String.format("Attempt to complete lease use: [%s, eventTimestamp: %s] - updated more "
                  + "rows than expected", flowAction, status.getEventTimestamp()));
        }, true);
  }

  /** Abstracts recurring pattern around resource management and exception re-mapping. */
  protected <T> T withPreparedStatement(String sql, CheckedFunction<PreparedStatement, T> f, boolean shouldCommit)
      throws IOException {
    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      T result = f.apply(statement);
      if (shouldCommit) {
        connection.commit();
      }
      statement.close();
      return result;
    } catch (SQLException e) {
      log.warn("Received SQL exception that can result from invalid connection. Checking if validation query is set {} "
          + "Exception is {}", ((HikariDataSource) this.dataSource).getConnectionTestQuery(), e);
      throw new IOException(e);
    }
  }


  /**
   * DTO for arbiter's current lease state for a FlowActionEvent
  */
  @Data
  static class GetEventInfoResult {
    private final Timestamp dbEventTimestamp;
    private final Timestamp dbLeaseAcquisitionTimestamp;
    private final boolean withinEpsilon;
    private final int leaseValidityStatus;
    private final int dbLinger;
    private final Timestamp dbCurrentTimestamp;
  }

  /**
   DTO for result of SELECT query used to determine status of lease acquisition attempt
  */
  @Data
  static class SelectInfoResult {
    private final long eventTimeMillis;
    private final long leaseAcquisitionTimeMillis;
    private final int dbLinger;
  }
}
