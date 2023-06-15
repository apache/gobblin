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

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
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

  // TODO: define retention on this table
  private static final String CREATE_LEASE_ARBITER_TABLE_STATEMENT = "CREATE TABLE IF NOT EXISTS %S ("
      + "flow_group varchar(" + ServiceConfigKeys.MAX_FLOW_GROUP_LENGTH + ") NOT NULL, flow_name varchar("
      + ServiceConfigKeys.MAX_FLOW_GROUP_LENGTH + ") NOT NULL, " + "flow_execution_id varchar("
      + ServiceConfigKeys.MAX_FLOW_EXECUTION_ID_LENGTH + ") NOT NULL, flow_action varchar(100) NOT NULL, "
      + "event_timestamp TIMESTAMP, "
      + "lease_acquisition_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
      + "PRIMARY KEY (flow_group,flow_name,flow_execution_id,flow_action))";
  private static final String CREATE_CONSTANTS_TABLE_STATEMENT = "CREATE TABLE IF NOT EXISTS %s "
      + "(epsilon INT, linger INT), PRIMARY KEY (epsilon, linger); INSERT INTO %s (epsilon, linger) VALUES (?,?)";
  protected static final String WHERE_CLAUSE_TO_MATCH_KEY = "WHERE flow_group=? AND flow_name=? AND flow_execution_id=?"
      + " AND flow_action=?";
  protected static final String WHERE_CLAUSE_TO_MATCH_ROW = WHERE_CLAUSE_TO_MATCH_KEY
      + " AND event_timestamp=? AND lease_acquisition_timestamp=?";
  protected static final String SELECT_AFTER_INSERT_STATEMENT = "SELECT ROW_COUNT() AS rows_inserted_count, "
      + "lease_acquisition_timestamp, linger FROM %s, %s " + WHERE_CLAUSE_TO_MATCH_KEY;
  // Does a cross join between the two tables to have epsilon and linger values available. Returns the following values:
  // event_timestamp, lease_acquisition_timestamp, isWithinEpsilon (boolean if event_timestamp in table is within
  // epsilon), leaseValidityStatus (1 if lease has not expired, 2 if expired, 3 if column is NULL or no longer leasing)
  protected static final String GET_EVENT_INFO_STATEMENT = "SELECT event_timestamp, lease_acquisition_timestamp, "
      + "abs(event_timestamp - ?) <= epsilon as isWithinEpsilon, CASE "
      + "WHEN CURRENT_TIMESTAMP < (lease_acquisition_timestamp + linger) then 1"
      + "WHEN CURRENT_TIMESTAMP >= (lease_acquisition_timestamp + linger) then 2"
      + "ELSE 3 END as leaseValidityStatus, linger FROM %s, %s " + WHERE_CLAUSE_TO_MATCH_KEY;
  // Insert or update row to acquire lease if values have not changed since the previous read
  // Need to define three separate statements to handle cases where row does not exist or has null values to check
  protected static final String CONDITIONALLY_ACQUIRE_LEASE_IF_NEW_ROW_STATEMENT = "INSERT INTO %s "
      + "(flow_group, flow_name, flow_execution_id, flow_action, event_timestamp) VALUES (?, ?, ?, ?, ?)";
  protected static final String CONDITIONALLY_ACQUIRE_LEASE_IF_FINISHED_LEASING_STATEMENT = "UPDATE %s "
      + "SET event_timestamp=?" + WHERE_CLAUSE_TO_MATCH_KEY
      + " AND event_timestamp=? AND lease_acquisition_timestamp is NULL";
  protected static final String CONDITIONALLY_ACQUIRE_LEASE_IF_MATCHING_ALL_COLS_STATEMENT = "UPDATE %s "
      + "SET event_timestamp=?" + WHERE_CLAUSE_TO_MATCH_ROW
      + " AND event_timestamp=? AND lease_acquisition_timestamp=?";
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
    this.dataSource = MysqlDataSourceFactory.get(config, SharedResourcesBrokerFactory.getImplicitBroker());
    try (Connection connection = dataSource.getConnection();
        PreparedStatement createStatement = connection.prepareStatement(String.format(
            CREATE_LEASE_ARBITER_TABLE_STATEMENT, leaseArbiterTableName))) {
      createStatement.executeUpdate();
      connection.commit();
    } catch (SQLException e) {
      throw new IOException("Table creation failure for " + leaseArbiterTableName, e);
    }
    withPreparedStatement(String.format(CREATE_CONSTANTS_TABLE_STATEMENT, this.constantsTableName, this.constantsTableName),
        createStatement -> {
      int i = 0;
      createStatement.setInt(++i, epsilon);
      createStatement.setInt(++i, linger);
      return createStatement.executeUpdate();}, true);
  }

  @Override
  public LeaseAttemptStatus tryAcquireLease(DagActionStore.DagAction flowAction, long eventTimeMillis)
      throws IOException {
    // Check table for an existing entry for this flow action and event time
    ResultSet resultSet = withPreparedStatement(
        String.format(GET_EVENT_INFO_STATEMENT, this.leaseArbiterTableName, this.constantsTableName),
        getInfoStatement -> {
          int i = 0;
          getInfoStatement.setTimestamp(++i, new Timestamp(eventTimeMillis));
          getInfoStatement.setString(++i, flowAction.getFlowGroup());
          getInfoStatement.setString(++i, flowAction.getFlowName());
          getInfoStatement.setString(++i, flowAction.getFlowExecutionId());
          getInfoStatement.setString(++i, flowAction.getFlowActionType().toString());
          return getInfoStatement.executeQuery();
        }, true);

    String formattedSelectAfterInsertStatement =
        String.format(SELECT_AFTER_INSERT_STATEMENT, this.leaseArbiterTableName, this.constantsTableName);
    try {
      // CASE 1: If no existing row for this flow action, then go ahead and insert
      if (!resultSet.next()) {
        String formattedAcquireLeaseNewRowStatement =
            String.format(CONDITIONALLY_ACQUIRE_LEASE_IF_NEW_ROW_STATEMENT, this.leaseArbiterTableName);
        ResultSet rs = withPreparedStatement(
            formattedAcquireLeaseNewRowStatement + "; " + formattedSelectAfterInsertStatement,
            insertStatement -> {
              completeInsertPreparedStatement(insertStatement, flowAction, eventTimeMillis);
              return insertStatement.executeQuery();
            }, true);
       return handleResultFromAttemptedLeaseObtainment(rs, flowAction, eventTimeMillis);
      }

      // Extract values from result set
      Timestamp dbEventTimestamp = resultSet.getTimestamp("event_timestamp");
      Timestamp dbLeaseAcquisitionTimestamp = resultSet.getTimestamp("lease_acquisition_timestamp");
      boolean isWithinEpsilon = resultSet.getBoolean("isWithinEpsilon");
      int leaseValidityStatus = resultSet.getInt("leaseValidityStatus");
      int dbLinger = resultSet.getInt("linger");

      // CASE 2: If our event timestamp is older than the last event in db, then skip this trigger
      if (eventTimeMillis < dbEventTimestamp.getTime()) {
        return new NoLongerLeasingStatus();
      }
      // Lease is valid
      if (leaseValidityStatus == 1) {
        // CASE 3: Same event, lease is valid
        if (isWithinEpsilon) {
          // Utilize db timestamp for reminder
          return new LeasedToAnotherStatus(flowAction, dbEventTimestamp.getTime(),
              dbLeaseAcquisitionTimestamp.getTime() + dbLinger - System.currentTimeMillis());
        }
        // CASE 4: Distinct event, lease is valid
        // Utilize db timestamp for wait time, but be reminded of own event timestamp
        return new LeasedToAnotherStatus(flowAction, eventTimeMillis,
            dbLeaseAcquisitionTimestamp.getTime() + dbLinger  - System.currentTimeMillis());
      }
      // CASE 5: Lease is out of date (regardless of whether same or distinct event)
      else if (leaseValidityStatus == 2) {
        if (isWithinEpsilon) {
          log.warn("Lease should not be out of date for the same trigger event since epsilon << linger for flowAction"
                  + " {}, db eventTimestamp {}, db leaseAcquisitionTimestamp {}, linger {}", flowAction,
              dbEventTimestamp, dbLeaseAcquisitionTimestamp, dbLinger);
        }
        // Use our event to acquire lease, check for previous db eventTimestamp and leaseAcquisitionTimestamp
        String formattedAcquireLeaseIfMatchingAllStatement =
            String.format(CONDITIONALLY_ACQUIRE_LEASE_IF_MATCHING_ALL_COLS_STATEMENT, this.leaseArbiterTableName);
        ResultSet rs = withPreparedStatement(
            formattedAcquireLeaseIfMatchingAllStatement + "; " + formattedSelectAfterInsertStatement,
            updateStatement -> {
              completeUpdatePreparedStatement(updateStatement, flowAction, eventTimeMillis, true,
                  true, dbEventTimestamp, dbLeaseAcquisitionTimestamp);
              return updateStatement.executeQuery();
            }, true);
        return handleResultFromAttemptedLeaseObtainment(rs, flowAction, eventTimeMillis);
      } // No longer leasing this event
        // CASE 6: Same event, no longer leasing event in db: terminate
        if (isWithinEpsilon) {
          return new NoLongerLeasingStatus();
        }
        // CASE 7: Distinct event, no longer leasing event in db
        // Use our event to acquire lease, check for previous db eventTimestamp and NULL leaseAcquisitionTimestamp
        String formattedAcquireLeaseIfFinishedStatement =
            String.format(CONDITIONALLY_ACQUIRE_LEASE_IF_FINISHED_LEASING_STATEMENT, this.leaseArbiterTableName);
        ResultSet rs = withPreparedStatement(
            formattedAcquireLeaseIfFinishedStatement + "; " + formattedSelectAfterInsertStatement,
            updateStatement -> {
              completeUpdatePreparedStatement(updateStatement, flowAction, eventTimeMillis, true,
                  false, dbEventTimestamp, null);
              return updateStatement.executeQuery();
            }, true);
        return handleResultFromAttemptedLeaseObtainment(rs, flowAction, eventTimeMillis);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Attempt lease by insert or update following a read based on the condition the state of the table has not changed
   * since the read. Parse the result to return the corresponding status based on successful insert/update or not.
   * @param resultSet
   * @param eventTimeMillis
   * @return LeaseAttemptStatus
   * @throws SQLException
   * @throws IOException
   */
  protected LeaseAttemptStatus handleResultFromAttemptedLeaseObtainment(ResultSet resultSet,
      DagActionStore.DagAction flowAction, long eventTimeMillis)
      throws SQLException, IOException {
    if (!resultSet.next()) {
      throw new IOException("Expected num rows and lease_acquisition_timestamp returned from query but received nothing");
    }
    int numRowsUpdated = resultSet.getInt(1);
    long leaseAcquisitionTimeMillis = resultSet.getTimestamp(2).getTime();
    int dbLinger = resultSet.getInt(3);
    if (numRowsUpdated == 1) {
      return new LeaseObtainedStatus(flowAction, eventTimeMillis, leaseAcquisitionTimeMillis);
    }
    // Another participant acquired lease in between
    return new LeasedToAnotherStatus(flowAction, eventTimeMillis,
        leaseAcquisitionTimeMillis + dbLinger - System.currentTimeMillis());
  }

  /**
   * Complete the INSERT statement for a new flow action lease where the flow action is not present in the table
   * @param statement
   * @param flowAction
   * @param eventTimeMillis
   * @throws SQLException
   */
  protected void completeInsertPreparedStatement(PreparedStatement statement, DagActionStore.DagAction flowAction,
      long eventTimeMillis) throws SQLException {
    int i = 0;
    // Values to set in new row
    statement.setString(++i, flowAction.getFlowGroup());
    statement.setString(++i, flowAction.getFlowName());
    statement.setString(++i, flowAction.getFlowExecutionId());
    statement.setString(++i, flowAction.getFlowActionType().toString());
    statement.setTimestamp(++i, new Timestamp(eventTimeMillis));
    // Values to check if existing row matches previous read
    statement.setString(++i, flowAction.getFlowGroup());
    statement.setString(++i, flowAction.getFlowName());
    statement.setString(++i, flowAction.getFlowExecutionId());
    statement.setString(++i, flowAction.getFlowActionType().toString());
    // Values to select for return
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
   * @param eventTimeMillis
   * @param needEventTimeCheck true if need to compare `originalEventTimestamp` with db event_timestamp
   * @param needLeaseAcquisitionTimeCheck true if need to compare `originalLeaseAcquisitionTimestamp` with db one
   * @param originalEventTimestamp value to compare to db one, null if not needed
   * @param originalLeaseAcquisitionTimestamp value to compare to db one, null if not needed
   * @throws SQLException
   */
  protected void completeUpdatePreparedStatement(PreparedStatement statement, DagActionStore.DagAction flowAction,
      long eventTimeMillis, boolean needEventTimeCheck, boolean needLeaseAcquisitionTimeCheck,
      Timestamp originalEventTimestamp, Timestamp originalLeaseAcquisitionTimestamp) throws SQLException {
    int i = 0;
    // Value to update
    statement.setTimestamp(++i, new Timestamp(eventTimeMillis));
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
    // Values to select for return
    statement.setString(++i, flowAction.getFlowGroup());
    statement.setString(++i, flowAction.getFlowName());
    statement.setString(++i, flowAction.getFlowExecutionId());
    statement.setString(++i, flowAction.getFlowActionType().toString());
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
            log.info("Multi-active lease arbiter lease attempt: [%s, eventTimestamp: %s] - FAILED to complete because "
                + "lease expired or event cleaned up before host completed required actions", flowAction,
                status.getEventTimestamp());
            return false;
          }
          if( numRowsUpdated == 1) {
            log.info("Multi-active lease arbiter lease attempt: [%s, eventTimestamp: %s] - COMPLETED, no longer leasing"
                    + " this event after this.", flowAction, status.getEventTimestamp());
            return true;
          };
          throw new IOException(String.format("Attempt to complete lease use: [%s, eventTimestamp: %s] - updated more "
                  + "rows than expected", flowAction, status.getEventTimestamp()));
        }, true);
  }

  /** Abstracts recurring pattern around resource management and exception re-mapping. */
  protected <T> T withPreparedStatement(String sql, CheckedFunction<PreparedStatement, T> f, boolean shouldCommit) throws IOException {
    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      T result = f.apply(statement);
      if (shouldCommit) {
        connection.commit();
      }
      return result;
    } catch (SQLException e) {
      log.warn("Received SQL exception that can result from invalid connection. Checking if validation query is set {} Exception is {}", ((HikariDataSource) this.dataSource).getConnectionTestQuery(), e);
      throw new IOException(e);
    }
  }
}
