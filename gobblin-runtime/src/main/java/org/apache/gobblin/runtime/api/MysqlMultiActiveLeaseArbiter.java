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
import java.util.Calendar;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.MysqlDataSourceFactory;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.util.ConfigUtils;

import static org.apache.gobblin.runtime.api.DagActionStore.DagAction.*;


/**
 * MySQL based implementation of the {@link MultiActiveLeaseArbiter} which uses a MySQL store to resolve ownership of
 * a flow event amongst multiple competing participants. A MySQL table is used to store flow identifying information as
 * well as the flow action associated with it. It uses two additional values of the `event_timestamp` and
 * `lease_acquisition_timestamp` to indicate an active lease, expired lease, and state of no longer leasing. The table
 * schema is as follows:
 * [flow_group | flow_name | flow_action | event_timestamp | lease_acquisition_timestamp]
 * (----------------------primary key------------------------)
 * We also maintain another table in the database with two constants that allow us to coordinate between participants
 * and ensure they are using the same values to base their coordination off of.
 * [epsilon | linger]
 * `epsilon` - time within we consider two event timestamps to be overlapping and can consolidate
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
  private final int epsilonMillis;
  private final int lingerMillis;
  private final long retentionPeriodMillis;
  private String thisTableRetentionStatement;
  private String thisTableGetInfoStatement;
  private String thisTableGetInfoStatementForReminder;
  private String thisTableSelectAfterInsertStatement;
  private String thisTableAcquireLeaseIfMatchingAllStatement;
  private String thisTableAcquireLeaseIfFinishedStatement;

  /*
    Notes:
    - Set `event_timestamp` default value to turn off timestamp auto-updates for row modifications which alters this col
      in an unexpected way upon completing the lease
    - MySQL converts TIMESTAMP values from the current time zone to UTC for storage, and back from UTC to the current
      time zone for retrieval. https://dev.mysql.com/doc/refman/8.0/en/datetime.html
        - Thus, for reading any timestamps from MySQL we convert the timezone from session (default) to UTC to always
          use epoch-millis in UTC locally
        - Similarly, for inserting/updating any timestamps we convert the timezone from UTC to session (as it will be
          (interpreted automatically as session time zone) and explicitly set all timestamp columns to avoid using the
          default auto-update/initialization values
    - We desire millisecond level precision and denote that with `(3)` for the TIMESTAMP types
   */
  private static final String CREATE_LEASE_ARBITER_TABLE_STATEMENT = "CREATE TABLE IF NOT EXISTS %s ("
      + "flow_group varchar(" + ServiceConfigKeys.MAX_FLOW_GROUP_LENGTH + ") NOT NULL, flow_name varchar("
      + ServiceConfigKeys.MAX_FLOW_GROUP_LENGTH + ") NOT NULL, " + " flow_action varchar(100) NOT NULL, "
      + "event_timestamp TIMESTAMP(3) NOT NULL, "
      + "lease_acquisition_timestamp TIMESTAMP(3) NULL, "
      + "PRIMARY KEY (flow_group,flow_name,flow_action))";
  // Deletes rows older than retention time period regardless of lease status as they should all be invalid or completed
  // since retention >> linger
  private static final String LEASE_ARBITER_TABLE_RETENTION_STATEMENT = "DELETE FROM %s WHERE event_timestamp < "
      + "DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL ? * 1000 MICROSECOND)";
  private static final String CREATE_CONSTANTS_TABLE_STATEMENT = "CREATE TABLE IF NOT EXISTS %s "
      + "(primary_key INT, epsilon INT, linger INT, PRIMARY KEY (primary_key))";
  // Only insert epsilon and linger values from config if this table does not contain a pre-existing values already.
  private static final String UPSERT_CONSTANTS_TABLE_STATEMENT = "INSERT INTO %s (primary_key, epsilon, linger) "
      + "VALUES(1, ?, ?) ON DUPLICATE KEY UPDATE epsilon=VALUES(epsilon), linger=VALUES(linger)";
  protected static final String WHERE_CLAUSE_TO_MATCH_KEY = "WHERE flow_group=? AND flow_name=? AND flow_action=?";
  protected static final String WHERE_CLAUSE_TO_MATCH_ROW = WHERE_CLAUSE_TO_MATCH_KEY
      + " AND event_timestamp=CONVERT_TZ(?, '+00:00', @@session.time_zone)"
      + " AND lease_acquisition_timestamp=CONVERT_TZ(?, '+00:00', @@session.time_zone)";
  protected static final String SELECT_AFTER_INSERT_STATEMENT = "SELECT "
      + "CONVERT_TZ(`event_timestamp`, @@session.time_zone, '+00:00') as utc_event_timestamp, "
      + "CONVERT_TZ(`lease_acquisition_timestamp`, @@session.time_zone, '+00:00') as utc_lease_acquisition_timestamp, "
      + "linger FROM %s, %s " + WHERE_CLAUSE_TO_MATCH_KEY;
  // Does a cross join between the two tables to have epsilon and linger values available. Returns the following values:
  // event_timestamp, lease_acquisition_timestamp, isWithinEpsilon (boolean if new event timestamp (current timestamp in
  // db) is within epsilon of event_timestamp in the table), leaseValidityStatus (1 if lease has not expired, 2 if
  // expired, 3 if column is NULL or no longer leasing)
  protected static final String GET_EVENT_INFO_STATEMENT = "SELECT "
      + "CONVERT_TZ(`event_timestamp`, @@session.time_zone, '+00:00') as utc_event_timestamp, "
      + "CONVERT_TZ(`lease_acquisition_timestamp`, @@session.time_zone, '+00:00') as utc_lease_acquisition_timestamp, "
      + "ABS(TIMESTAMPDIFF(microsecond, event_timestamp, CURRENT_TIMESTAMP(3))) / 1000 <= epsilon as is_within_epsilon, CASE "
      + "WHEN CURRENT_TIMESTAMP(3) < DATE_ADD(lease_acquisition_timestamp, INTERVAL linger*1000 MICROSECOND) then 1 "
      + "WHEN CURRENT_TIMESTAMP(3) >= DATE_ADD(lease_acquisition_timestamp, INTERVAL linger*1000 MICROSECOND) then 2 "
      + "ELSE 3 END as lease_validity_status, linger, "
      + "CONVERT_TZ(CURRENT_TIMESTAMP(3), @@session.time_zone, '+00:00') as utc_current_timestamp FROM %s, %s "
      + WHERE_CLAUSE_TO_MATCH_KEY;
  // Same as query above, except that isWithinEpsilon is True if the reminder event timestamp (provided by caller) is
  // OLDER than or equal to the db event_timestamp and within epsilon away from it.
  protected static final String GET_EVENT_INFO_STATEMENT_FOR_REMINDER = "SELECT "
      + "CONVERT_TZ(`event_timestamp`, @@session.time_zone, '+00:00') as utc_event_timestamp, "
      + "CONVERT_TZ(`lease_acquisition_timestamp`, @@session.time_zone, '+00:00') as utc_lease_acquisition_timestamp, "
      + "TIMESTAMPDIFF(microsecond, event_timestamp, CONVERT_TZ(?, '+00:00', @@session.time_zone)) / 1000 <= epsilon as is_within_epsilon, CASE "
      + "WHEN CURRENT_TIMESTAMP(3) < DATE_ADD(lease_acquisition_timestamp, INTERVAL linger*1000 MICROSECOND) then 1 "
      + "WHEN CURRENT_TIMESTAMP(3) >= DATE_ADD(lease_acquisition_timestamp, INTERVAL linger*1000 MICROSECOND) then 2 "
      + "ELSE 3 END as lease_validity_status, linger, "
      + "CONVERT_TZ(CURRENT_TIMESTAMP(3), @@session.time_zone, '+00:00') as utc_current_timestamp FROM %s, %s "
      + WHERE_CLAUSE_TO_MATCH_KEY;
  // Insert or update row to acquire lease if values have not changed since the previous read
  // Need to define three separate statements to handle cases where row does not exist or has null values to check
  protected static final String ACQUIRE_LEASE_IF_NEW_ROW_STATEMENT = "INSERT INTO %s (flow_group, flow_name, "
      + "flow_action, event_timestamp, lease_acquisition_timestamp) VALUES(?, ?, ?, CURRENT_TIMESTAMP(3), "
      + "CURRENT_TIMESTAMP(3))";
  protected static final String CONDITIONALLY_ACQUIRE_LEASE_IF_FINISHED_LEASING_STATEMENT = "UPDATE %s "
      + "SET event_timestamp=CURRENT_TIMESTAMP(3), lease_acquisition_timestamp=CURRENT_TIMESTAMP(3) "
      + WHERE_CLAUSE_TO_MATCH_KEY + " AND event_timestamp=CONVERT_TZ(?, '+00:00', @@session.time_zone) AND "
      + "lease_acquisition_timestamp is NULL";
  protected static final String CONDITIONALLY_ACQUIRE_LEASE_IF_MATCHING_ALL_COLS_STATEMENT = "UPDATE %s "
      + "SET event_timestamp=CURRENT_TIMESTAMP(3), lease_acquisition_timestamp=CURRENT_TIMESTAMP(3) "
      + WHERE_CLAUSE_TO_MATCH_ROW;
  // Complete lease acquisition if values have not changed since lease was acquired
  protected static final String CONDITIONALLY_COMPLETE_LEASE_STATEMENT = "UPDATE %s SET "
      + "event_timestamp=event_timestamp, lease_acquisition_timestamp = NULL " + WHERE_CLAUSE_TO_MATCH_ROW;
  private static final ThreadLocal<Calendar> UTC_CAL =
      ThreadLocal.withInitial(() -> Calendar.getInstance(TimeZone.getTimeZone("UTC")));

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
    this.epsilonMillis = ConfigUtils.getInt(config, ConfigurationKeys.SCHEDULER_EVENT_EPSILON_MILLIS_KEY,
        ConfigurationKeys.DEFAULT_SCHEDULER_EVENT_EPSILON_MILLIS);
    this.lingerMillis = ConfigUtils.getInt(config, ConfigurationKeys.SCHEDULER_EVENT_LINGER_MILLIS_KEY,
        ConfigurationKeys.DEFAULT_SCHEDULER_EVENT_LINGER_MILLIS);
    this.retentionPeriodMillis = ConfigUtils.getLong(config, ConfigurationKeys.SCHEDULER_LEASE_DETERMINATION_TABLE_RETENTION_PERIOD_MILLIS_KEY,
        ConfigurationKeys.DEFAULT_SCHEDULER_LEASE_DETERMINATION_TABLE_RETENTION_PERIOD_MILLIS);
    this.thisTableRetentionStatement = String.format(LEASE_ARBITER_TABLE_RETENTION_STATEMENT, this.leaseArbiterTableName);
    this.thisTableGetInfoStatement = String.format(GET_EVENT_INFO_STATEMENT, this.leaseArbiterTableName,
        this.constantsTableName);
    this.thisTableGetInfoStatementForReminder = String.format(GET_EVENT_INFO_STATEMENT_FOR_REMINDER,
        this.leaseArbiterTableName, this.constantsTableName);
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
    runRetentionOnArbitrationTable();
    log.info("MysqlMultiActiveLeaseArbiter initialized");
  }

  // Initialize Constants table if needed and insert row into it if one does not exist
  private void initializeConstantsTable() throws IOException {
    String createConstantsStatement = String.format(CREATE_CONSTANTS_TABLE_STATEMENT, this.constantsTableName);
    withPreparedStatement(createConstantsStatement, createStatement -> createStatement.executeUpdate(), true);

    String insertConstantsStatement = String.format(UPSERT_CONSTANTS_TABLE_STATEMENT, this.constantsTableName);
    withPreparedStatement(insertConstantsStatement, insertStatement -> {
      int i = 0;
      insertStatement.setInt(++i, epsilonMillis);
      insertStatement.setInt(++i, lingerMillis);
      return insertStatement.executeUpdate();
    }, true);
  }

  /**
   * Periodically deletes all rows in the table with event_timestamp older than the retention period defined by config.
   * // TODO: create a utility to run a SQL commend in a STPE using interval T
   */
  private void runRetentionOnArbitrationTable() {
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
    Runnable retentionTask = () -> {
      try {
        withPreparedStatement(thisTableRetentionStatement,
            retentionStatement -> {
              retentionStatement.setLong(1, retentionPeriodMillis);
              int numRowsDeleted = retentionStatement.executeUpdate();
              if (numRowsDeleted != 0) {
                log.info("Multi-active lease arbiter retention thread deleted {} rows from the lease arbiter table",
                    numRowsDeleted);
              }
              return numRowsDeleted;
            }, true);
      } catch (IOException e) {
        log.error("Failing to run retention on lease arbiter table. Unbounded growth can lead to database slowness and "
            + "affect our system performance. Examine exception: ", e);
      }
    };

    // Run retention thread every 4 hours (6 times a day)
    executor.scheduleAtFixedRate(retentionTask, 0, 4, TimeUnit.HOURS);
  }

  @Override
  public LeaseAttemptStatus tryAcquireLease(DagActionStore.DagAction flowAction, long eventTimeMillis,
      boolean isReminderEvent) throws IOException {
    log.info("Multi-active scheduler about to handle trigger event: [{}, is: {}, triggerEventTimestamp: {}]",
        flowAction, isReminderEvent ? "reminder" : "original", eventTimeMillis);
    // Query lease arbiter table about this flow action
    Optional<GetEventInfoResult> getResult = getExistingEventInfo(flowAction, isReminderEvent, eventTimeMillis);

    // TODO: change all the `CASE N: ...` statements back to debug statements after uncovering issue
    try {
      if (!getResult.isPresent()) {
        log.info("tryAcquireLease for [{}, is; {}, eventTimestamp: {}] - CASE 1: no existing row for this flow action,"
            + " then go ahead and insert", flowAction, isReminderEvent ? "reminder" : "original", eventTimeMillis);
        int numRowsUpdated = attemptLeaseIfNewRow(flowAction);
       return evaluateStatusAfterLeaseAttempt(numRowsUpdated, flowAction, Optional.empty(), isReminderEvent);
      }

      // Extract values from result set
      Timestamp dbEventTimestamp = getResult.get().getDbEventTimestamp();
      Timestamp dbLeaseAcquisitionTimestamp = getResult.get().getDbLeaseAcquisitionTimestamp();
      boolean isWithinEpsilon = getResult.get().isWithinEpsilon();
      int leaseValidityStatus = getResult.get().getLeaseValidityStatus();
      // Used to calculate minimum amount of time until a participant should check whether a lease expired
      int dbLinger = getResult.get().getDbLinger();
      Timestamp dbCurrentTimestamp = getResult.get().getDbCurrentTimestamp();

      // For reminder event, we can stop early if the reminder eventTimeMillis is older than the current event in the db
      // because db laundering tells us that the currently worked on db event is newer and will have its own reminders
      if (isReminderEvent) {
        if (eventTimeMillis < dbEventTimestamp.getTime()) {
          log.info("tryAcquireLease for [{}, is: {}, eventTimestamp: {}] - dbEventTimeMillis: {} - A new event trigger "
                  + "is being worked on, so this older reminder will be dropped.", flowAction,
              isReminderEvent ? "reminder" : "original", eventTimeMillis, dbEventTimestamp);
          return new NoLongerLeasingStatus();
        }
        if (eventTimeMillis > dbEventTimestamp.getTime()) {
          // TODO: emit metric here to capture this unexpected behavior
          log.warn("tryAcquireLease for [{}, is: {}, eventTimestamp: {}] - dbEventTimeMillis: {} - Severe constraint "
                  + "violation encountered: a reminder event newer than db event was found when db laundering should "
                  + "ensure monotonically increasing laundered event times.", flowAction,
              isReminderEvent ? "reminder" : "original", eventTimeMillis, dbEventTimestamp.getTime());
        }
        if (eventTimeMillis == dbEventTimestamp.getTime()) {
          // TODO: change this to a debug after fixing issue
          log.info("tryAcquireLease for [{}, is: {}, eventTimestamp: {}] - dbEventTimeMillis: {} - Reminder event time "
                  + "is the same as db event.", flowAction, isReminderEvent ? "reminder" : "original",
              eventTimeMillis, dbEventTimestamp);
        }
      }

      log.info("Multi-active arbiter replacing local trigger event timestamp [{}, is: {}, triggerEventTimestamp: {}] "
          + "with database eventTimestamp {} (in epoch-millis)", flowAction, isReminderEvent ? "reminder" : "original",
          eventTimeMillis, dbCurrentTimestamp.getTime());

      // Lease is valid
      if (leaseValidityStatus == 1) {
        if (isWithinEpsilon) {
          DagActionStore.DagAction updatedFlowAction = flowAction.updateFlowExecutionId(dbEventTimestamp.getTime());
          log.info("tryAcquireLease for [{}, is: {}, eventTimestamp: {}] - CASE 2: Same event, lease is valid",
              updatedFlowAction, isReminderEvent ? "reminder" : "original", dbCurrentTimestamp.getTime());
          // Utilize db timestamp for reminder
          return new LeasedToAnotherStatus(updatedFlowAction,
              dbLeaseAcquisitionTimestamp.getTime() + dbLinger - dbCurrentTimestamp.getTime());
        }
        DagActionStore.DagAction updatedFlowAction = flowAction.updateFlowExecutionId(dbCurrentTimestamp.getTime());
        log.info("tryAcquireLease for [{}, is: {}, eventTimestamp: {}] - CASE 3: Distinct event, lease is valid",
            updatedFlowAction, isReminderEvent ? "reminder" : "original", dbCurrentTimestamp.getTime());
        // Utilize db lease acquisition timestamp for wait time
        return new LeasedToAnotherStatus(updatedFlowAction,
            dbLeaseAcquisitionTimestamp.getTime() + dbLinger  - dbCurrentTimestamp.getTime());
      } // Lease is invalid
      else if (leaseValidityStatus == 2) {
        log.info("tryAcquireLease for [{}, is: {}, eventTimestamp: {}] - CASE 4: Lease is out of date (regardless of "
            + "whether same or distinct event)", flowAction, isReminderEvent ? "reminder" : "original",
            dbCurrentTimestamp.getTime());
        if (isWithinEpsilon && !isReminderEvent) {
          log.warn("Lease should not be out of date for the same trigger event since epsilon << linger for flowAction"
                  + " {}, db eventTimestamp {}, db leaseAcquisitionTimestamp {}, linger {}", flowAction,
              dbEventTimestamp, dbLeaseAcquisitionTimestamp, dbLinger);
        }
        // Use our event to acquire lease, check for previous db eventTimestamp and leaseAcquisitionTimestamp
        int numRowsUpdated = attemptLeaseIfExistingRow(thisTableAcquireLeaseIfMatchingAllStatement, flowAction,
            true,true, dbEventTimestamp, dbLeaseAcquisitionTimestamp);
        return evaluateStatusAfterLeaseAttempt(numRowsUpdated, flowAction, Optional.of(dbCurrentTimestamp), isReminderEvent);
      } // No longer leasing this event
        if (isWithinEpsilon) {
          log.info("tryAcquireLease for [{}, is: {}, eventTimestamp: {}] - CASE 5: Same event, no longer leasing event"
              + " in db", flowAction, isReminderEvent ? "reminder" : "original", dbCurrentTimestamp.getTime());
          return new NoLongerLeasingStatus();
        }
        log.info("tryAcquireLease for [{}, is: {}, eventTimestamp: {}] - CASE 6: Distinct event, no longer leasing "
            + "event in db", flowAction, isReminderEvent ? "reminder" : "original", dbCurrentTimestamp.getTime());
        // Use our event to acquire lease, check for previous db eventTimestamp and NULL leaseAcquisitionTimestamp
        int numRowsUpdated = attemptLeaseIfExistingRow(thisTableAcquireLeaseIfFinishedStatement, flowAction,
            true, false, dbEventTimestamp, null);
        return evaluateStatusAfterLeaseAttempt(numRowsUpdated, flowAction, Optional.of(dbCurrentTimestamp), isReminderEvent);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Checks leaseArbiterTable for an existing entry for this flow action and event time
   */
  protected Optional<GetEventInfoResult> getExistingEventInfo(DagActionStore.DagAction flowAction,
      boolean isReminderEvent, long eventTimeMillis) throws IOException {
    return withPreparedStatement(isReminderEvent ? thisTableGetInfoStatementForReminder : thisTableGetInfoStatement,
        getInfoStatement -> {
          int i = 0;
          if (isReminderEvent) {
            getInfoStatement.setTimestamp(++i, new Timestamp(eventTimeMillis), UTC_CAL.get());
          }
          getInfoStatement.setString(++i, flowAction.getFlowGroup());
          getInfoStatement.setString(++i, flowAction.getFlowName());
          getInfoStatement.setString(++i, flowAction.getFlowActionType().toString());
          ResultSet resultSet = getInfoStatement.executeQuery();
          try {
            if (!resultSet.next()) {
              return Optional.<GetEventInfoResult>empty();
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
      Timestamp dbEventTimestamp = resultSet.getTimestamp("utc_event_timestamp", UTC_CAL.get());
      Timestamp dbLeaseAcquisitionTimestamp = resultSet.getTimestamp("utc_lease_acquisition_timestamp", UTC_CAL.get());
      boolean withinEpsilon = resultSet.getBoolean("is_within_epsilon");
      int leaseValidityStatus = resultSet.getInt("lease_validity_status");
      int dbLinger = resultSet.getInt("linger");
      Timestamp dbCurrentTimestamp = resultSet.getTimestamp("utc_current_timestamp", UTC_CAL.get());
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
          completeUpdatePreparedStatement(insertStatement, flowAction, needEventTimeCheck, needLeaseAcquisition,
              dbEventTimestamp, dbLeaseAcquisitionTimestamp);
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
          throw new IOException("Expected resultSet containing row information for the lease that was attempted but "
              + "received nothing.");
        }
        if (resultSet.getTimestamp("utc_event_timestamp", UTC_CAL.get()) == null) {
          throw new IOException("event_timestamp should never be null (it is always set to current timestamp)");
        }
        long eventTimeMillis = resultSet.getTimestamp("utc_event_timestamp", UTC_CAL.get()).getTime();
        // Lease acquisition timestamp is null if another participant has completed the lease
        Optional<Long> leaseAcquisitionTimeMillis =
            resultSet.getTimestamp("utc_lease_acquisition_timestamp", UTC_CAL.get()) == null ? Optional.empty() :
            Optional.of(resultSet.getTimestamp("utc_lease_acquisition_timestamp", UTC_CAL.get()).getTime());
        int dbLinger = resultSet.getInt("linger");
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
      DagActionStore.DagAction flowAction, Optional<Timestamp> dbCurrentTimestamp, boolean isReminderEvent)
      throws SQLException, IOException {
    // Fetch values in row after attempted insert
    SelectInfoResult selectInfoResult = getRowInfo(flowAction);
    // Another participant won the lease in between
    if (!selectInfoResult.getLeaseAcquisitionTimeMillis().isPresent()) {
      return new NoLongerLeasingStatus();
    }
    DagActionStore.DagAction updatedFlowAction = flowAction.updateFlowExecutionId(selectInfoResult.eventTimeMillis);
    if (numRowsUpdated == 1) {
      log.info("Obtained lease for [{}, is: {}, eventTimestamp: {}] successfully!", updatedFlowAction,
          isReminderEvent ? "reminder" : "original", selectInfoResult.eventTimeMillis);
      return new LeaseObtainedStatus(updatedFlowAction, selectInfoResult.getLeaseAcquisitionTimeMillis().get());
    }
    log.info("Another participant acquired lease in between for [{}, is: {}, eventTimestamp: {}] - num rows updated: {}",
        updatedFlowAction, isReminderEvent ? "reminder" : "original", selectInfoResult.eventTimeMillis, numRowsUpdated);
    // Another participant acquired lease in between
    return new LeasedToAnotherStatus(updatedFlowAction,
        selectInfoResult.getLeaseAcquisitionTimeMillis().get() + selectInfoResult.getDbLinger()
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
    statement.setString(++i, flowAction.getFlowActionType().toString());
    // Values that may be needed depending on the insert statement
    if (needEventTimeCheck) {
      statement.setTimestamp(++i, originalEventTimestamp, UTC_CAL.get());
    }
    if (needLeaseAcquisitionTimeCheck) {
      statement.setTimestamp(++i, originalLeaseAcquisitionTimestamp, UTC_CAL.get());
    }
  }

  @Override
  public boolean recordLeaseSuccess(LeaseObtainedStatus status)
      throws IOException {
    DagActionStore.DagAction flowAction = status.getFlowAction();
    String flowGroup = flowAction.getFlowGroup();
    String flowName = flowAction.getFlowName();
    DagActionStore.FlowActionType flowActionType = flowAction.getFlowActionType();
    return withPreparedStatement(String.format(CONDITIONALLY_COMPLETE_LEASE_STATEMENT, leaseArbiterTableName),
        updateStatement -> {
          int i = 0;
          updateStatement.setString(++i, flowGroup);
          updateStatement.setString(++i, flowName);
          updateStatement.setString(++i, flowActionType.toString());
          updateStatement.setTimestamp(++i, new Timestamp(status.getEventTimeMillis()), UTC_CAL.get());
          updateStatement.setTimestamp(++i, new Timestamp(status.getLeaseAcquisitionTimestamp()), UTC_CAL.get());
          int numRowsUpdated = updateStatement.executeUpdate();
          if (numRowsUpdated == 0) {
            log.info("Multi-active lease arbiter lease attempt: [{}, eventTimestamp: {}] - FAILED to complete because "
                + "lease expired or event cleaned up before host completed required actions", flowAction,
                status.getEventTimeMillis());
            return false;
          }
          if( numRowsUpdated == 1) {
            log.info("Multi-active lease arbiter lease attempt: [{}, eventTimestamp: {}] - COMPLETED, no longer leasing"
                    + " this event after this.", flowAction, status.getEventTimeMillis());
            return true;
          };
          throw new IOException(String.format("Attempt to complete lease use: [%s, eventTimestamp: %s] - updated more "
                  + "rows than expected", flowAction, status.getEventTimeMillis()));
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
    private final Optional<Long> leaseAcquisitionTimeMillis;
    private final int dbLinger;
  }
}
