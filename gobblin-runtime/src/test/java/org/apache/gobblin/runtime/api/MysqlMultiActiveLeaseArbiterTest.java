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

import com.typesafe.config.Config;
import java.io.IOException;
import java.sql.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.gobblin.runtime.api.MysqlMultiActiveLeaseArbiter.*;



@Slf4j
public class MysqlMultiActiveLeaseArbiterTest {
  private static final int EPSILON = 30000;
  private static final int LINGER = 80000;
  private static final String USER = "testUser";
  private static final String PASSWORD = "testPassword";
  private static final String TABLE = "mysql_multi_active_lease_arbiter_store";
  private static final String flowGroup = "testFlowGroup";
  private static final String flowName = "testFlowName";
  private static final String flowExecutionId = "12345677";
  // The following are considered unique because they correspond to different flow action types
  private static DagActionStore.DagAction launchDagAction =
      new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, DagActionStore.FlowActionType.LAUNCH);
  private static DagActionStore.DagAction resumeDagAction =
      new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, DagActionStore.FlowActionType.RESUME);
  private static final long eventTimeMillis = System.currentTimeMillis();
  private MysqlMultiActiveLeaseArbiter mysqlMultiActiveLeaseArbiter;
  private String formattedAcquireLeaseNewRowStatement =
      String.format(MysqlMultiActiveLeaseArbiter.CONDITIONALLY_ACQUIRE_LEASE_IF_NEW_ROW_STATEMENT,
          TABLE, TABLE);
  private String formattedAcquireLeaseIfMatchingAllStatement =
      String.format(CONDITIONALLY_ACQUIRE_LEASE_IF_MATCHING_ALL_COLS_STATEMENT, TABLE);
  private String formattedAcquireLeaseIfFinishedStatement =
      String.format(CONDITIONALLY_ACQUIRE_LEASE_IF_FINISHED_LEASING_STATEMENT, TABLE);
  private String formattedSelectAfterInsertStatement = String.format(SELECT_AFTER_INSERT_STATEMENT, TABLE,
          ConfigurationKeys.DEFAULT_MULTI_ACTIVE_SCHEDULER_CONSTANTS_DB_TABLE);

  // The setup functionality verifies that the initialization of the tables is done correctly and verifies any SQL
  // syntax errors.
  @BeforeClass
  public void setUp() throws Exception {
    ITestMetastoreDatabase testDb = TestMetastoreDatabaseFactory.get();

    Config config = ConfigBuilder.create()
        .addPrimitive(ConfigurationKeys.SCHEDULER_EVENT_EPSILON_MILLIS_KEY, EPSILON)
        .addPrimitive(ConfigurationKeys.SCHEDULER_EVENT_LINGER_MILLIS_KEY, LINGER)
        .addPrimitive(ConfigurationKeys.MYSQL_LEASE_ARBITER_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_URL_KEY, testDb.getJdbcUrl())
        .addPrimitive(ConfigurationKeys.MYSQL_LEASE_ARBITER_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_USER_KEY, USER)
        .addPrimitive(ConfigurationKeys.MYSQL_LEASE_ARBITER_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, PASSWORD)
        .addPrimitive(ConfigurationKeys.SCHEDULER_LEASE_DETERMINATION_STORE_DB_TABLE_KEY, TABLE)
        .build();

    this.mysqlMultiActiveLeaseArbiter = new MysqlMultiActiveLeaseArbiter(config);
  }

  /*
     Tests all cases of trying to acquire a lease (CASES 1-6 detailed below) for a flow action event with one
     participant involved.
  */
  // TODO: refactor this to break it into separate test cases as much is possible
  @Test
  public void testAcquireLeaseSingleParticipant() throws Exception {
    // Tests CASE 1 of acquire lease for a flow action event not present in DB
    MultiActiveLeaseArbiter.LeaseAttemptStatus firstLaunchStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchDagAction, eventTimeMillis);
    Assert.assertTrue(firstLaunchStatus instanceof MultiActiveLeaseArbiter.LeaseObtainedStatus);
    MultiActiveLeaseArbiter.LeaseObtainedStatus firstObtainedStatus =
        (MultiActiveLeaseArbiter.LeaseObtainedStatus) firstLaunchStatus;
    Assert.assertTrue(firstObtainedStatus.getEventTimestamp() <=
        firstObtainedStatus.getLeaseAcquisitionTimestamp());
    Assert.assertTrue(firstObtainedStatus.getFlowAction().equals(
        new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, DagActionStore.FlowActionType.LAUNCH)));

    // Verify that different DagAction types for the same flow can have leases at the same time
    DagActionStore.DagAction killDagAction = new
        DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, DagActionStore.FlowActionType.KILL);
    MultiActiveLeaseArbiter.LeaseAttemptStatus killStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(killDagAction, eventTimeMillis);
    Assert.assertTrue(killStatus instanceof MultiActiveLeaseArbiter.LeaseObtainedStatus);
    MultiActiveLeaseArbiter.LeaseObtainedStatus killObtainedStatus =
        (MultiActiveLeaseArbiter.LeaseObtainedStatus) killStatus;
    Assert.assertTrue(
        killObtainedStatus.getLeaseAcquisitionTimestamp() >= killObtainedStatus.getEventTimestamp());

    // Tests CASE 2 of acquire lease for a flow action event that already has a valid lease for the same event in db
    // Very little time should have passed if this test directly follows the one above so this call will be considered
    // the same as the previous event
    MultiActiveLeaseArbiter.LeaseAttemptStatus secondLaunchStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchDagAction, eventTimeMillis);
    Assert.assertTrue(secondLaunchStatus instanceof MultiActiveLeaseArbiter.LeasedToAnotherStatus);
    MultiActiveLeaseArbiter.LeasedToAnotherStatus secondLeasedToAnotherStatus =
        (MultiActiveLeaseArbiter.LeasedToAnotherStatus) secondLaunchStatus;
    Assert.assertTrue(secondLeasedToAnotherStatus.getEventTimeMillis() == firstObtainedStatus.getEventTimestamp());
    Assert.assertTrue(secondLeasedToAnotherStatus.getMinimumLingerDurationMillis() >= LINGER);

    // Tests CASE 3 of trying to acquire a lease for a distinct flow action event, while the previous event's lease is
    // valid
    // Allow enough time to pass for this trigger to be considered distinct, but not enough time so the lease expires
    Thread.sleep(EPSILON * 3/2);
    MultiActiveLeaseArbiter.LeaseAttemptStatus thirdLaunchStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchDagAction, eventTimeMillis);
    Assert.assertTrue(thirdLaunchStatus instanceof MultiActiveLeaseArbiter.LeasedToAnotherStatus);
    MultiActiveLeaseArbiter.LeasedToAnotherStatus thirdLeasedToAnotherStatus =
        (MultiActiveLeaseArbiter.LeasedToAnotherStatus) thirdLaunchStatus;
    Assert.assertTrue(thirdLeasedToAnotherStatus.getEventTimeMillis() > firstObtainedStatus.getEventTimestamp());
    Assert.assertTrue(thirdLeasedToAnotherStatus.getMinimumLingerDurationMillis() < LINGER);

    // Tests CASE 4 of lease out of date
    Thread.sleep(LINGER);
    MultiActiveLeaseArbiter.LeaseAttemptStatus fourthLaunchStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchDagAction, eventTimeMillis);
    Assert.assertTrue(fourthLaunchStatus instanceof MultiActiveLeaseArbiter.LeaseObtainedStatus);
    MultiActiveLeaseArbiter.LeaseObtainedStatus fourthObtainedStatus =
        (MultiActiveLeaseArbiter.LeaseObtainedStatus) fourthLaunchStatus;
    Assert.assertTrue(fourthObtainedStatus.getEventTimestamp() > eventTimeMillis + LINGER);
    Assert.assertTrue(fourthObtainedStatus.getEventTimestamp()
        <= fourthObtainedStatus.getLeaseAcquisitionTimestamp());

    // Tests CASE 5 of no longer leasing the same event in DB
    // done immediately after previous lease obtainment so should be marked as the same event
    Assert.assertTrue(mysqlMultiActiveLeaseArbiter.recordLeaseSuccess(fourthObtainedStatus));
    Assert.assertTrue(System.currentTimeMillis() - fourthObtainedStatus.getEventTimestamp() < EPSILON);
    MultiActiveLeaseArbiter.LeaseAttemptStatus fifthLaunchStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchDagAction, eventTimeMillis);
    Assert.assertTrue(fifthLaunchStatus instanceof MultiActiveLeaseArbiter.NoLongerLeasingStatus);

    // Tests CASE 6 of no longer leasing a distinct event in DB
    // Wait so this event is considered distinct and a new lease will be acquired
    Thread.sleep(EPSILON * 3/2);
    MultiActiveLeaseArbiter.LeaseAttemptStatus sixthLaunchStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchDagAction, eventTimeMillis);
    Assert.assertTrue(sixthLaunchStatus instanceof MultiActiveLeaseArbiter.LeaseObtainedStatus);
    MultiActiveLeaseArbiter.LeaseObtainedStatus sixthObtainedStatus =
        (MultiActiveLeaseArbiter.LeaseObtainedStatus) sixthLaunchStatus;
    Assert.assertTrue(sixthObtainedStatus.getEventTimestamp()
        <= sixthObtainedStatus.getLeaseAcquisitionTimestamp());
  }

  /*
     Tests CONDITIONALLY_ACQUIRE_LEASE_IF_NEW_ROW_STATEMENT to ensure an insertion is not attempted unless the table
     state remains the same as the prior read, which expects no row matching the primary key in the table
     Note: this isolates and tests CASE 1 in which another participant could have acquired the lease between the time
     the read was done and subsequent write was carried out
  */
  @Test //(dependsOnMethods = "testAcquireLeaseSingleParticipant")
  public void testConditionallyAcquireLeaseIfNewRow() throws IOException {
    // Inserting the first time should update 1 row
    int numRowsUpdated = this.mysqlMultiActiveLeaseArbiter.withPreparedStatement(formattedAcquireLeaseNewRowStatement,
        insertStatement -> {
          completeInsertPreparedStatement(insertStatement, resumeDagAction);
          return insertStatement.executeUpdate();
        }, true);
    Assert.assertEquals(numRowsUpdated, 1);
    // Inserting the second time should update 0 rows but not throw any error
    numRowsUpdated = this.mysqlMultiActiveLeaseArbiter.withPreparedStatement(formattedAcquireLeaseNewRowStatement,
        insertStatement -> {
          completeInsertPreparedStatement(insertStatement, resumeDagAction);
          return insertStatement.executeUpdate();
        }, true);
    Assert.assertEquals(numRowsUpdated, 0);
  }

    /*
  CASE 4; lease out of date and try to acquire lease if matching all -> update in btwn so not matching one or more of
  the timestamps
  - will end up returning LeasedToAnother
   */
  @Test (dependsOnMethods = "testConditionallyAcquireLeaseIfNewRow")
  public void testConditionallyAcquireLeaseIfFMatchingAllColsStatement() throws IOException {
    MysqlMultiActiveLeaseArbiter.SelectInfoResult selectInfoResult =
        this.mysqlMultiActiveLeaseArbiter.withPreparedStatement(formattedSelectAfterInsertStatement,
        selectStatement -> {
          completeWhereClauseMatchingKeyPreparedStatement(selectStatement, resumeDagAction);
          return MysqlMultiActiveLeaseArbiter.createSelectInfoResult(selectStatement.executeQuery());
        }, true);
    // This insert should work since the values match all the columns
    int numRowsUpdated = this.mysqlMultiActiveLeaseArbiter.withPreparedStatement(
        formattedAcquireLeaseIfMatchingAllStatement,
        insertStatement -> {
          completeUpdatePreparedStatement(insertStatement, resumeDagAction, true,
              true, new Timestamp(selectInfoResult.getEventTimeMillis()),
              new Timestamp(selectInfoResult.getLeaseAcquisitionTimeMillis()));
          return insertStatement.executeUpdate();
        }, true);
    Assert.assertEquals(numRowsUpdated, 1);

    // The following insert will fail since the eventTimestamp does not match
    numRowsUpdated = this.mysqlMultiActiveLeaseArbiter.withPreparedStatement(
        formattedAcquireLeaseIfMatchingAllStatement,
        insertStatement -> {
          completeUpdatePreparedStatement(insertStatement, resumeDagAction, true,
              true, new Timestamp(99999),
              new Timestamp(selectInfoResult.getLeaseAcquisitionTimeMillis()));
          return insertStatement.executeUpdate();
        }, true);
    Assert.assertEquals(numRowsUpdated, 0);

    // The following insert will fail since the leaseAcquisitionTimestamp does not match
    numRowsUpdated = this.mysqlMultiActiveLeaseArbiter.withPreparedStatement(
        formattedAcquireLeaseIfMatchingAllStatement,
        insertStatement -> {
          completeUpdatePreparedStatement(insertStatement, resumeDagAction, true,
              true, new Timestamp(selectInfoResult.getEventTimeMillis()),
              new Timestamp(99999));
          return insertStatement.executeUpdate();
        }, true);
    Assert.assertEquals(numRowsUpdated, 0);
  }

  /*
  CASE 6: distinct event, no longer leasing in db will try to acquire lease if event time remains same
  - in btwn update the event time and keep others same
  - expected behavior also get LEASED to another instead of LeaseObtainedStatus
   */
  @Test (dependsOnMethods = "testConditionallyAcquireLeaseIfFMatchingAllColsStatement")
  public void testConditionallyAcquireLeaseIfFinishedLeasingStatement() throws IOException, InterruptedException {
    // Mark the resume action lease from above as completed by fabricating a LeaseObtainedStatus
    MysqlMultiActiveLeaseArbiter.SelectInfoResult selectInfoResult =
        this.mysqlMultiActiveLeaseArbiter.withPreparedStatement(formattedSelectAfterInsertStatement,
            selectStatement -> {
              completeWhereClauseMatchingKeyPreparedStatement(selectStatement, resumeDagAction);
              return MysqlMultiActiveLeaseArbiter.createSelectInfoResult(selectStatement.executeQuery());
            }, true);
    boolean markedSuccess = this.mysqlMultiActiveLeaseArbiter.recordLeaseSuccess(new LeaseObtainedStatus(
        resumeDagAction, selectInfoResult.getEventTimeMillis(), selectInfoResult.getLeaseAcquisitionTimeMillis()));
    Assert.assertTrue(markedSuccess);

    // Sleep enough time for event to be considered distinct
    Thread.sleep(LINGER);

    // The following insert will fail since eventTimestamp does not match the expected
    int numRowsUpdated = this.mysqlMultiActiveLeaseArbiter.withPreparedStatement(
        formattedAcquireLeaseIfFinishedStatement,
        insertStatement -> {
          completeUpdatePreparedStatement(insertStatement, resumeDagAction, true,
              false, new Timestamp(99999), null);
          return insertStatement.executeUpdate();
        }, true);
    Assert.assertEquals(numRowsUpdated, 0);

    // This insert does match since we utilize the right eventTimestamp
    numRowsUpdated = this.mysqlMultiActiveLeaseArbiter.withPreparedStatement(
        formattedAcquireLeaseIfFinishedStatement,
        insertStatement -> {
          completeUpdatePreparedStatement(insertStatement, resumeDagAction, true,
              false, new Timestamp(selectInfoResult.getEventTimeMillis()), null);
          return insertStatement.executeUpdate();
        }, true);
    Assert.assertEquals(numRowsUpdated, 1);
  }
}
