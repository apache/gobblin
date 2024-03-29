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

package org.apache.gobblin.service.modules.orchestration;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Optional;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;


@Slf4j
public class MysqlMultiActiveLeaseArbiterTest {
  private static final long EPSILON = 10000L;
  private static final long MORE_THAN_EPSILON = (long) (EPSILON * 1.1);
  private static final long LINGER = 50000L;
  private static final long MORE_THAN_LINGER = (long) (LINGER * 1.1);
  private static final String USER = "testUser";
  private static final String PASSWORD = "testPassword";
  private static final String TABLE = "mysql_multi_active_lease_arbiter_store";
  private static final String CONSTANTS_TABLE = "constants_store";
  private static final String flowGroup = "testFlowGroup";
  private static final String flowGroup2 = "testFlowGroup2";
  private static final String flowName = "testFlowName";
  private static final String jobName = "testJobName";
  private static final String flowExecutionId = "12345677";
  // Dag actions with the same flow info but different flow action types are considered unique
  private static DagActionStore.DagAction launchDagAction =
      new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, jobName, DagActionStore.DagActionType.LAUNCH);
  private static DagActionStore.DagAction resumeDagAction =
      new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, jobName, DagActionStore.DagActionType.RESUME);
  private static DagActionStore.DagAction launchDagAction2 =
      new DagActionStore.DagAction(flowGroup2, flowName, flowExecutionId, jobName, DagActionStore.DagActionType.LAUNCH);
  private static final long eventTimeMillis = System.currentTimeMillis();
  private static final Timestamp dummyTimestamp = new Timestamp(99999);
  private MysqlMultiActiveLeaseArbiter mysqlMultiActiveLeaseArbiter;
  private String formattedAcquireLeaseIfMatchingAllStatement =
      String.format(MysqlMultiActiveLeaseArbiter.CONDITIONALLY_ACQUIRE_LEASE_IF_MATCHING_ALL_COLS_STATEMENT, TABLE);
  private String formattedAcquireLeaseIfFinishedStatement =
      String.format(MysqlMultiActiveLeaseArbiter.CONDITIONALLY_ACQUIRE_LEASE_IF_FINISHED_LEASING_STATEMENT, TABLE);

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
        .addPrimitive(ConfigurationKeys.LEASE_DETERMINATION_STORE_DB_TABLE_KEY, TABLE)
        .addPrimitive(ConfigurationKeys.MULTI_ACTIVE_CONSTANTS_DB_TABLE_KEY, CONSTANTS_TABLE)
        .build();

    this.mysqlMultiActiveLeaseArbiter = new MysqlMultiActiveLeaseArbiter(config);
  }

  /*
     Tests all cases of trying to acquire a lease (CASES 1-6 detailed below) for a flow action event with one
     participant involved. All of the cases allow the flowExecutionId to be updated by lease arbiter by setting
     `adoptConsensusFlowExecutionId` to true.
  */
  // TODO: refactor this to break it into separate test cases as much is possible
  @Test
  public void testAcquireLeaseSingleParticipant() throws Exception {
    // Tests CASE 1 of acquire lease for a flow action event not present in DB
    LeaseAttemptStatus firstLaunchStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchDagAction, eventTimeMillis, false, true);
    Assert.assertTrue(firstLaunchStatus instanceof LeaseAttemptStatus.LeaseObtainedStatus);
    LeaseAttemptStatus.LeaseObtainedStatus firstObtainedStatus =
        (LeaseAttemptStatus.LeaseObtainedStatus) firstLaunchStatus;
    Assert.assertTrue(firstObtainedStatus.getEventTimeMillis() <=
        firstObtainedStatus.getLeaseAcquisitionTimestamp());
    Assert.assertTrue(firstObtainedStatus.getDagAction().equals(
        new DagActionStore.DagAction(flowGroup, flowName, String.valueOf(firstObtainedStatus.getEventTimeMillis()),
            jobName, DagActionStore.DagActionType.LAUNCH)));

    // Verify that different DagAction types for the same flow can have leases at the same time
    DagActionStore.DagAction killDagAction = new
        DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, jobName, DagActionStore.DagActionType.KILL);
    LeaseAttemptStatus killStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(killDagAction, eventTimeMillis, false, true);
    Assert.assertTrue(killStatus instanceof LeaseAttemptStatus.LeaseObtainedStatus);
    LeaseAttemptStatus.LeaseObtainedStatus killObtainedStatus =
        (LeaseAttemptStatus.LeaseObtainedStatus) killStatus;
    Assert.assertTrue(
        killObtainedStatus.getLeaseAcquisitionTimestamp() >= killObtainedStatus.getEventTimeMillis());

    // Tests CASE 2 of acquire lease for a flow action event that already has a valid lease for the same event in db
    // Very little time should have passed if this test directly follows the one above so this call will be considered
    // the same as the previous event
    LeaseAttemptStatus secondLaunchStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchDagAction, eventTimeMillis, false, true);
    Assert.assertTrue(secondLaunchStatus instanceof LeaseAttemptStatus.LeasedToAnotherStatus);
    LeaseAttemptStatus.LeasedToAnotherStatus secondLeasedToAnotherStatus =
        (LeaseAttemptStatus.LeasedToAnotherStatus) secondLaunchStatus;
    Assert.assertEquals(firstObtainedStatus.getEventTimeMillis(), secondLeasedToAnotherStatus.getEventTimeMillis());
    Assert.assertTrue(secondLeasedToAnotherStatus.getMinimumLingerDurationMillis() > 0);

    // Tests CASE 3 of trying to acquire a lease for a distinct flow action event, while the previous event's lease is
    // valid
    // Allow enough time to pass for this trigger to be considered distinct, but not enough time so the lease expires
    Thread.sleep(MORE_THAN_EPSILON);
    LeaseAttemptStatus thirdLaunchStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchDagAction, eventTimeMillis, false, true);
    Assert.assertTrue(thirdLaunchStatus instanceof LeaseAttemptStatus.LeasedToAnotherStatus);
    LeaseAttemptStatus.LeasedToAnotherStatus thirdLeasedToAnotherStatus =
        (LeaseAttemptStatus.LeasedToAnotherStatus) thirdLaunchStatus;
    Assert.assertTrue(thirdLeasedToAnotherStatus.getEventTimeMillis() > firstObtainedStatus.getEventTimeMillis());
    Assert.assertTrue(thirdLeasedToAnotherStatus.getMinimumLingerDurationMillis() < LINGER);

    // Tests CASE 4 of lease out of date
    Thread.sleep(LINGER);
    LeaseAttemptStatus fourthLaunchStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchDagAction, eventTimeMillis, false, true);
    Assert.assertTrue(fourthLaunchStatus instanceof LeaseAttemptStatus.LeaseObtainedStatus);
    LeaseAttemptStatus.LeaseObtainedStatus fourthObtainedStatus =
        (LeaseAttemptStatus.LeaseObtainedStatus) fourthLaunchStatus;
    Assert.assertTrue(fourthObtainedStatus.getEventTimeMillis() > eventTimeMillis + LINGER);
    Assert.assertTrue(fourthObtainedStatus.getEventTimeMillis()
        <= fourthObtainedStatus.getLeaseAcquisitionTimestamp());

    // Tests CASE 5 of no longer leasing the same event in DB
    // done immediately after previous lease obtainment so should be marked as the same event
    Assert.assertTrue(mysqlMultiActiveLeaseArbiter.recordLeaseSuccess(fourthObtainedStatus));
    Assert.assertTrue(System.currentTimeMillis() - fourthObtainedStatus.getEventTimeMillis() < EPSILON);
    LeaseAttemptStatus fifthLaunchStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchDagAction, eventTimeMillis, false, true);
    Assert.assertTrue(fifthLaunchStatus instanceof LeaseAttemptStatus.NoLongerLeasingStatus);

    // Tests CASE 6 of no longer leasing a distinct event in DB
    // Wait so this event is considered distinct and a new lease will be acquired
    Thread.sleep(MORE_THAN_EPSILON);
    LeaseAttemptStatus sixthLaunchStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchDagAction, eventTimeMillis, false, true);
    Assert.assertTrue(sixthLaunchStatus instanceof LeaseAttemptStatus.LeaseObtainedStatus);
    LeaseAttemptStatus.LeaseObtainedStatus sixthObtainedStatus =
        (LeaseAttemptStatus.LeaseObtainedStatus) sixthLaunchStatus;
    Assert.assertTrue(sixthObtainedStatus.getEventTimeMillis()
        <= sixthObtainedStatus.getLeaseAcquisitionTimestamp());
  }

  /*
     Tests attemptLeaseIfNewRow() method to ensure a new row is inserted if no row matches the primary key in the table.
     If such a row does exist, the method should disregard the resulting SQL error and return 0 rows updated, indicating
     the lease was not acquired.
     Note: this isolates and tests CASE 1 in which another participant could have acquired the lease between the time
     the read was done and subsequent write was carried out
  */
  @Test (dependsOnMethods = "testAcquireLeaseSingleParticipant")
  public void testAcquireLeaseIfNewRow() throws IOException {
    // Inserting the first time should update 1 row
    Assert.assertEquals(mysqlMultiActiveLeaseArbiter.attemptLeaseIfNewRow(resumeDagAction), 1);
    // Inserting the second time should not update any rows
    Assert.assertEquals(mysqlMultiActiveLeaseArbiter.attemptLeaseIfNewRow(resumeDagAction), 0);
  }

    /*
    Tests CONDITIONALLY_ACQUIRE_LEASE_IF_MATCHING_ALL_COLS_STATEMENT to ensure insertion is not completed if another
    participant updated the table between the prior reed and attempted insertion.
    Note: this isolates and tests CASE 4 in which a flow action event has an out of date lease, so a participant
    attempts a new one given the table the eventTimestamp and leaseAcquisitionTimestamp values are unchanged.
   */
  @Test (dependsOnMethods = "testAcquireLeaseIfNewRow")
  public void testConditionallyAcquireLeaseIfFMatchingAllColsStatement() throws IOException {
    MysqlMultiActiveLeaseArbiter.SelectInfoResult selectInfoResult =
        mysqlMultiActiveLeaseArbiter.getRowInfo(resumeDagAction);

    // The following insert will fail since the eventTimestamp does not match
    int numRowsUpdated = mysqlMultiActiveLeaseArbiter.attemptLeaseIfExistingRow(
        formattedAcquireLeaseIfMatchingAllStatement, resumeDagAction, true, true,
        dummyTimestamp, new Timestamp(selectInfoResult.getLeaseAcquisitionTimeMillis().get()));
    Assert.assertEquals(numRowsUpdated, 0);

    // The following insert will fail since the leaseAcquisitionTimestamp does not match
    numRowsUpdated = mysqlMultiActiveLeaseArbiter.attemptLeaseIfExistingRow(
        formattedAcquireLeaseIfMatchingAllStatement, resumeDagAction, true, true,
        new Timestamp(selectInfoResult.getEventTimeMillis()), dummyTimestamp);
    Assert.assertEquals(numRowsUpdated, 0);

    // This insert should work since the values match all the columns
    numRowsUpdated = mysqlMultiActiveLeaseArbiter.attemptLeaseIfExistingRow(
        formattedAcquireLeaseIfMatchingAllStatement, resumeDagAction, true, true,
        new Timestamp(selectInfoResult.getEventTimeMillis()),
        new Timestamp(selectInfoResult.getLeaseAcquisitionTimeMillis().get()));
    Assert.assertEquals(numRowsUpdated, 1);
  }

  /*
  Tests CONDITIONALLY_ACQUIRE_LEASE_IF_FINISHED_LEASING_STATEMENT to ensure the insertion will only succeed if another
  participant has not updated the eventTimestamp state since the prior read.
  Note: This isolates and tests CASE 6 during which current participant saw a distinct flow action event had completed
  its prior lease, encouraging the current participant to acquire a lease for its event.
   */
  @Test (dependsOnMethods = "testConditionallyAcquireLeaseIfFMatchingAllColsStatement")
  public void testConditionallyAcquireLeaseIfFinishedLeasingStatement()
      throws IOException, InterruptedException, SQLException {
    // Mark the resume action lease from above as completed by fabricating a LeaseObtainedStatus
    MysqlMultiActiveLeaseArbiter.SelectInfoResult selectInfoResult =
        mysqlMultiActiveLeaseArbiter.getRowInfo(resumeDagAction);
    DagActionStore.DagAction updatedResumeDagAction = resumeDagAction.updateFlowExecutionId(
        selectInfoResult.getEventTimeMillis());
    boolean markedSuccess = mysqlMultiActiveLeaseArbiter.recordLeaseSuccess(new LeaseAttemptStatus.LeaseObtainedStatus(
        updatedResumeDagAction, selectInfoResult.getLeaseAcquisitionTimeMillis().get(), LINGER, null));
    Assert.assertTrue(markedSuccess);
    // Ensure no NPE results from calling this after a lease has been completed and acquisition timestamp val is NULL
    mysqlMultiActiveLeaseArbiter.evaluateStatusAfterLeaseAttempt(1, resumeDagAction,
        Optional.empty(), false, true);

    // The following insert will fail since eventTimestamp does not match the expected
    int numRowsUpdated = mysqlMultiActiveLeaseArbiter.attemptLeaseIfExistingRow(
        formattedAcquireLeaseIfFinishedStatement, resumeDagAction, true, false,
        dummyTimestamp, null);
    Assert.assertEquals(numRowsUpdated, 0);

    // This insert does match since we utilize the right eventTimestamp
    numRowsUpdated = mysqlMultiActiveLeaseArbiter.attemptLeaseIfExistingRow(
        formattedAcquireLeaseIfFinishedStatement, resumeDagAction, true, false,
        new Timestamp(selectInfoResult.getEventTimeMillis()), null);
    Assert.assertEquals(numRowsUpdated, 1);
  }

  /*
  Tests calling `tryAcquireLease` for an older reminder event which should be immediately returned as `NoLongerLeasing`
   */
  @Test (dependsOnMethods = "testConditionallyAcquireLeaseIfFinishedLeasingStatement")
  public void testOlderReminderEventAcquireLease() throws IOException {
    // Read database to obtain existing db eventTimeMillis and use it to construct an older event
    MysqlMultiActiveLeaseArbiter.SelectInfoResult selectInfoResult =
        mysqlMultiActiveLeaseArbiter.getRowInfo(resumeDagAction);
    long olderEventTimestamp = selectInfoResult.getEventTimeMillis() - 1;
    LeaseAttemptStatus attemptStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(resumeDagAction, olderEventTimestamp, true, true);
    Assert.assertTrue(attemptStatus instanceof LeaseAttemptStatus.NoLongerLeasingStatus);
  }

  /*
  Tests calling `tryAcquireLease` for a reminder event for which a valid lease exists in the database. We don't expect
  this case to occur because the reminderEvent should be triggered after the lease expires, but ensure it's handled
  correctly anyway.
   */
  @Test (dependsOnMethods = "testOlderReminderEventAcquireLease")
  public void testReminderEventAcquireLeaseOnValidLease() throws IOException {
    // Read database to obtain existing db eventTimeMillis and re-use it for the reminder event time
    MysqlMultiActiveLeaseArbiter.SelectInfoResult selectInfoResult =
        mysqlMultiActiveLeaseArbiter.getRowInfo(resumeDagAction);
    LeaseAttemptStatus attemptStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(resumeDagAction, selectInfoResult.getEventTimeMillis(), true, true);
    Assert.assertTrue(attemptStatus instanceof LeaseAttemptStatus.LeasedToAnotherStatus);
    LeaseAttemptStatus.LeasedToAnotherStatus leasedToAnotherStatus = (LeaseAttemptStatus.LeasedToAnotherStatus) attemptStatus;
    Assert.assertEquals(leasedToAnotherStatus.getEventTimeMillis(), selectInfoResult.getEventTimeMillis());
  }

  /*
  Tests calling `tryAcquireLease` for a reminder event whose lease has expired in the database and should successfully
  acquire a new lease
   */
  @Test (dependsOnMethods = "testReminderEventAcquireLeaseOnValidLease")
  public void testReminderEventAcquireLeaseOnInvalidLease() throws IOException, InterruptedException {
    // Read database to obtain existing db eventTimeMillis and wait enough time for the lease to expire
    MysqlMultiActiveLeaseArbiter.SelectInfoResult selectInfoResult =
        mysqlMultiActiveLeaseArbiter.getRowInfo(resumeDagAction);
    Thread.sleep(MORE_THAN_LINGER);
    LeaseAttemptStatus attemptStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(resumeDagAction, selectInfoResult.getEventTimeMillis(), true, true);
    Assert.assertTrue(attemptStatus instanceof LeaseAttemptStatus.LeaseObtainedStatus);
    LeaseAttemptStatus.LeaseObtainedStatus obtainedStatus = (LeaseAttemptStatus.LeaseObtainedStatus) attemptStatus;
    Assert.assertTrue(obtainedStatus.getEventTimeMillis() > selectInfoResult.getEventTimeMillis());
    Assert.assertTrue(obtainedStatus.getLeaseAcquisitionTimestamp() > selectInfoResult.getLeaseAcquisitionTimeMillis().get().longValue());
  }

   /*
  Tests calling `tryAcquireLease` for a reminder event whose lease has completed in the database and should return
  `NoLongerLeasing` status.
  Note: that we wait for enough time to pass that the event would have been considered distinct for a non-reminder case
  to ensure that the comparison made for reminder events is against the preserved event time not current time in db
   */
   @Test (dependsOnMethods = "testReminderEventAcquireLeaseOnInvalidLease")
   public void testReminderEventAcquireLeaseOnCompletedLease() throws IOException, InterruptedException {
     // Mark the resume action lease from above as completed by fabricating a LeaseObtainedStatus
     MysqlMultiActiveLeaseArbiter.SelectInfoResult selectInfoResult =
         mysqlMultiActiveLeaseArbiter.getRowInfo(resumeDagAction);
     DagActionStore.DagAction updatedResumeDagAction = resumeDagAction.updateFlowExecutionId(
         selectInfoResult.getEventTimeMillis());
     boolean markedSuccess = mysqlMultiActiveLeaseArbiter.recordLeaseSuccess(new LeaseAttemptStatus.LeaseObtainedStatus(
         updatedResumeDagAction, selectInfoResult.getLeaseAcquisitionTimeMillis().get(), LINGER, null));
     Assert.assertTrue(markedSuccess);

     // Sleep enough time for the event to have been considered distinct
     Thread.sleep(MORE_THAN_EPSILON);
     // Now have a reminder event check-in on the completed lease
     LeaseAttemptStatus attemptStatus =
         mysqlMultiActiveLeaseArbiter.tryAcquireLease(resumeDagAction, selectInfoResult.getEventTimeMillis(), true, true);
     Assert.assertTrue(attemptStatus instanceof LeaseAttemptStatus.NoLongerLeasingStatus);
   }

   /*
   Tests calling `tryAcquireLease` when `adoptConsensusFlowExecutionId` is set to True and verify that flowExecutionId
   returned is the same as flowExecutionId provided to it for a LeaseObtainedStatus and LeasedToAnotherStatus object
   (CASE 1 & 2).
   */
  @Test
  public void testSkipAdoptingConsensusFlowExecutionId() throws IOException {
    // Obtain a lease for a new action and verify its flowExecutionId is not updated
    LeaseAttemptStatus firstLaunchStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchDagAction2, eventTimeMillis, false, false);
    Assert.assertTrue(firstLaunchStatus instanceof LeaseAttemptStatus.LeaseObtainedStatus);
    LeaseAttemptStatus.LeaseObtainedStatus firstObtainedStatus =
        (LeaseAttemptStatus.LeaseObtainedStatus) firstLaunchStatus;
    Assert.assertTrue(firstObtainedStatus.getEventTimeMillis() <= firstObtainedStatus.getLeaseAcquisitionTimestamp());
    Assert.assertTrue(firstObtainedStatus.getDagAction().equals(
        new DagActionStore.DagAction(flowGroup2, flowName, flowExecutionId, jobName, DagActionStore.DagActionType.LAUNCH)));

    // A second attempt to obtain a lease on the same action should return a LeasedToAnotherStatus which also contains
    // the original flowExecutionId
    LeaseAttemptStatus secondLaunchStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchDagAction2, eventTimeMillis, false, false);
    Assert.assertTrue(secondLaunchStatus instanceof LeaseAttemptStatus.LeasedToAnotherStatus);
    LeaseAttemptStatus.LeasedToAnotherStatus secondLeasedToAnotherStatus =
        (LeaseAttemptStatus.LeasedToAnotherStatus) secondLaunchStatus;
    Assert.assertEquals(firstObtainedStatus.getEventTimeMillis(), secondLeasedToAnotherStatus.getEventTimeMillis());
    Assert.assertTrue(firstObtainedStatus.getDagAction().equals(
        new DagActionStore.DagAction(flowGroup2, flowName, flowExecutionId, jobName, DagActionStore.DagActionType.LAUNCH)));
  }
}
