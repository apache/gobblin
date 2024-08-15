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
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Optional;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.util.ExponentialBackoff;


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
  private static final long flowExecutionId = 12345677L;
  private static final long eventTimeMillis = 1710451837L;
  // Dag actions with the same flow info but different flow action types are considered unique
  private static final DagActionStore.DagAction launchDagAction =
      new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, jobName, DagActionStore.DagActionType.LAUNCH);
  private static final DagActionStore.LeaseParams
      launchLeaseParams = new DagActionStore.LeaseParams(launchDagAction, false, eventTimeMillis);
  private static final DagActionStore.DagAction resumeDagAction =
      new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, jobName, DagActionStore.DagActionType.RESUME);
  private static final DagActionStore.LeaseParams
      resumeLeaseParams = new DagActionStore.LeaseParams(resumeDagAction, false, eventTimeMillis);
  private static final DagActionStore.DagAction launchDagAction2 =
      new DagActionStore.DagAction(flowGroup2, flowName, flowExecutionId, jobName, DagActionStore.DagActionType.LAUNCH);
  private static final DagActionStore.LeaseParams
      launchLeaseParams2 = new DagActionStore.LeaseParams(launchDagAction2, false, eventTimeMillis);
  private static final Timestamp dummyTimestamp = new Timestamp(99999);
  private ITestMetastoreDatabase testDb;
  private MysqlMultiActiveLeaseArbiter mysqlMultiActiveLeaseArbiter;
  private final String formattedAcquireLeaseIfMatchingAllStatement =
      String.format(MysqlMultiActiveLeaseArbiter.CONDITIONALLY_ACQUIRE_LEASE_IF_MATCHING_ALL_COLS_STATEMENT, TABLE);
  private final String formattedAcquireLeaseIfFinishedStatement =
      String.format(MysqlMultiActiveLeaseArbiter.CONDITIONALLY_ACQUIRE_LEASE_IF_FINISHED_LEASING_STATEMENT, TABLE);

  // The setup functionality verifies that the initialization of the tables is done correctly and verifies any SQL
  // syntax errors.
  @BeforeClass
  public void setUp() throws Exception {
    this.testDb = TestMetastoreDatabaseFactory.get();
    this.mysqlMultiActiveLeaseArbiter = new MysqlMultiActiveLeaseArbiter(getLeaseArbiterTestConfigs(this.testDb));
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() throws IOException {
    // `.close()` to avoid (in the aggregate, across multiple suites) - java.sql.SQLNonTransientConnectionException: Too many connections
    this.testDb.close();
  }

  public static Config getLeaseArbiterTestConfigs(ITestMetastoreDatabase testDb) throws URISyntaxException {
    return ConfigBuilder.create()
        .addPrimitive(ConfigurationKeys.SCHEDULER_EVENT_EPSILON_MILLIS_KEY, EPSILON)
        .addPrimitive(ConfigurationKeys.SCHEDULER_EVENT_LINGER_MILLIS_KEY, LINGER)
        .addPrimitive(ConfigurationKeys.MYSQL_LEASE_ARBITER_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_URL_KEY, testDb.getJdbcUrl())
        .addPrimitive(ConfigurationKeys.MYSQL_LEASE_ARBITER_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_USER_KEY, USER)
        .addPrimitive(ConfigurationKeys.MYSQL_LEASE_ARBITER_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, PASSWORD)
        .addPrimitive(ConfigurationKeys.LEASE_DETERMINATION_STORE_DB_TABLE_KEY, TABLE)
        .addPrimitive(ConfigurationKeys.MULTI_ACTIVE_CONSTANTS_DB_TABLE_KEY, CONSTANTS_TABLE)
        .build();
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
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchLeaseParams, true);
    Assert.assertTrue(firstLaunchStatus instanceof LeaseAttemptStatus.LeaseObtainedStatus);
    LeaseAttemptStatus.LeaseObtainedStatus firstObtainedStatus =
        (LeaseAttemptStatus.LeaseObtainedStatus) firstLaunchStatus;
    long consensusEventTimeMillis = firstObtainedStatus.getEventTimeMillis();
    Assert.assertTrue(consensusEventTimeMillis <= firstObtainedStatus.getLeaseAcquisitionTimestamp());
    // Make sure consensusEventTimeMillis is set, and it's not 0 or the original event time
    log.info("consensus event time is {} eventtimeMillis is {}", consensusEventTimeMillis, eventTimeMillis);
    Assert.assertTrue(consensusEventTimeMillis != eventTimeMillis && consensusEventTimeMillis != 0);
    Assert.assertEquals(new DagActionStore.DagAction(flowGroup, flowName, consensusEventTimeMillis, jobName,
        DagActionStore.DagActionType.LAUNCH), firstObtainedStatus.getConsensusDagAction());
    Assert.assertEquals(firstObtainedStatus.getEventTimeMillis(), consensusEventTimeMillis);
    Assert.assertFalse(firstObtainedStatus.getConsensusLeaseParams().isReminder);

    // Verify that different DagAction types for the same flow can have leases at the same time
    DagActionStore.DagAction killDagAction = new
        DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, jobName, DagActionStore.DagActionType.KILL);
    LeaseAttemptStatus killStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(
            new DagActionStore.LeaseParams(killDagAction, false, eventTimeMillis), true);
    Assert.assertTrue(killStatus instanceof LeaseAttemptStatus.LeaseObtainedStatus);
    LeaseAttemptStatus.LeaseObtainedStatus killObtainedStatus =
        (LeaseAttemptStatus.LeaseObtainedStatus) killStatus;
    Assert.assertTrue(
        killObtainedStatus.getLeaseAcquisitionTimestamp() >= killObtainedStatus.getEventTimeMillis());

    // Tests CASE 2 of acquire lease for a flow action event that already has a valid lease for the same event in db
    // Very little time should have passed if this test directly follows the one above so this call will be considered
    // the same as the previous event
    LeaseAttemptStatus secondLaunchStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchLeaseParams, true);
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
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchLeaseParams, true);
    Assert.assertTrue(thirdLaunchStatus instanceof LeaseAttemptStatus.LeasedToAnotherStatus);
    LeaseAttemptStatus.LeasedToAnotherStatus thirdLeasedToAnotherStatus =
        (LeaseAttemptStatus.LeasedToAnotherStatus) thirdLaunchStatus;
    Assert.assertTrue(thirdLeasedToAnotherStatus.getEventTimeMillis() > firstObtainedStatus.getEventTimeMillis());
    Assert.assertTrue(thirdLeasedToAnotherStatus.getMinimumLingerDurationMillis() < LINGER);

    // Tests CASE 4 of lease out of date
    Thread.sleep(LINGER);
    LeaseAttemptStatus fourthLaunchStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchLeaseParams, true);
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
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchLeaseParams, true);
    Assert.assertTrue(fifthLaunchStatus instanceof LeaseAttemptStatus.NoLongerLeasingStatus);

    // Tests CASE 6 of no longer leasing a distinct event in DB
    // Wait so this event is considered distinct and a new lease will be acquired
    Thread.sleep(MORE_THAN_EPSILON);
    LeaseAttemptStatus sixthLaunchStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchLeaseParams, true);
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
  @Test
  public void testAcquireLeaseIfNewRow() throws IOException {
    // Inserting the first time should update 1 row
    Assert.assertEquals(mysqlMultiActiveLeaseArbiter.attemptLeaseIfNewRow(resumeDagAction,
        ExponentialBackoff.builder().maxRetries(MysqlMultiActiveLeaseArbiter.MAX_RETRIES)
            .initialDelay(MysqlMultiActiveLeaseArbiter.MIN_INITIAL_DELAY_MILLIS).build()), 1);
    // Inserting the second time should not update any rows
    Assert.assertEquals(mysqlMultiActiveLeaseArbiter.attemptLeaseIfNewRow(resumeDagAction,
        ExponentialBackoff.builder().maxRetries(MysqlMultiActiveLeaseArbiter.MAX_RETRIES)
            .initialDelay(MysqlMultiActiveLeaseArbiter.MIN_INITIAL_DELAY_MILLIS).build()), 0);
  }

    /*
    Tests CONDITIONALLY_ACQUIRE_LEASE_IF_MATCHING_ALL_COLS_STATEMENT to ensure insertion is not completed if another
    participant updated the table between the prior read and attempted insertion.
    Note: this isolates and tests CASE 4 in which a flow action event has an out of date lease, so a participant
    attempts a new one given the table the eventTimestamp and leaseAcquisitionTimestamp values are unchanged.
   */
  @Test (dependsOnMethods = "testAcquireLeaseIfNewRow")
  public void testConditionallyAcquireLeaseIfMatchingAllColsStatement() throws IOException {
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
  @Test (dependsOnMethods = "testConditionallyAcquireLeaseIfMatchingAllColsStatement")
  public void testConditionallyAcquireLeaseIfFinishedLeasingStatement()
      throws IOException, SQLException {
    MysqlMultiActiveLeaseArbiter.SelectInfoResult selectInfoResult = completeLeaseHelper(resumeLeaseParams);
    // Ensure no NPE results from calling this after a lease has been completed and acquisition timestamp val is NULL
    mysqlMultiActiveLeaseArbiter.evaluateStatusAfterLeaseAttempt(1, resumeLeaseParams,
        Optional.empty(), true);

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
  @Test
  public void testOlderReminderEventAcquireLease() throws IOException {
    DagActionStore.LeaseParams newLaunchLeaseParams = getUniqueLaunchLeaseParams();
    mysqlMultiActiveLeaseArbiter.tryAcquireLease(newLaunchLeaseParams, false);
    // Read database to obtain existing db eventTimeMillis and use it to construct an older event
    MysqlMultiActiveLeaseArbiter.SelectInfoResult selectInfoResult =
        mysqlMultiActiveLeaseArbiter.getRowInfo(newLaunchLeaseParams.getDagAction());
    long olderEventTimestamp = selectInfoResult.getEventTimeMillis() - 1;
    DagActionStore.LeaseParams updatedLeaseParams =
        new DagActionStore.LeaseParams(newLaunchLeaseParams.getDagAction(), true, olderEventTimestamp);
    LeaseAttemptStatus attemptStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(updatedLeaseParams, true);
    Assert.assertTrue(attemptStatus instanceof LeaseAttemptStatus.NoLongerLeasingStatus);
  }

  /*
  Tests calling `tryAcquireLease` for a reminder event for which a valid lease exists in the database. We don't expect
  this case to occur because the reminderEvent should be triggered after the lease expires, but ensure it's handled
  correctly anyway.
   */
  @Test
  public void testReminderEventAcquireLeaseOnValidLease() throws IOException {
    DagActionStore.LeaseParams newLaunchLeaseParams = getUniqueLaunchLeaseParams();
    LeaseAttemptStatus.LeaseObtainedStatus leaseObtainedStatus =
        (LeaseAttemptStatus.LeaseObtainedStatus) mysqlMultiActiveLeaseArbiter.tryAcquireLease(newLaunchLeaseParams, false);
    // Use the consensusLeaseParams containing the new eventTimeMillis for the reminder event time
    DagActionStore.LeaseParams updatedLeaseParams =
        new DagActionStore.LeaseParams(newLaunchLeaseParams.getDagAction(), true,
            leaseObtainedStatus.getEventTimeMillis());
    LeaseAttemptStatus attemptStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(updatedLeaseParams, true);
    Assert.assertTrue(attemptStatus instanceof LeaseAttemptStatus.LeasedToAnotherStatus);
    LeaseAttemptStatus.LeasedToAnotherStatus leasedToAnotherStatus = (LeaseAttemptStatus.LeasedToAnotherStatus) attemptStatus;
    Assert.assertEquals(leasedToAnotherStatus.getEventTimeMillis(), leaseObtainedStatus.getEventTimeMillis());
  }

  /*
  Tests calling `tryAcquireLease` for a reminder event whose lease has expired in the database and should successfully
  acquire a new lease
   */
  @Test
  public void testReminderEventAcquireLeaseOnInvalidLease() throws IOException, InterruptedException {
    DagActionStore.LeaseParams newLaunchLeaseParams = getUniqueLaunchLeaseParams();
    mysqlMultiActiveLeaseArbiter.tryAcquireLease(newLaunchLeaseParams, false);
    MysqlMultiActiveLeaseArbiter.SelectInfoResult selectInfoResult = mysqlMultiActiveLeaseArbiter.getRowInfo(newLaunchLeaseParams.getDagAction());
    // Wait enough time for the lease to expire
    Thread.sleep(MORE_THAN_LINGER);
    LeaseAttemptStatus attemptStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(newLaunchLeaseParams, true);
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
   @Test
   public void testReminderEventAcquireLeaseOnCompletedLease() throws IOException, InterruptedException {
     // Create a new dag action and complete the lease
     DagActionStore.LeaseParams newLaunchLeaseParams = getUniqueLaunchLeaseParams();
     mysqlMultiActiveLeaseArbiter.tryAcquireLease(newLaunchLeaseParams, false);
     MysqlMultiActiveLeaseArbiter.SelectInfoResult selectInfoResult = completeLeaseHelper(newLaunchLeaseParams);

     // Sleep enough time for the event to have been considered distinct
     Thread.sleep(MORE_THAN_EPSILON);
     // Now have a reminder event check-in on the completed lease
     DagActionStore.DagAction updatedDagAction = newLaunchLeaseParams.getDagAction().updateFlowExecutionId(
         selectInfoResult.getEventTimeMillis());
     DagActionStore.LeaseParams updatedLeaseParams =
         new DagActionStore.LeaseParams(updatedDagAction, true, newLaunchLeaseParams.getEventTimeMillis());
     LeaseAttemptStatus attemptStatus =
         mysqlMultiActiveLeaseArbiter.tryAcquireLease(updatedLeaseParams, true);
     Assert.assertTrue(attemptStatus instanceof LeaseAttemptStatus.NoLongerLeasingStatus);
   }

   /*
   Tests calling `tryAcquireLease` when `adoptConsensusFlowExecutionId` is set to False and verify that flowExecutionId
   returned is the same as flowExecutionId provided to it for a LeaseObtainedStatus and LeasedToAnotherStatus object
   (CASE 1 & 2). It also verifies that the `eventTimeMillis` stored in a lease obtained status can be used to complete
   the lease.
   */
  @Test
  public void testSkipAdoptingConsensusFlowExecutionId() throws IOException {
    // Obtain a lease for a new action and verify its flowExecutionId is not updated
    LeaseAttemptStatus firstLaunchStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchLeaseParams2, false);
    Assert.assertTrue(firstLaunchStatus instanceof LeaseAttemptStatus.LeaseObtainedStatus);
    LeaseAttemptStatus.LeaseObtainedStatus firstObtainedStatus =
        (LeaseAttemptStatus.LeaseObtainedStatus) firstLaunchStatus;
    Assert.assertTrue(firstObtainedStatus.getEventTimeMillis() <= firstObtainedStatus.getLeaseAcquisitionTimestamp());
    Assert.assertTrue(firstObtainedStatus.getEventTimeMillis() != Long.valueOf(firstObtainedStatus.getConsensusDagAction().getFlowExecutionId()));
    Assert.assertEquals(new DagActionStore.DagAction(flowGroup2, flowName, flowExecutionId, jobName,
        DagActionStore.DagActionType.LAUNCH), firstObtainedStatus.getConsensusDagAction());
    Assert.assertFalse(firstObtainedStatus.getConsensusLeaseParams().isReminder());

    // A second attempt to obtain a lease on the same action should return a LeasedToAnotherStatus which also contains
    // the original flowExecutionId and the same event time from the previous LeaseAttemptStatus
    LeaseAttemptStatus secondLaunchStatus =
        mysqlMultiActiveLeaseArbiter.tryAcquireLease(launchLeaseParams2, false);
    Assert.assertTrue(secondLaunchStatus instanceof LeaseAttemptStatus.LeasedToAnotherStatus);
    LeaseAttemptStatus.LeasedToAnotherStatus secondLeasedToAnotherStatus =
        (LeaseAttemptStatus.LeasedToAnotherStatus) secondLaunchStatus;
    Assert.assertEquals(firstObtainedStatus.getEventTimeMillis(), secondLeasedToAnotherStatus.getEventTimeMillis());
    Assert.assertEquals(new DagActionStore.DagAction(flowGroup2, flowName, flowExecutionId, jobName,
        DagActionStore.DagActionType.LAUNCH), secondLeasedToAnotherStatus.getConsensusDagAction());
    Assert.assertFalse(firstObtainedStatus.getConsensusLeaseParams().isReminder());

    Assert.assertTrue(mysqlMultiActiveLeaseArbiter.recordLeaseSuccess(firstObtainedStatus));
  }

  public static String generateUniqueName() {
    UUID uuid = UUID.randomUUID();
    return uuid.toString().substring(0, 10);
  }

  /**
   * Returns a unique launch type dagAction event by using 'generateUniqueName' as flowName to create a unique event
   */
  public DagActionStore.DagAction getUniqueLaunchDagAction() {
    return new DagActionStore.DagAction(flowGroup, generateUniqueName(), flowExecutionId, jobName,
        DagActionStore.DagActionType.LAUNCH);
  }

  /**
   * Returns a unique launch type {@link org.apache.gobblin.service.modules.orchestration.DagActionStore.LeaseParams}
   * using #getUniqueLaunchDagAction() to create a unique flowName
   */
  public DagActionStore.LeaseParams getUniqueLaunchLeaseParams() {
    return new DagActionStore.LeaseParams(getUniqueLaunchDagAction(), false, eventTimeMillis);
  }

  /**
   * Marks the lease associated with the dagAction as completed by fabricating a LeaseObtainedStatus
   * @return SelectInfoResult object containing the event information used to complete the lease
   */
  public MysqlMultiActiveLeaseArbiter.SelectInfoResult completeLeaseHelper(
      DagActionStore.LeaseParams previouslyLeasedLeaseParams) throws IOException {
    MysqlMultiActiveLeaseArbiter.SelectInfoResult selectInfoResult =
        mysqlMultiActiveLeaseArbiter.getRowInfo(previouslyLeasedLeaseParams.getDagAction());
    DagActionStore.DagAction updatedDagAction = previouslyLeasedLeaseParams.getDagAction().updateFlowExecutionId(
        selectInfoResult.getEventTimeMillis());
    DagActionStore.LeaseParams
        updatedLeaseParams = new DagActionStore.LeaseParams(updatedDagAction, false, selectInfoResult.getEventTimeMillis());
    boolean markedSuccess = mysqlMultiActiveLeaseArbiter.recordLeaseSuccess(new LeaseAttemptStatus.LeaseObtainedStatus(
        updatedLeaseParams, selectInfoResult.getLeaseAcquisitionTimeMillis().get(), LINGER, null));
    Assert.assertTrue(markedSuccess);
    return selectInfoResult;
  }
}
