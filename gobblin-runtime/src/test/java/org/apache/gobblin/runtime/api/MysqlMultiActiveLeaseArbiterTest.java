package org.apache.gobblin.runtime.api;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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
  private static DagActionStore.DagAction launchDagAction =
      new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, DagActionStore.FlowActionType.LAUNCH);

  private static final long eventTimeMillis = System.currentTimeMillis();
  private MysqlMultiActiveLeaseArbiter mysqlMultiActiveLeaseArbiter;

  // The setup functionality verifies that the initialization of the tables is done correctly and verifies any SQL
  // syntax errors.
  @BeforeClass
  public void setUp() throws Exception {
    ITestMetastoreDatabase testDb = TestMetastoreDatabaseFactory.get();

    Config config = ConfigBuilder.create()
        .addPrimitive(ConfigurationKeys.MYSQL_LEASE_ARBITER_PREFIX + "." + ConfigurationKeys.SCHEDULER_EVENT_EPSILON_MILLIS_KEY, EPSILON)
        .addPrimitive(ConfigurationKeys.MYSQL_LEASE_ARBITER_PREFIX + "." + ConfigurationKeys.SCHEDULER_EVENT_LINGER_MILLIS_KEY, LINGER)
        .addPrimitive(ConfigurationKeys.MYSQL_LEASE_ARBITER_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_URL_KEY, testDb.getJdbcUrl())
        .addPrimitive(ConfigurationKeys.MYSQL_LEASE_ARBITER_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_USER_KEY, USER)
        .addPrimitive(ConfigurationKeys.MYSQL_LEASE_ARBITER_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, PASSWORD)
        .addPrimitive(ConfigurationKeys.MYSQL_LEASE_ARBITER_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_TABLE_KEY, TABLE)
        .build();

    this.mysqlMultiActiveLeaseArbiter = new MysqlMultiActiveLeaseArbiter(config);
  }

  /*
     Tests all cases of trying to acquire a lease (CASES 1-6 detailed below) for a flow action event with one
     participant involved.
  */
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
    Assert.assertTrue(secondLeasedToAnotherStatus.getMinimumLingerDurationMillis() > LINGER);

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
    Assert.assertTrue(thirdLeasedToAnotherStatus.getMinimumLingerDurationMillis() > LINGER);

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
}
