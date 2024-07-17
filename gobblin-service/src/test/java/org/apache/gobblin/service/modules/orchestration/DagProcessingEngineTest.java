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
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import org.mockito.Mockito;
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
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.orchestration.proc.DagProc;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.testing.AssertWithBackoff;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

@Slf4j
public class DagProcessingEngineTest {
  private static final String TEST_USER = "testUser";
  private static final String TEST_PASSWORD = "testPassword";
  private static final String TEST_TABLE = "quotas";
  private DagManagementTaskStreamImpl dagManagementTaskStream;
  private DagTaskStream dagTaskStream;
  private DagProcFactory dagProcFactory;
  private static MostlyMySqlDagManagementStateStore testDagManagementStateStore;
  private ITestMetastoreDatabase testMetastoreDatabase;
  static LeaseAttemptStatus.LeaseObtainedStatus leaseObtainedStatus;

  @BeforeClass
  public void setUp() throws Exception {
    // Setting up mock DB
    testMetastoreDatabase = TestMetastoreDatabaseFactory.get();
    DagActionStore dagActionStore = mock(DagActionStore.class);
    doReturn(true).when(dagActionStore).deleteDagAction(any());
    leaseObtainedStatus = mock(LeaseAttemptStatus.LeaseObtainedStatus.class);
    doReturn(true).when(leaseObtainedStatus).completeLease();

    Config config;
    ConfigBuilder configBuilder = ConfigBuilder.create();
    configBuilder.addPrimitive(MostlyMySqlDagManagementStateStore.DAG_STATESTORE_CLASS_KEY, MysqlDagStateStoreTest.TestMysqlDagStateStore.class.getName())
        .addPrimitive(MysqlUserQuotaManager.qualify(ConfigurationKeys.STATE_STORE_DB_URL_KEY), testMetastoreDatabase.getJdbcUrl())
        .addPrimitive(MysqlUserQuotaManager.qualify(ConfigurationKeys.STATE_STORE_DB_USER_KEY), TEST_USER)
        .addPrimitive(MysqlUserQuotaManager.qualify(ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY), TEST_PASSWORD)
        .addPrimitive(MysqlUserQuotaManager.qualify(ConfigurationKeys.STATE_STORE_DB_TABLE_KEY), TEST_TABLE);
    config = configBuilder.build();

    testDagManagementStateStore = spy(MostlyMySqlDagManagementStateStoreTest.getDummyDMSS(testMetastoreDatabase));
    doReturn(true).when(dagActionStore).deleteDagAction(any());
    dagManagementTaskStream =
        new DagManagementTaskStreamImpl(config, Optional.of(mock(DagActionStore.class)),
            mock(MultiActiveLeaseArbiter.class), Optional.of(mock(DagActionReminderScheduler.class)), false,
            testDagManagementStateStore);
    this.dagProcFactory = new DagProcFactory(null);

    DagProcessingEngine.DagProcEngineThread dagProcEngineThread =
        new DagProcessingEngine.DagProcEngineThread(dagManagementTaskStream, this.dagProcFactory,
            testDagManagementStateStore, 0);
    this.dagTaskStream = spy(new MockedDagTaskStream());
    DagProcessingEngine dagProcessingEngine =
        new DagProcessingEngine(config, Optional.ofNullable(dagTaskStream), Optional.ofNullable(this.dagProcFactory),
            Optional.ofNullable(testDagManagementStateStore), 100000L);
    dagProcessingEngine.startAsync();
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() throws IOException {
    // `.close()` to avoid (in the aggregate, across multiple suites) - java.sql.SQLNonTransientConnectionException: Too many connections
    testMetastoreDatabase.close();
  }

  static class MockedDagTaskStream implements DagTaskStream {
    public static final int MAX_NUM_OF_TASKS = 1000;
    public static final int FAILING_DAGS_FREQUENCY = 10;
    volatile int i=0;

    @Override
    public boolean hasNext() {
      return true;
    }

    @Override
    public synchronized DagTask next() throws NoSuchElementException {
      i++;
      if (i > MAX_NUM_OF_TASKS) {
        throw new RuntimeException("Simulating an exception to stop the thread!");
      }
      if (i % FAILING_DAGS_FREQUENCY == 0 ) {
        return new MockedDagTask(new DagActionStore.DagAction("fg-" + i, "fn-" + i, (1234L + i), "jn-" + i, DagActionStore.DagActionType.LAUNCH), true);
      } else {
        return new MockedDagTask(new DagActionStore.DagAction("fg-" + i, "fn-" + i, (1234L + i), "jn-" + i, DagActionStore.DagActionType.LAUNCH), false);
      }
    }
  }

  static class MockedDagTask extends DagTask {
    private final boolean isBad;

    public MockedDagTask(DagActionStore.DagAction dagAction, boolean isBad) {
      super(dagAction, leaseObtainedStatus, testDagManagementStateStore);
      this.isBad = isBad;
    }

    @Override
    public <T> T host(DagTaskVisitor<T> visitor) {
      return (T) new MockedDagProc(this, this.isBad);
    }
  }

  static class MockedDagProc extends DagProc<Void> {
    private final boolean isBad;

    public MockedDagProc(MockedDagTask mockedDagTask, boolean isBad) {
      super(mockedDagTask);
      this.isBad = isBad;
    }

    @Override
    public DagManager.DagId getDagId() {
      return new DagManager.DagId("fg", "fn", 12345L);
    }

    @Override
    protected Void initialize(DagManagementStateStore dagManagementStateStore) {
      return null;
    }

    @Override
    protected void act(DagManagementStateStore dagManagementStateStore, Void state) {
      if (this.isBad) {
        throw new RuntimeException("Simulating an exception!");
      }
    }
  }

  // This tests verifies that all the dag tasks entered to the dag task stream are retrieved by dag proc engine threads
  @Test
  public void dagProcessingTest()
      throws InterruptedException, TimeoutException, IOException {

    // there are MAX_NUM_OF_TASKS dag tasks returned and then each thread additionally call (infinitely) once to wait
    // in this unit tests, it does not infinitely wait though, because the mocked task stream throws an exception on
    // (MAX_NUM_OF_TASKS + 1) th call
    int expectedNumOfInvocations = MockedDagTaskStream.MAX_NUM_OF_TASKS + ServiceConfigKeys.DEFAULT_NUM_DAG_PROC_THREADS;
    int expectedExceptions = MockedDagTaskStream.MAX_NUM_OF_TASKS / MockedDagTaskStream.FAILING_DAGS_FREQUENCY;

    AssertWithBackoff.assertTrue(input -> Mockito.mockingDetails(this.dagTaskStream).getInvocations().size() == expectedNumOfInvocations,
        10000L, "dagTaskStream was not called " + expectedNumOfInvocations + " number of times. "
            + "Actual number of invocations " + Mockito.mockingDetails(this.dagTaskStream).getInvocations().size(),
        log, 1, 1000L);

    Assert.assertEquals(testDagManagementStateStore.getDagManagerMetrics().dagProcessingExceptionMeter.getCount(),  expectedExceptions);
  }
}
