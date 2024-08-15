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
import java.util.concurrent.TimeoutException;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.exception.NonTransientException;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.proc.DagProc;
import org.apache.gobblin.service.modules.orchestration.task.DagProcessingEngineMetrics;
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
  private DagTaskStream dagTaskStream;
  // Field is static because it's used to instantiate every MockedDagTask
  private static MySqlDagManagementStateStore dagManagementStateStore;
  private static DagProcessingEngineMetrics mockedDagProcEngineMetrics;
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

    ConfigBuilder configBuilder = ConfigBuilder.create();
    configBuilder.addPrimitive(MySqlDagManagementStateStore.DAG_STATESTORE_CLASS_KEY, MysqlDagStateStoreTest.TestMysqlDagStateStore.class.getName())
        .addPrimitive(MysqlUserQuotaManager.qualify(ConfigurationKeys.STATE_STORE_DB_URL_KEY), testMetastoreDatabase.getJdbcUrl())
        .addPrimitive(MysqlUserQuotaManager.qualify(ConfigurationKeys.STATE_STORE_DB_USER_KEY), TEST_USER)
        .addPrimitive(MysqlUserQuotaManager.qualify(ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY), TEST_PASSWORD)
        .addPrimitive(MysqlUserQuotaManager.qualify(ConfigurationKeys.STATE_STORE_DB_TABLE_KEY), TEST_TABLE)
        .addPrimitive(ServiceConfigKeys.DAG_PROC_ENGINE_NON_RETRYABLE_EXCEPTIONS_KEY, NonTransientException.class.getName());

    Config config = configBuilder.build();

    dagManagementStateStore = spy(MySqlDagManagementStateStoreTest.getDummyDMSS(testMetastoreDatabase));
    doReturn(true).when(dagActionStore).deleteDagAction(any());
    DagProcFactory dagProcFactory = new DagProcFactory(ConfigFactory.empty(), null);

    this.dagTaskStream = spy(new MockedDagTaskStream());
    DagProcessingEngine dagProcessingEngine = new DagProcessingEngine(config, dagTaskStream, dagProcFactory,
            dagManagementStateStore, 100000L, mock(DagProcessingEngineMetrics.class));
    dagProcessingEngine.startAsync();
    mockedDagProcEngineMetrics = mock(DagProcessingEngineMetrics.class);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() throws IOException {
    // `.close()` to avoid (in the aggregate, across multiple suites) - java.sql.SQLNonTransientConnectionException: Too many connections
    testMetastoreDatabase.close();
  }

  static class MockedDagTaskStream implements DagTaskStream {
    public static final int MAX_NUM_OF_TASKS = 1000;
    public static final int FAILING_DAGS_FREQUENCY = 10;
    // this number should be a multiple of FAILING_DAGS_FREQUENCY, so that all non-retryable exceptions should also be exceptions
    public static final int FAILING_DAGS_WITH_NON_RETRYABLE_EXCEPTIONS_FREQUENCY = 30;
    volatile int i = 0;

    @Override
    public boolean hasNext() {
      return true;
    }

    @Override
    public synchronized DagTask next()
        throws NoSuchElementException {
      i++;
      if (i > MAX_NUM_OF_TASKS) {
        throw new RuntimeException("Simulating an exception to stop the thread!");
      }
      if (i % FAILING_DAGS_WITH_NON_RETRYABLE_EXCEPTIONS_FREQUENCY == 0) {
        return new MockedDagTask(new DagActionStore.DagAction("fg-" + i, "fn-" + i, (1234L + i), "jn-" + i,
            DagActionStore.DagActionType.LAUNCH), ExceptionType.NON_RETRYABLE_EXCEPTION);
      } else {
        if (i % FAILING_DAGS_FREQUENCY == 0) {
          return new MockedDagTask(new DagActionStore.DagAction("fg-" + i, "fn-" + i, (1234L + i), "jn-" + i,
              DagActionStore.DagActionType.LAUNCH), ExceptionType.RETRYABLE_EXCEPTION);
        } else {
          return new MockedDagTask(new DagActionStore.DagAction("fg-" + i, "fn-" + i, (1234L + i), "jn-" + i,
              DagActionStore.DagActionType.LAUNCH), ExceptionType.NO_EXCEPTION);
        }
      }
    }

    static class MockedDagTask extends DagTask {
      private final ExceptionType exceptionType;

      public MockedDagTask(DagActionStore.DagAction dagAction, ExceptionType exceptionType) {
        super(dagAction, leaseObtainedStatus, DagProcessingEngineTest.dagManagementStateStore,
            mockedDagProcEngineMetrics);
        this.exceptionType = exceptionType;
      }

      @Override
      public <T> T host(DagTaskVisitor<T> visitor) {
        return (T) new MockedDagProc(this, this.exceptionType);
      }
    }

    static class MockedDagProc extends DagProc<Void> {
      private final ExceptionType exceptionType;

      public MockedDagProc(MockedDagTask mockedDagTask, ExceptionType exceptionType) {
        super(mockedDagTask, ConfigBuilder.create()
            .addPrimitive(ServiceConfigKeys.DAG_PROC_ENGINE_NON_RETRYABLE_EXCEPTIONS_KEY, NonTransientException.class.getName())
            .build());
        this.exceptionType = exceptionType;
      }

      @Override
      public Dag.DagId getDagId() {
        return new Dag.DagId("fg", "fn", 12345L);
      }

      @Override
      protected Void initialize(DagManagementStateStore dagManagementStateStore) {
        return null;
      }

      @Override
      protected void act(DagManagementStateStore dagManagementStateStore, Void state,
          DagProcessingEngineMetrics dagProcEngineMetrics) {

        switch (this.exceptionType) {
          case NON_RETRYABLE_EXCEPTION:
            throw new NonTransientException("Simulating a non retryable exception!");
          case RETRYABLE_EXCEPTION:
            throw new RuntimeException("Simulating an exception!");
          default:
        }
      }
    }
  }

  // This tests verifies that all the dag tasks entered to the dag task stream are retrieved by dag proc engine threads
  @Test
  public void dagProcessingTest()
      throws InterruptedException, TimeoutException {

    // there are MAX_NUM_OF_TASKS dag tasks returned and then each thread additionally call (infinitely) once to wait
    // in this unit tests, it does not infinitely wait though, because the mocked task stream throws an exception on
    // (MAX_NUM_OF_TASKS + 1) th call
    int expectedNumOfInvocations = MockedDagTaskStream.MAX_NUM_OF_TASKS + ServiceConfigKeys.DEFAULT_NUM_DAG_PROC_THREADS;
    int expectedExceptions = MockedDagTaskStream.MAX_NUM_OF_TASKS / MockedDagTaskStream.FAILING_DAGS_FREQUENCY;
    int expectedNonRetryableExceptions = MockedDagTaskStream.MAX_NUM_OF_TASKS / MockedDagTaskStream.FAILING_DAGS_WITH_NON_RETRYABLE_EXCEPTIONS_FREQUENCY;

    AssertWithBackoff.assertTrue(input -> Mockito.mockingDetails(this.dagTaskStream).getInvocations().size() == expectedNumOfInvocations,
        10000L, "dagTaskStream was not called " + expectedNumOfInvocations + " number of times. "
            + "Actual number of invocations " + Mockito.mockingDetails(this.dagTaskStream).getInvocations().size(),
        log, 1, 1000L);

    Assert.assertEquals(dagManagementStateStore.getDagManagerMetrics().dagProcessingExceptionMeter.getCount(),  expectedExceptions);
    Assert.assertEquals(dagManagementStateStore.getDagManagerMetrics().dagProcessingNonRetryableExceptionMeter.getCount(),  expectedNonRetryableExceptions);
  }

  private enum ExceptionType {
    NO_EXCEPTION,
    RETRYABLE_EXCEPTION,
    NON_RETRYABLE_EXCEPTION
  }
}
