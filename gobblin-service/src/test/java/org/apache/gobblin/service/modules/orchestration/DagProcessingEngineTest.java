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

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.orchestration.proc.DagProc;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.testing.AssertWithBackoff;

import static org.mockito.Mockito.spy;

@Slf4j
public class DagProcessingEngineTest {
  private static final String TEST_USER = "testUser";
  private static final String TEST_PASSWORD = "testPassword";
  private static final String TEST_TABLE = "quotas";
  static ITestMetastoreDatabase testMetastoreDatabase;
  DagProcessingEngine.DagProcEngineThread dagProcEngineThread;
  DagManagementTaskStreamImpl dagManagementTaskStream;
  DagProcessingEngine dagProcessingEngine;
  DagTaskStream dagTaskStream;
  DagProcFactory dagProcFactory;

  @BeforeClass
  public void setUp() throws Exception {
    // Setting up mock DB
    testMetastoreDatabase = TestMetastoreDatabaseFactory.get();

    Config config;
    ConfigBuilder configBuilder = ConfigBuilder.create();
    configBuilder.addPrimitive(MostlyMySqlDagManagementStateStore.DAG_STATESTORE_CLASS_KEY, MostlyMySqlDagManagementStateStoreTest.TestMysqlDagStateStore.class.getName())
        .addPrimitive(MysqlUserQuotaManager.qualify(ConfigurationKeys.STATE_STORE_DB_URL_KEY), testMetastoreDatabase.getJdbcUrl())
        .addPrimitive(MysqlUserQuotaManager.qualify(ConfigurationKeys.STATE_STORE_DB_USER_KEY), TEST_USER)
        .addPrimitive(MysqlUserQuotaManager.qualify(ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY), TEST_PASSWORD)
        .addPrimitive(MysqlUserQuotaManager.qualify(ConfigurationKeys.STATE_STORE_DB_TABLE_KEY), TEST_TABLE);
    config = configBuilder.build();

    // Constructing TopologySpecMap.
    Map<URI, TopologySpec> topologySpecMap = new HashMap<>();
    String specExecInstance = "mySpecExecutor";
    TopologySpec topologySpec = DagTestUtils.buildNaiveTopologySpec(specExecInstance);
    URI specExecURI = new URI(specExecInstance);
    topologySpecMap.put(specExecURI, topologySpec);
    MostlyMySqlDagManagementStateStore dagManagementStateStore = new MostlyMySqlDagManagementStateStore(config, null, null);
    dagManagementStateStore.setTopologySpecMap(topologySpecMap);
    this.dagManagementTaskStream =
        new DagManagementTaskStreamImpl(config, Optional.empty());
    this.dagProcFactory = new DagProcFactory(null);
    this.dagProcEngineThread = new DagProcessingEngine.DagProcEngineThread(
        this.dagManagementTaskStream, this.dagProcFactory, dagManagementStateStore);
    this.dagTaskStream = spy(new MockedDagTaskStream());
    this.dagProcessingEngine =
        new DagProcessingEngine(config, dagTaskStream, this.dagProcFactory, dagManagementStateStore);
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
        return new MockedDagTask(new DagActionStore.DagAction("fg-" + i, "fn-" + i, "1234" + i, DagActionStore.FlowActionType.LAUNCH), true);
      } else {
        return new MockedDagTask(new DagActionStore.DagAction("fg-" + i, "fn-" + i, "1234" + i, DagActionStore.FlowActionType.LAUNCH), false);
      }
    }
  }

  static class MockedDagTask extends DagTask {
    private final boolean isBad;

    public MockedDagTask(DagActionStore.DagAction dagAction, boolean isBad) {
      super(dagAction, null);
      this.isBad = isBad;
    }

    @Override
    public <T> T host(DagTaskVisitor<T> visitor) {
      return (T) new MockedDagProc(this.isBad);
    }

    @Override
    public boolean conclude() {
      return false;
    }
  }

  static class MockedDagProc extends DagProc<Void, Void> {
    private final boolean isBad;
    public MockedDagProc(boolean isBad) {
      this.isBad = isBad;
    }

    @Override
    protected Void initialize(DagManagementStateStore dagManagementStateStore) {
      return null;
    }

    @Override
    protected Void act(DagManagementStateStore dagManagementStateStore, Void state) {
      if (this.isBad) {
        throw new RuntimeException("Simulating an exception!");
      }
      return null;
    }

    @Override
    protected void sendNotification(Void result, EventSubmitter eventSubmitter) {
    }

    @Override
    protected void commit(DagManagementStateStore dagManagementStateStore, Void result) {
    }
  }

  // This tests verifies that all the dag tasks entered to the dag task stream are retrieved by dag proc engine threads
  @Test
  public void dagProcessingTest() throws InterruptedException, TimeoutException {
    // there are MAX_NUM_OF_TASKS dag tasks returned and then each thread additionally call (infinitely) once to wait
    // in this unit tests, it does not infinitely wait though, because the mocked task stream throws an exception on
    // (MAX_NUM_OF_TASKS + 1) th call
    int expectedNumOfInvocations = MockedDagTaskStream.MAX_NUM_OF_TASKS + ServiceConfigKeys.DEFAULT_NUM_DAG_PROC_THREADS;
    int expectedExceptions = MockedDagTaskStream.MAX_NUM_OF_TASKS / MockedDagTaskStream.FAILING_DAGS_FREQUENCY;

    AssertWithBackoff.assertTrue(input -> Mockito.mockingDetails(this.dagTaskStream).getInvocations().size() == expectedNumOfInvocations,
        10000L, "dagTaskStream was not called " + expectedNumOfInvocations + " number of times",
        log, 1, 1000L);

    Assert.assertEquals(DagManagementTaskStreamImpl.getDagManagerMetrics().dagProcessingExceptionMeter.getCount(),  expectedExceptions);
  }
}
