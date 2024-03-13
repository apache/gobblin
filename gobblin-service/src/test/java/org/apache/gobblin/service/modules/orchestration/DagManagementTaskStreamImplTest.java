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
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.typesafe.config.Config;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.modules.orchestration.proc.DagProc;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.service.modules.orchestration.task.LaunchDagTask;

public class DagManagementTaskStreamImplTest {
  private static final String TEST_USER = "testUser";
  private static final String TEST_PASSWORD = "testPassword";
  private static final String TEST_TABLE = "quotas";
  static ITestMetastoreDatabase testMetastoreDatabase;
  DagProcessingEngine.DagProcEngineThread dagProcEngineThread;
  DagManagementTaskStreamImpl dagManagementTaskStream;
  DagProcFactory dagProcFactory;

  @BeforeClass
  public void setUp() throws Exception {
    // Setting up mock DB
    testMetastoreDatabase = TestMetastoreDatabaseFactory.get();

    ConfigBuilder configBuilder = ConfigBuilder.create();
    configBuilder.addPrimitive(MostlyMySqlDagManagementStateStore.DAG_STATESTORE_CLASS_KEY, MostlyMySqlDagManagementStateStoreTest.TestMysqlDagStateStore.class.getName())
        .addPrimitive(MysqlUserQuotaManager.qualify(ConfigurationKeys.STATE_STORE_DB_URL_KEY), testMetastoreDatabase.getJdbcUrl())
        .addPrimitive(MysqlUserQuotaManager.qualify(ConfigurationKeys.STATE_STORE_DB_USER_KEY), TEST_USER)
        .addPrimitive(MysqlUserQuotaManager.qualify(ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY), TEST_PASSWORD)
        .addPrimitive(MysqlUserQuotaManager.qualify(ConfigurationKeys.STATE_STORE_DB_TABLE_KEY), TEST_TABLE);
    Config config = configBuilder.build();

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
  }

  // This tests adding and removal of dag actions from dag task stream
  // when we have different dag procs in future, we can test dag processing and exception handling
  @Test
  public void addRemoveDagActions() throws IOException {
    dagManagementTaskStream.addDagAction(
        new DagActionStore.DagAction("fg", "fn", "12345", DagActionStore.FlowActionType.LAUNCH));
    DagTask dagTask = dagManagementTaskStream.next();
    Assert.assertTrue(dagTask instanceof LaunchDagTask);
    DagProc dagProc = dagTask.host(this.dagProcFactory);
    Assert.assertNotNull(dagProc);
  }
}
