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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.runtime.app.ServiceBasedAppLauncher;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.runtime.spec_catalog.TopologyCatalog;
import org.apache.gobblin.runtime.spec_executorInstance.InMemorySpecExecutor;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.GobblinServiceManagerTest;
import org.apache.gobblin.service.modules.flow.IdentityFlowToJobSpecCompiler;
import org.apache.gobblin.service.modules.flow.SpecCompiler;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.utils.FlowCompilationValidationHelper;
import org.apache.gobblin.service.modules.utils.SharedFlowMetricsSingleton;
import org.apache.gobblin.service.monitoring.FlowStatusGenerator;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PathUtils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


public class OrchestratorTest {
  private static final Logger logger = LoggerFactory.getLogger(TopologyCatalog.class);
  private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

  private static final String SPEC_STORE_PARENT_DIR = "/tmp/orchestrator/";
  private static final String SPEC_DESCRIPTION = "Test Orchestrator";
  private static final String SPEC_VERSION = FlowSpec.Builder.DEFAULT_VERSION;
  private static final String TEST_FLOW_GROUP_NAME = "myTestFlowGroup";
  private static final String TOPOLOGY_SPEC_STORE_DIR = "/tmp/orchestrator/topologyTestSpecStore";
  private static final String FLOW_SPEC_STORE_DIR = "/tmp/orchestrator/flowTestSpecStore";
  private static final String FLOW_SPEC_GROUP_DIR = "/tmp/orchestrator/flowTestSpecStore/flowTestGroupDir";

  private ServiceBasedAppLauncher serviceLauncher;
  private TopologyCatalog topologyCatalog;
  private TopologySpec topologySpec;

  private FlowCatalog flowCatalog;
  private FlowSpec flowSpec;

  private ITestMetastoreDatabase testMetastoreDatabase;
  private FlowStatusGenerator mockFlowStatusGenerator;
  private FlowCompilationValidationHelper mockedFlowCompilationValidationHelper;
  private Orchestrator dagMgrNotFlowLaunchHandlerBasedOrchestrator;

  @BeforeClass
  public void setUpClass() throws Exception {
    // PERF: when within `@{Before,After}Class` the 7 current tests take only 33s; when `@{Before,After}Method` `.get()`s a per-test DB, the same take 1m49s!
    this.testMetastoreDatabase = TestMetastoreDatabaseFactory.get();
  }

  @BeforeMethod
  public void setUp() throws Exception {
    cleanUpDir(TOPOLOGY_SPEC_STORE_DIR);
    cleanUpDir(FLOW_SPEC_STORE_DIR);

    Properties orchestratorProperties = new Properties();

    Properties topologyProperties = new Properties();
    topologyProperties.put("specStore.fs.dir", TOPOLOGY_SPEC_STORE_DIR);

    Properties flowProperties = new Properties();
    flowProperties.put("specStore.fs.dir", FLOW_SPEC_STORE_DIR);

    this.serviceLauncher = new ServiceBasedAppLauncher(orchestratorProperties, "OrchestratorCatalogTest");

    this.topologyCatalog = new TopologyCatalog(ConfigUtils.propertiesToConfig(topologyProperties),
        Optional.of(logger));
    this.serviceLauncher.addService(topologyCatalog);

    // Test warm standby flow catalog, which has orchestrator as listener
    this.flowCatalog = new FlowCatalog(ConfigUtils.propertiesToConfig(flowProperties), Optional.of(logger), Optional.absent(), true);

    this.serviceLauncher.addService(flowCatalog);
    this.mockFlowStatusGenerator = mock(FlowStatusGenerator.class);

    MySqlDagManagementStateStore dagManagementStateStore =
        spy(MySqlDagManagementStateStoreTest.getDummyDMSS(this.testMetastoreDatabase));

    SharedFlowMetricsSingleton sharedFlowMetricsSingleton = new SharedFlowMetricsSingleton(ConfigUtils.propertiesToConfig(orchestratorProperties));

    FlowCompilationValidationHelper flowCompilationValidationHelper = new FlowCompilationValidationHelper(ConfigFactory.empty(),
        sharedFlowMetricsSingleton, mock(UserQuotaManager.class), mockFlowStatusGenerator);
    this.dagMgrNotFlowLaunchHandlerBasedOrchestrator = new Orchestrator(ConfigUtils.propertiesToConfig(orchestratorProperties),
        this.topologyCatalog, Optional.of(logger), mock(FlowLaunchHandler.class), sharedFlowMetricsSingleton, dagManagementStateStore,
        flowCompilationValidationHelper, mock(JobStatusRetriever.class));

    /* Initialize a second orchestrator with a mocked flowCompilationValidationHelper to use Mockito to spoof the dag
    returned by a call to compile a flowSpec
    */
    this.mockedFlowCompilationValidationHelper = mock(FlowCompilationValidationHelper.class);
    when(mockedFlowCompilationValidationHelper.getSpecCompiler()).thenReturn(mock(SpecCompiler.class));

    this.topologyCatalog.addListener(dagMgrNotFlowLaunchHandlerBasedOrchestrator);
    this.flowCatalog.addListener(dagMgrNotFlowLaunchHandlerBasedOrchestrator);
    // Start application
    this.serviceLauncher.start();
    // Create Spec to play with
    this.topologySpec = initTopologySpec();
    this.flowSpec = initFlowSpec();
  }

  @AfterMethod
  public void tearDown() throws Exception {
    // Shutdown Catalog
    this.serviceLauncher.stop();

    File specStoreDir = new File(SPEC_STORE_PARENT_DIR);
    if (specStoreDir.exists()) {
      FileUtils.deleteDirectory(specStoreDir);
    }
  }

  @AfterClass(alwaysRun = true)
  public void tearDownClass() throws Exception {
    if (this.testMetastoreDatabase != null) {
      // `.close()` to avoid (in the aggregate, across multiple suites) - java.sql.SQLNonTransientConnectionException: Too many connections
      this.testMetastoreDatabase.close();
    }
  }

  private void cleanUpDir(String dir) throws Exception {
    File specStoreDir = new File(dir);
    if (specStoreDir.exists()) {
      FileUtils.deleteDirectory(specStoreDir);
    }
  }

  private TopologySpec initTopologySpec() {
    Properties properties = new Properties();
    properties.put("specStore.fs.dir", TOPOLOGY_SPEC_STORE_DIR);
    properties.put("specExecInstance.capabilities", "source:destination");
    Config config = ConfigUtils.propertiesToConfig(properties);

    SpecExecutor specExecutorInstance = new InMemorySpecExecutor(config);

    TopologySpec.Builder topologySpecBuilder = TopologySpec.builder(computeTopologySpecURI(SPEC_STORE_PARENT_DIR,
            TOPOLOGY_SPEC_STORE_DIR))
        .withConfig(config)
        .withDescription(SPEC_DESCRIPTION)
        .withVersion(SPEC_VERSION)
        .withSpecExecutor(specExecutorInstance);
    return topologySpecBuilder.build();
  }

  private FlowSpec initFlowSpec() {
    Properties properties = new Properties();
    String flowName = "test_flowName";
    String flowGroup = "test_flowGroup";
    properties.put(ConfigurationKeys.FLOW_NAME_KEY, flowName);
    properties.put(ConfigurationKeys.FLOW_GROUP_KEY, flowGroup);
    properties.put("job.name", flowName);
    properties.put("job.group", flowGroup);
    properties.put("specStore.fs.dir", FLOW_SPEC_STORE_DIR);
    properties.put("specExecInstance.capabilities", "source:destination");
    properties.put("job.schedule", "0 0 0 ? * * 2050");
    properties.put("gobblin.flow.sourceIdentifier", "source");
    properties.put("gobblin.flow.destinationIdentifier", "destination");
    Config config = ConfigUtils.propertiesToConfig(properties);

    FlowSpec.Builder flowSpecBuilder = null;
    flowSpecBuilder = FlowSpec.builder(computeTopologySpecURI(SPEC_STORE_PARENT_DIR,
            FLOW_SPEC_GROUP_DIR))
        .withConfig(config)
        .withDescription(SPEC_DESCRIPTION)
        .withVersion(SPEC_VERSION)
        .withTemplate(URI.create("templateURI"));
    return flowSpecBuilder.build();
  }

  private FlowSpec initBadFlowSpec() {
    // Bad Flow Spec as we don't set the job name,  and will fail the compilation
    Properties properties = new Properties();
    properties.put("specStore.fs.dir", FLOW_SPEC_STORE_DIR);
    properties.put("specExecInstance.capabilities", "source:destination");
    properties.put("gobblin.flow.sourceIdentifier", "source");
    properties.put("gobblin.flow.destinationIdentifier", "destination");
    Config config = ConfigUtils.propertiesToConfig(properties);

    FlowSpec.Builder flowSpecBuilder = null;
    try {
      flowSpecBuilder = FlowSpec.builder(computeTopologySpecURI(SPEC_STORE_PARENT_DIR,
              FLOW_SPEC_GROUP_DIR))
          .withConfig(config)
          .withDescription(SPEC_DESCRIPTION)
          .withVersion(SPEC_VERSION)
          .withTemplate(new URI("templateURI"));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    return flowSpecBuilder.build();
  }

  public URI computeTopologySpecURI(String parent, String current) {
    // Make sure this is relative
    URI uri = PathUtils.relativizePath(new Path(current), new Path(parent)).toUri();
    return uri;
  }

  // TODO: this test doesn't exercise `Orchestrator` and so belongs elsewhere - move it, then rework into `@BeforeMethod` init (since others depend on this)
  @Test
  public void createTopologySpec() {
    IdentityFlowToJobSpecCompiler specCompiler = (IdentityFlowToJobSpecCompiler) this.dagMgrNotFlowLaunchHandlerBasedOrchestrator.getSpecCompiler();

    // List Current Specs
    Collection<Spec> specs = topologyCatalog.getSpecs();
    logger.info("[Before Create] Number of specs: " + specs.size());
    int i=0;
    for (Spec spec : specs) {
      TopologySpec topologySpec = (TopologySpec) spec;
      logger.info("[Before Create] Spec " + i++ + ": " + gson.toJson(topologySpec));
    }
    // Make sure TopologyCatalog is empty
    Assert.assertTrue(specs.isEmpty(), "Spec store should be empty before addition");
    // Make sure TopologyCatalog Listener is empty
    Assert.assertTrue(specCompiler.getTopologySpecMap().isEmpty(), "SpecCompiler should not know about any Topology "
        + "before addition");

    // Create and add Spec
    this.topologyCatalog.put(topologySpec);

    // List Specs after adding
    specs = topologyCatalog.getSpecs();
    logger.info("[After Create] Number of specs: " + specs.size());
    i = 0;
    for (Spec spec : specs) {
      topologySpec = (TopologySpec) spec;
      logger.info("[After Create] Spec " + i++ + ": " + gson.toJson(topologySpec));
    }
    // Make sure TopologyCatalog has the added Topology
    Assert.assertEquals(specs.size(), 1, "Spec store should contain 1 Spec after addition");
    // Make sure TopologyCatalog Listener knows about added Topology
    Assert.assertEquals(specCompiler.getTopologySpecMap().size(), 1,
        "SpecCompiler should contain 1 Spec after addition");
  }

  @Test
  public void createFlowSpec() throws Throwable {
    // TODO: fix this lingering inter-test dep from when `@BeforeClass` init, which we've since replaced by `Mockito.verify`-friendly `@BeforeMethod`
    createTopologySpec(); // make 1 Topology with 1 SpecProducer available and responsible for our new FlowSpec

    IdentityFlowToJobSpecCompiler specCompiler = (IdentityFlowToJobSpecCompiler) this.dagMgrNotFlowLaunchHandlerBasedOrchestrator.getSpecCompiler();
    SpecExecutor sei = specCompiler.getTopologySpecMap().values().iterator().next().getSpecExecutor();

    // List Current Specs
    Collection<Spec> specs = flowCatalog.getSpecs();
    logger.info("[Before Create] Number of specs: " + specs.size());
    int i=0;
    for (Spec spec : specs) {
      FlowSpec flowSpec = (FlowSpec) spec;
      logger.info("[Before Create] Spec " + i++ + ": " + gson.toJson(flowSpec));
    }
    // Make sure FlowCatalog is empty
    Assert.assertEquals(specs.size(), 0, "Spec store should be empty before addition");
    // Make sure FlowCatalog Listener is empty
    Assert.assertEquals(sei.getProducer().get().listSpecs().get().size(), 0,
        "SpecProducer should not know about " + "any Flow before addition");
    // Make sure we cannot add flow to specCatalog it flowSpec cannot compile
    Assert.expectThrows(Exception.class,() -> this.flowCatalog.put(initBadFlowSpec()));
    Assert.assertEquals(specs.size(), 0, "Spec store should be empty after adding bad flow spec");

    // Create and add Spec
    this.flowCatalog.put(flowSpec);

    // List Specs after adding
    specs = flowCatalog.getSpecs();
    logger.info("[After Create] Number of specs: " + specs.size());
    i = 0;
    for (Spec spec : specs) {
      flowSpec = (FlowSpec) spec;
      logger.info("[After Create] Spec " + i++ + ": " + gson.toJson(flowSpec));
    }

    // Make sure FlowCatalog has the added Flow
    Assert.assertEquals(specs.size(), 1, "Spec store should contain 1 Spec after addition");
    // Orchestrator is a no-op listener for any new FlowSpecs
    Assert.assertEquals(sei.getProducer().get().listSpecs().get().size(), 0,
        "SpecProducer should contain 0 " + "Spec after addition");
  }

  @Test
  public void deleteFlowSpec() throws Throwable {
    // TODO: fix this lingering inter-test dep from when `@BeforeClass` init, which we've since replaced by `Mockito.verify`-friendly `@BeforeMethod`
    createFlowSpec(); // make 1 Flow available (for deletion herein)

    IdentityFlowToJobSpecCompiler specCompiler = (IdentityFlowToJobSpecCompiler) this.dagMgrNotFlowLaunchHandlerBasedOrchestrator.getSpecCompiler();
    SpecExecutor sei = specCompiler.getTopologySpecMap().values().iterator().next().getSpecExecutor();

    // List Current Specs
    Collection<Spec> specs = flowCatalog.getSpecs();
    logger.info("[Before Delete] Number of specs: " + specs.size());
    int i=0;
    for (Spec spec : specs) {
      FlowSpec flowSpec = (FlowSpec) spec;
      logger.info("[Before Delete] Spec " + i++ + ": " + gson.toJson(flowSpec));
    }
    // Make sure FlowCatalog has the previously added Flow
    Assert.assertEquals(specs.size(), 1, "Spec store should contain 1 Flow that was added in last test");
    // Orchestrator is a no-op listener for any new FlowSpecs, so no FlowSpecs should be around
    int specsInSEI = sei.getProducer().get().listSpecs().get().size();
    Assert.assertEquals(specsInSEI, 0, "SpecProducer should contain 0 "
        + "Spec after addition because Orchestrator is a no-op listener for any new FlowSpecs");

    // Remove the flow
    this.flowCatalog.remove(flowSpec.getUri());

    // List Specs after adding
    specs = flowCatalog.getSpecs();
    logger.info("[After Delete] Number of specs: " + specs.size());
    i = 0;
    for (Spec spec : specs) {
      flowSpec = (FlowSpec) spec;
      logger.info("[After Delete] Spec " + i++ + ": " + gson.toJson(flowSpec));
    }

    // Make sure FlowCatalog has the Flow removed
    Assert.assertEquals(specs.size(), 0, "Spec store should not contain Spec after deletion");
    // Make sure FlowCatalog Listener knows about the deletion
    specsInSEI = sei.getProducer().get().listSpecs().get().size();
    Assert.assertEquals(specsInSEI, 0, "SpecProducer should not contain " + "Spec after deletion");
  }

  @Test
  public void doNotRegisterMetricsAdhocFlows() throws Throwable {
    // TODO: fix this lingering inter-test dep from when `@BeforeClass` init, which we've since replaced by `Mockito.verify`-friendly `@BeforeMethod`
    createTopologySpec(); // for flow compilation to pass

    FlowId flowId = GobblinServiceManagerTest.createFlowIdWithUniqueName(TEST_FLOW_GROUP_NAME);
    MetricContext metricContext = this.dagMgrNotFlowLaunchHandlerBasedOrchestrator.getSharedFlowMetricsSingleton().getMetricContext();
    String metricName = MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX, flowId.getFlowGroup(), flowId.getFlowName(), ServiceMetricNames.COMPILED);

    this.topologyCatalog.getInitComplete().countDown(); // unblock orchestration

    FlowSpec adhocSpec = createBasicFlowSpecForFlowId(flowId);
    this.dagMgrNotFlowLaunchHandlerBasedOrchestrator.orchestrate(adhocSpec, new Properties(), 0, false);
    Assert.assertNull(metricContext.getParent().get().getGauges().get(metricName));

    Properties scheduledProps = new Properties();
    scheduledProps.setProperty("job.schedule", "0/2 * * * * ?");
    FlowSpec scheduledSpec = createBasicFlowSpecForFlowId(flowId, scheduledProps);
    this.dagMgrNotFlowLaunchHandlerBasedOrchestrator.orchestrate(scheduledSpec, new Properties(), 0, false);
    Assert.assertNotNull(metricContext.getParent().get().getGauges().get(metricName));
  }

  @Test(enabled = false)
  // todo - re-write with dag proc engine
  public void removeFlowSpecWhenDagAdded() throws Throwable {
    // TODO: fix this lingering inter-test dep from when `@BeforeClass` init, which we've since replaced by `Mockito.verify`-friendly `@BeforeMethod`
    createTopologySpec(); // for flow compilation to pass

    FlowId flowId = GobblinServiceManagerTest.createFlowIdWithUniqueName(TEST_FLOW_GROUP_NAME);
    FlowSpec flowSpec = createBasicFlowSpecForFlowId(flowId);
    this.topologyCatalog.getInitComplete().countDown(); // unblock orchestration

    this.dagMgrNotFlowLaunchHandlerBasedOrchestrator.orchestrate(flowSpec, new Properties(), 0, false);
  }

  @Test(enabled = false)
  // todo - re-write with dag proc engine
  public void removeFlowSpecEvenWhenDagNotAddedDueToCompilationFailure() throws Throwable {
    // to cause flow compilation to fail, DO NOT EXECUTE: createTopologySpec();

    FlowId flowId = GobblinServiceManagerTest.createFlowIdWithUniqueName(TEST_FLOW_GROUP_NAME);
    FlowSpec flowSpec = createBasicFlowSpecForFlowId(flowId);
    this.topologyCatalog.getInitComplete().countDown(); // unblock orchestration

    this.dagMgrNotFlowLaunchHandlerBasedOrchestrator.orchestrate(flowSpec, new Properties(), 0, false);

    // (verifies that compilation failure precedes enforcement of concurrent flow executions)
    Mockito.verify(this.mockFlowStatusGenerator, Mockito.never()).isFlowRunning(any(), any(), anyLong());
  }

  @Test(enabled = false)
  // todo - re-write with dag proc engine
  public void removeFlowSpecEvenWhenDagNotAddedDueToConcurrentExecution() throws Throwable {
    // TODO: fix this lingering inter-test dep from when `@BeforeClass` init, which we've since replaced by `Mockito.verify`-friendly `@BeforeMethod`
    createTopologySpec(); // for flow compilation to pass

    FlowId flowId = GobblinServiceManagerTest.createFlowIdWithUniqueName(TEST_FLOW_GROUP_NAME);
    Properties noConcurrentExecsProps = new Properties();
    noConcurrentExecsProps.setProperty("flow.allowConcurrentExecution", "false");
    FlowSpec flowSpec = createBasicFlowSpecForFlowId(flowId, noConcurrentExecsProps);
    this.topologyCatalog.getInitComplete().countDown(); // unblock orchestration

    Mockito.when(this.mockFlowStatusGenerator.isFlowRunning(eq(flowId.getFlowName()), eq(flowId.getFlowGroup()), anyLong())).thenReturn(true);

    this.dagMgrNotFlowLaunchHandlerBasedOrchestrator.orchestrate(flowSpec, new Properties(), 0, false);

    // (verifies enforcement of concurrent flow executions)
    Mockito.verify(this.mockFlowStatusGenerator, Mockito.times(1)).isFlowRunning(eq(flowId.getFlowName()), eq(flowId.getFlowGroup()), anyLong());
  }


  /**
   * Tests that when compiling and forwarding a dagAction from
   * {@link org.apache.gobblin.service.monitoring.DagActionStoreChangeMonitor#submitFlowToDagManagerHelper} to the
   * DagManager that {@link DagManager#removeFlowSpecIfAdhoc(FlowSpec)} is called to ensure adhoc flowSpecs are deleted
   * after compilation.
   */
  @Test(enabled = false)
  // todo - re-write with dag proc engine
  public void testDeleteFlowSpecCalledForMultiActivePath()
      throws IOException, URISyntaxException, InterruptedException {
    FlowId flowId = GobblinServiceManagerTest.createFlowIdWithUniqueName(TEST_FLOW_GROUP_NAME);
    FlowSpec adhocSpec = createBasicFlowSpecForFlowId(flowId);
    FlowSpec flowSpec1 = initFlowSpec();

    Optional<Dag<JobExecutionPlan>> dag = Optional.of(
        DagTestUtils.buildDag("0", 123L, "FINISH_RUNNING", false));
    Mockito.when(this.mockedFlowCompilationValidationHelper.createExecutionPlanIfValid(flowSpec1)).thenReturn(dag);
  }

  public static FlowSpec createBasicFlowSpecForFlowId(FlowId flowId) throws URISyntaxException {
    return createBasicFlowSpecForFlowId(flowId, new Properties());
  }

  public static FlowSpec createBasicFlowSpecForFlowId(FlowId flowId, Properties moreProps) throws URISyntaxException {
    URI flowUri = FlowSpec.Utils.createFlowSpecUri(flowId);
    Properties flowProps = new Properties();
    // needed by - Orchestrator::orchestrate
    flowProps.setProperty(ConfigurationKeys.FLOW_GROUP_KEY, flowId.getFlowGroup());
    flowProps.setProperty(ConfigurationKeys.FLOW_NAME_KEY, flowId.getFlowName());
    // needed by - IdentityFlowToJobSpecCompiler::compileFlow
    flowProps.put("gobblin.flow.sourceIdentifier", "source");
    flowProps.put("gobblin.flow.destinationIdentifier", "destination");
    // needed by - IdentityFlowToJobSpecCompiler::getJobExecutionPlans
    flowProps.put("specExecInstance.capabilities", "source:destination");

    flowProps.putAll(moreProps);

    return new FlowSpec(flowUri, "1", "", ConfigUtils.propertiesToConfig(flowProps), flowProps, Optional.absent(), Optional.absent());
  }
}