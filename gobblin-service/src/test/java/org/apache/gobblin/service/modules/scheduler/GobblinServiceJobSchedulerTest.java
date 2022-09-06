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
package org.apache.gobblin.service.modules.scheduler;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.JobException;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecCatalogListener;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.app.ServiceBasedAppLauncher;
import org.apache.gobblin.runtime.spec_catalog.AddSpecResponse;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.runtime.spec_catalog.TopologyCatalog;
import org.apache.gobblin.runtime.spec_executorInstance.InMemorySpecExecutor;
import org.apache.gobblin.scheduler.SchedulerService;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flow.MockedSpecCompiler;
import org.apache.gobblin.service.modules.flow.SpecCompiler;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.AbstractUserQuotaManager;
import org.apache.gobblin.service.modules.orchestration.InMemoryUserQuotaManager;
import org.apache.gobblin.service.modules.orchestration.Orchestrator;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalogTest;
import org.apache.gobblin.service.modules.orchestration.UserQuotaManager;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.spec.JobExecutionPlanDagFactory;
import org.apache.gobblin.testing.AssertWithBackoff;
import org.apache.gobblin.util.ConfigUtils;

import org.mockito.Mockito;
import org.mockito.invocation.Invocation;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.gobblin.runtime.spec_catalog.FlowCatalog.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;


public class GobblinServiceJobSchedulerTest {
  private static final String TEST_GROUP_NAME = "testGroup";
  private static final String TEST_FLOW_NAME = "testFlow";
  private static final String TEST_SCHEDULE = "0 1/0 * ? * *";
  private static final String TEST_TEMPLATE_URI = "FS:///templates/test.template";

  private Config quotaConfig;
  @BeforeClass
  public void setUp() {
    this.quotaConfig = ConfigFactory.empty().withValue(AbstractUserQuotaManager.PER_FLOWGROUP_QUOTA, ConfigValueFactory.fromAnyRef("group1:1"));
  }
  /**
   * Test whenever JobScheduler is calling setActive, the FlowSpec is loading into scheduledFlowSpecs (eventually)
   */
  @Test
  public void testJobSchedulerInit() throws Throwable {
    // Mock a FlowCatalog.
    File specDir = Files.createTempDir();

    Properties properties = new Properties();
    properties.setProperty(FLOWSPEC_STORE_DIR_KEY, specDir.getAbsolutePath());
    FlowCatalog flowCatalog = new FlowCatalog(ConfigUtils.propertiesToConfig(properties));
    SpecCatalogListener mockListener = Mockito.mock(SpecCatalogListener.class);
    when(mockListener.getName()).thenReturn(ServiceConfigKeys.GOBBLIN_SERVICE_JOB_SCHEDULER_LISTENER_CLASS);
    when(mockListener.onAddSpec(any())).thenReturn(new AddSpecResponse(""));
    flowCatalog.addListener(mockListener);
    ServiceBasedAppLauncher serviceLauncher = new ServiceBasedAppLauncher(properties, "GaaSJobSchedulerTest");

    serviceLauncher.addService(flowCatalog);
    serviceLauncher.start();

    FlowSpec flowSpec0 = FlowCatalogTest.initFlowSpec(specDir.getAbsolutePath(), URI.create("spec0"));
    FlowSpec flowSpec1 = FlowCatalogTest.initFlowSpec(specDir.getAbsolutePath(), URI.create("spec1"));

    flowCatalog.put(flowSpec0, true);
    flowCatalog.put(flowSpec1, true);

    Assert.assertEquals(flowCatalog.getSpecs().size(), 2);

    Orchestrator mockOrchestrator = Mockito.mock(Orchestrator.class);
    UserQuotaManager quotaManager = new InMemoryUserQuotaManager(quotaConfig);

    // Mock a GaaS scheduler.
    TestGobblinServiceJobScheduler scheduler = new TestGobblinServiceJobScheduler("testscheduler",
        ConfigFactory.empty(), Optional.of(flowCatalog), null, mockOrchestrator, Optional.of(quotaManager), null, false);

    SpecCompiler mockCompiler = Mockito.mock(SpecCompiler.class);
    Mockito.when(mockOrchestrator.getSpecCompiler()).thenReturn(mockCompiler);
    Mockito.doAnswer((Answer<Void>) a -> {
      scheduler.isCompilerHealthy = true;
      return null;
    }).when(mockCompiler).awaitHealthy();

    scheduler.setActive(true);

    AssertWithBackoff.create().timeoutMs(20000).maxSleepMs(2000).backoffFactor(2)
        .assertTrue(new Predicate<Void>() {
          @Override
          public boolean apply(Void input) {
            Map<String, Spec> scheduledFlowSpecs = scheduler.scheduledFlowSpecs;
            if (scheduledFlowSpecs != null && scheduledFlowSpecs.size() == 2) {
              return scheduler.scheduledFlowSpecs.containsKey("spec0") &&
                  scheduler.scheduledFlowSpecs.containsKey("spec1");
            } else {
              return false;
            }
          }
        }, "Waiting all flowSpecs to be scheduled");
  }

  @Test
  public void testDisableFlowRunImmediatelyOnStart()
      throws Exception {
    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.FLOW_RUN_IMMEDIATELY, "true");
    properties.setProperty(ConfigurationKeys.JOB_SCHEDULE_KEY, TEST_SCHEDULE);
    properties.setProperty(ConfigurationKeys.JOB_GROUP_KEY, TEST_GROUP_NAME);
    properties.setProperty(ConfigurationKeys.JOB_NAME_KEY, TEST_FLOW_NAME);
    Config config = ConfigFactory.parseProperties(properties);
    FlowSpec spec = FlowSpec.builder().withTemplate(new URI(TEST_TEMPLATE_URI)).withVersion("version")
        .withConfigAsProperties(properties).withConfig(config).build();
    FlowSpec modifiedSpec = (FlowSpec) GobblinServiceJobScheduler.disableFlowRunImmediatelyOnStart(spec);
    for (URI templateURI : modifiedSpec.getTemplateURIs().get()) {
      Assert.assertEquals(templateURI.toString(), TEST_TEMPLATE_URI);
    }
    Assert.assertEquals(modifiedSpec.getVersion(), "version");
    Config modifiedConfig = modifiedSpec.getConfig();
    Assert.assertFalse(modifiedConfig.getBoolean(ConfigurationKeys.FLOW_RUN_IMMEDIATELY));
    Assert.assertEquals(modifiedConfig.getString(ConfigurationKeys.JOB_GROUP_KEY), TEST_GROUP_NAME);
    Assert.assertEquals(modifiedConfig.getString(ConfigurationKeys.JOB_NAME_KEY), TEST_FLOW_NAME);
  }

  /**
   * Test that flowSpecs that throw compilation errors do not block the scheduling of other flowSpecs
   */
  @Test
  public void testJobSchedulerInitWithFailedSpec() throws Throwable {
    // Mock a FlowCatalog.
    File specDir = Files.createTempDir();

    Properties properties = new Properties();
    properties.setProperty(FLOWSPEC_STORE_DIR_KEY, specDir.getAbsolutePath());
    FlowCatalog flowCatalog = new FlowCatalog(ConfigUtils.propertiesToConfig(properties));
    ServiceBasedAppLauncher serviceLauncher = new ServiceBasedAppLauncher(properties, "GaaSJobSchedulerTest");

    // Assume that the catalog can store corrupted flows
    SpecCatalogListener mockListener = Mockito.mock(SpecCatalogListener.class);
    when(mockListener.getName()).thenReturn(ServiceConfigKeys.GOBBLIN_SERVICE_JOB_SCHEDULER_LISTENER_CLASS);
    when(mockListener.onAddSpec(any())).thenReturn(new AddSpecResponse(""));
    flowCatalog.addListener(mockListener);

    serviceLauncher.addService(flowCatalog);
    serviceLauncher.start();

    FlowSpec flowSpec0 = FlowCatalogTest.initFlowSpec(specDir.getAbsolutePath(), URI.create("spec0"),
        MockedSpecCompiler.UNCOMPILABLE_FLOW);
    FlowSpec flowSpec1 = FlowCatalogTest.initFlowSpec(specDir.getAbsolutePath(), URI.create("spec1"));
    FlowSpec flowSpec2 = FlowCatalogTest.initFlowSpec(specDir.getAbsolutePath(), URI.create("spec2"));

    // Ensure that these flows are scheduled
    flowCatalog.put(flowSpec0, true);
    flowCatalog.put(flowSpec1, true);
    flowCatalog.put(flowSpec2, true);

    Assert.assertEquals(flowCatalog.getSpecs().size(), 3);

    Orchestrator mockOrchestrator = Mockito.mock(Orchestrator.class);

    // Mock a GaaS scheduler.
    TestGobblinServiceJobScheduler scheduler = new TestGobblinServiceJobScheduler("testscheduler",
        ConfigFactory.empty(), Optional.of(flowCatalog), null, mockOrchestrator, Optional.of(new InMemoryUserQuotaManager(quotaConfig)), null, false);

    SpecCompiler mockCompiler = Mockito.mock(SpecCompiler.class);
    Mockito.when(mockOrchestrator.getSpecCompiler()).thenReturn(mockCompiler);
    Mockito.doAnswer((Answer<Void>) a -> {
      scheduler.isCompilerHealthy = true;
      return null;
    }).when(mockCompiler).awaitHealthy();

    scheduler.setActive(true);

    AssertWithBackoff.create().timeoutMs(20000).maxSleepMs(2000).backoffFactor(2)
        .assertTrue(new Predicate<Void>() {
          @Override
          public boolean apply(Void input) {
            Map<String, Spec> scheduledFlowSpecs = scheduler.scheduledFlowSpecs;
            if (scheduledFlowSpecs != null && scheduledFlowSpecs.size() == 2) {
              return scheduler.scheduledFlowSpecs.containsKey("spec1") &&
                  scheduler.scheduledFlowSpecs.containsKey("spec2");
            } else {
              return false;
            }
          }
        }, "Waiting all flowSpecs to be scheduled");
  }

  /**
   * Test that flowSpecs that throw compilation errors do not block the scheduling of other flowSpecs
   */
  @Test
  public void testJobSchedulerUnschedule() throws Throwable {
    // Mock a FlowCatalog.
    File specDir = Files.createTempDir();

    Properties properties = new Properties();
    properties.setProperty(FLOWSPEC_STORE_DIR_KEY, specDir.getAbsolutePath());
    FlowCatalog flowCatalog = new FlowCatalog(ConfigUtils.propertiesToConfig(properties));
    ServiceBasedAppLauncher serviceLauncher = new ServiceBasedAppLauncher(properties, "GaaSJobSchedulerTest");

    // Assume that the catalog can store corrupted flows
    SpecCatalogListener mockListener = Mockito.mock(SpecCatalogListener.class);
    when(mockListener.getName()).thenReturn(ServiceConfigKeys.GOBBLIN_SERVICE_JOB_SCHEDULER_LISTENER_CLASS);
    when(mockListener.onAddSpec(any())).thenReturn(new AddSpecResponse(""));
    flowCatalog.addListener(mockListener);

    serviceLauncher.addService(flowCatalog);
    serviceLauncher.start();

    FlowSpec flowSpec0 = FlowCatalogTest.initFlowSpec(specDir.getAbsolutePath(), URI.create("spec0"));
    FlowSpec flowSpec1 = FlowCatalogTest.initFlowSpec(specDir.getAbsolutePath(), URI.create("spec1"));
    FlowSpec flowSpec2 = FlowCatalogTest.initFlowSpec(specDir.getAbsolutePath(), URI.create("spec2"));

    // Ensure that these flows are scheduled
    flowCatalog.put(flowSpec0, true);
    flowCatalog.put(flowSpec1, true);
    flowCatalog.put(flowSpec2, true);

    Assert.assertEquals(flowCatalog.getSpecs().size(), 3);

    Orchestrator mockOrchestrator = Mockito.mock(Orchestrator.class);
    SchedulerService schedulerService = new SchedulerService(new Properties());
    // Mock a GaaS scheduler.
    TestGobblinServiceJobScheduler scheduler = new TestGobblinServiceJobScheduler("testscheduler",
        ConfigFactory.empty(), Optional.of(flowCatalog), null, mockOrchestrator, Optional.of(new InMemoryUserQuotaManager(quotaConfig)), schedulerService, false);

    schedulerService.startAsync().awaitRunning();
    scheduler.startUp();
    SpecCompiler mockCompiler = Mockito.mock(SpecCompiler.class);
    Mockito.when(mockOrchestrator.getSpecCompiler()).thenReturn(mockCompiler);
    Mockito.doAnswer((Answer<Void>) a -> {
      scheduler.isCompilerHealthy = true;
      return null;
    }).when(mockCompiler).awaitHealthy();

    scheduler.setActive(true);

    AssertWithBackoff.create().timeoutMs(20000).maxSleepMs(2000).backoffFactor(2)
        .assertTrue(new Predicate<Void>() {
          @Override
          public boolean apply(Void input) {
            Map<String, Spec> scheduledFlowSpecs = scheduler.scheduledFlowSpecs;
            if (scheduledFlowSpecs != null && scheduledFlowSpecs.size() == 3) {
              return scheduler.scheduledFlowSpecs.containsKey("spec0") &&
                  scheduler.scheduledFlowSpecs.containsKey("spec1") &&
                  scheduler.scheduledFlowSpecs.containsKey("spec2");
            } else {
              return false;
            }
          }
        }, "Waiting all flowSpecs to be scheduled");

    // set scheduler to be inactive and unschedule flows
    scheduler.setActive(false);
    Collection<Invocation> invocations = Mockito.mockingDetails(mockOrchestrator).getInvocations();

    for (Invocation invocation: invocations) {
      // ensure that orchestrator is not calling remove
      Assert.assertFalse(invocation.getMethod().getName().equals("remove"));
    }

    Assert.assertEquals(scheduler.scheduledFlowSpecs.size(), 0);
    Assert.assertEquals(schedulerService.getScheduler().getJobGroupNames().size(), 0);
  }

  @Test
  public void testJobSchedulerAddFlowQuotaExceeded() throws Exception {
    File specDir = Files.createTempDir();

    Properties properties = new Properties();
    properties.setProperty(FLOWSPEC_STORE_DIR_KEY, specDir.getAbsolutePath());
    FlowCatalog flowCatalog = new FlowCatalog(ConfigUtils.propertiesToConfig(properties));
    ServiceBasedAppLauncher serviceLauncher = new ServiceBasedAppLauncher(properties, "GaaSJobSchedulerTest");


    serviceLauncher.addService(flowCatalog);
    serviceLauncher.start();

    FlowSpec flowSpec0 = FlowCatalogTest.initFlowSpec(specDir.getAbsolutePath(), URI.create("spec0"), "flowName0", "group1",
        ConfigFactory.empty().withValue(ConfigurationKeys.FLOW_RUN_IMMEDIATELY, ConfigValueFactory.fromAnyRef("true")));
    FlowSpec flowSpec1 = FlowCatalogTest.initFlowSpec(specDir.getAbsolutePath(), URI.create("spec1"), "flowName1", "group1",
        ConfigFactory.empty().withValue(ConfigurationKeys.FLOW_RUN_IMMEDIATELY, ConfigValueFactory.fromAnyRef("true")));

    Orchestrator mockOrchestrator = Mockito.mock(Orchestrator.class);
    SpecCompiler mockSpecCompiler = Mockito.mock(SpecCompiler.class);
    when(mockOrchestrator.getSpecCompiler()).thenReturn(mockSpecCompiler);
    Dag<JobExecutionPlan> mockDag0 = this.buildDag(flowSpec0.getConfig(), "0");
    Dag<JobExecutionPlan> mockDag1 = this.buildDag(flowSpec1.getConfig(), "1");
    when(mockSpecCompiler.compileFlow(flowSpec0)).thenReturn(mockDag0);
    when(mockSpecCompiler.compileFlow(flowSpec1)).thenReturn(mockDag1);

    SchedulerService schedulerService = new SchedulerService(new Properties());
    // Mock a GaaS scheduler not in warm standby mode
    GobblinServiceJobScheduler scheduler = new GobblinServiceJobScheduler("testscheduler",
        ConfigFactory.empty(), Optional.absent(), Optional.of(flowCatalog), null, mockOrchestrator, schedulerService, Optional.of(new InMemoryUserQuotaManager(quotaConfig)), Optional.absent(), false);

    schedulerService.startAsync().awaitRunning();
    scheduler.startUp();
    scheduler.setActive(true);

    scheduler.onAddSpec(flowSpec0); //Ignore the response for this request
    Assert.assertThrows(RuntimeException.class, () -> scheduler.onAddSpec(flowSpec1));

    Assert.assertEquals(scheduler.scheduledFlowSpecs.size(), 1);
    // Second flow should not be added to scheduled flows since it was rejected
    Assert.assertEquals(scheduler.scheduledFlowSpecs.size(), 1);
    // set scheduler to be inactive and unschedule flows
    scheduler.setActive(false);
    Assert.assertEquals(scheduler.scheduledFlowSpecs.size(), 0);

    //Mock a GaaS scheduler in warm standby mode, where we don't check quota
    GobblinServiceJobScheduler schedulerWithWarmStandbyEnabled = new GobblinServiceJobScheduler("testscheduler",
        ConfigFactory.empty(), Optional.absent(), Optional.of(flowCatalog), null, mockOrchestrator, schedulerService, Optional.of(new InMemoryUserQuotaManager(quotaConfig)), Optional.absent(), true);

    schedulerWithWarmStandbyEnabled.startUp();
    schedulerWithWarmStandbyEnabled.setActive(true);

    schedulerWithWarmStandbyEnabled.onAddSpec(flowSpec0); //Ignore the response for this request
    Assert.assertEquals(schedulerWithWarmStandbyEnabled.scheduledFlowSpecs.size(), 1);
    schedulerWithWarmStandbyEnabled.onAddSpec(flowSpec1);
    // Second flow should be added to scheduled flows since no quota check in this case
    Assert.assertEquals(schedulerWithWarmStandbyEnabled.scheduledFlowSpecs.size(), 2);
    // set scheduler to be inactive and unschedule flows
    schedulerWithWarmStandbyEnabled.setActive(false);
    Assert.assertEquals(schedulerWithWarmStandbyEnabled.scheduledFlowSpecs.size(), 0);
  }

  class TestGobblinServiceJobScheduler extends GobblinServiceJobScheduler {
    public boolean isCompilerHealthy = false;
    private boolean hasScheduler = false;

    public TestGobblinServiceJobScheduler(String serviceName, Config config,
        Optional<FlowCatalog> flowCatalog, Optional<TopologyCatalog> topologyCatalog, Orchestrator orchestrator, Optional<UserQuotaManager> quotaManager,
        SchedulerService schedulerService, boolean isWarmStandbyEnabled) throws Exception {
      super(serviceName, config, Optional.absent(), flowCatalog, topologyCatalog, orchestrator, schedulerService, quotaManager, Optional.absent(), isWarmStandbyEnabled);
      if (schedulerService != null) {
        hasScheduler = true;
      }
    }

    /**
     * Override super method to only add spec into in-memory containers but not scheduling anything to simplify testing.
     */
    @Override
    public AddSpecResponse onAddSpec(Spec addedSpec) {
      String flowName = (String) ((FlowSpec) addedSpec).getConfigAsProperties().get(ConfigurationKeys.FLOW_NAME_KEY);
      if (flowName.equals(MockedSpecCompiler.UNCOMPILABLE_FLOW)) {
        throw new RuntimeException("Could not compile flow");
      }
      super.scheduledFlowSpecs.put(addedSpec.getUri().toString(), addedSpec);
      if (hasScheduler) {
        try {
          scheduleJob(((FlowSpec) addedSpec).getConfigAsProperties(), null);
        } catch (JobException e) {
          throw new RuntimeException(e);
        }
      }
      // Check that compiler is healthy at time of scheduling flows
      Assert.assertTrue(isCompilerHealthy);
      return new AddSpecResponse(addedSpec.getDescription());
    }
  }

   Dag<JobExecutionPlan> buildDag(Config additionalConfig, String id) throws URISyntaxException {
    Config config = ConfigFactory.empty().
        withValue(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, ConfigValueFactory.fromAnyRef(System.currentTimeMillis()));

    config = additionalConfig.withFallback(config);
    List<JobExecutionPlan> jobExecutionPlans = new ArrayList<>();
    JobSpec js = JobSpec.builder("test_job_" + id).withVersion(id).withConfig(config).
        withTemplate(new URI("job_" + id)).build();
    SpecExecutor specExecutor = InMemorySpecExecutor.createDummySpecExecutor(new URI("jobExecutor"));
    JobExecutionPlan jobExecutionPlan = new JobExecutionPlan(js, specExecutor);
    jobExecutionPlans.add(jobExecutionPlan);
    return new JobExecutionPlanDagFactory().createDag(jobExecutionPlans);
  }
}