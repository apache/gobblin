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

import java.io.File;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.Invocation;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

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
import org.apache.gobblin.runtime.spec_catalog.FlowCatalogTest;
import org.apache.gobblin.runtime.spec_executorInstance.InMemorySpecExecutor;
import org.apache.gobblin.scheduler.SchedulerService;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flow.MockedSpecCompiler;
import org.apache.gobblin.service.modules.flow.SpecCompiler;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.AbstractUserQuotaManager;
import org.apache.gobblin.service.modules.orchestration.FlowLaunchHandler;
import org.apache.gobblin.service.modules.orchestration.InMemoryUserQuotaManager;
import org.apache.gobblin.service.modules.orchestration.Orchestrator;
import org.apache.gobblin.service.modules.orchestration.UserQuotaManager;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.spec.JobExecutionPlanDagFactory;
import org.apache.gobblin.testing.AssertWithBackoff;
import org.apache.gobblin.util.ConfigUtils;

import static org.apache.gobblin.runtime.spec_catalog.FlowCatalog.FLOWSPEC_STORE_DIR_KEY;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


public class GobblinServiceJobSchedulerTest {
  private static final String TEST_GROUP_NAME = "testGroup";
  private static final String TEST_FLOW_NAME = "testFlow";
  private static final String TEST_SCHEDULE = "0 1/0 * ? * *";
  private static final String TEST_TEMPLATE_URI = "FS:///templates/test.template";

  private Config quotaConfig;
  private SpecCatalogListener mockListener;
  private Closer closer = Closer.create();
  @BeforeClass
  public void setUp() {
    this.quotaConfig = ConfigFactory.empty().withValue(AbstractUserQuotaManager.PER_FLOWGROUP_QUOTA, ConfigValueFactory.fromAnyRef("group1:1"));
    this.mockListener = mock(SpecCatalogListener.class);
    when(this.mockListener.getName()).thenReturn(ServiceConfigKeys.GOBBLIN_ORCHESTRATOR_LISTENER_CLASS);
    when(this.mockListener.onAddSpec(any())).thenReturn(new AddSpecResponse<>(""));
  }

  @AfterClass(alwaysRun = true)
  public void tearDownClass() throws Exception {
    closer.close();
  }


  @Test
  public void testIsNextRunWithinRangeToSchedule() {
    int thresholdToSkipScheduling = 100;
    Assert.assertFalse(GobblinServiceJobScheduler.isWithinRange("0 0 0 ? 1 1 2050", thresholdToSkipScheduling));
    Assert.assertFalse(GobblinServiceJobScheduler.isWithinRange("0 0 0 ? 1 1 2030", thresholdToSkipScheduling));
    // Schedule at 3:20am every day should pass
    Assert.assertTrue(GobblinServiceJobScheduler.isWithinRange("0 20 3 * * ?", thresholdToSkipScheduling));
    // Schedule every sun, mon 4am should pass
    Assert.assertTrue(GobblinServiceJobScheduler.isWithinRange("0 * 4 ? * 1,2", thresholdToSkipScheduling));
    // For adhoc flows schedule is empty string
    Assert.assertTrue(GobblinServiceJobScheduler.isWithinRange("", thresholdToSkipScheduling));
    // Schedule for midnight of the current day which is in the past if threshold is set to zero (today)
    Assert.assertFalse(GobblinServiceJobScheduler.isWithinRange("0 0 0 * * ?", 0));
    // Capture invalid schedules in the past
    Assert.assertFalse(GobblinServiceJobScheduler.isWithinRange("0 0 0 ? * 6L 2022", thresholdToSkipScheduling));

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
    flowCatalog.addListener(mockListener);
    ServiceBasedAppLauncher serviceLauncher = closer.register(new ServiceBasedAppLauncher(properties, "GaaSJobSchedulerTest"));

    serviceLauncher.addService(flowCatalog);
    serviceLauncher.start();

    FlowSpec flowSpec0 = FlowCatalogTest.initFlowSpec(specDir.getAbsolutePath(), URI.create("spec0"));
    FlowSpec flowSpec1 = FlowCatalogTest.initFlowSpec(specDir.getAbsolutePath(), URI.create("spec1"));

    flowCatalog.put(flowSpec0, true);
    flowCatalog.put(flowSpec1, true);

    Assert.assertEquals(flowCatalog.getSpecs().size(), 2);

    Orchestrator mockOrchestrator = mock(Orchestrator.class);
    UserQuotaManager quotaManager = new InMemoryUserQuotaManager(quotaConfig);

    // Mock a GaaS scheduler.
    TestGobblinServiceJobScheduler scheduler =
        new TestGobblinServiceJobScheduler("testscheduler", ConfigFactory.empty(), flowCatalog,
            mockOrchestrator, quotaManager, null);

    SpecCompiler mockCompiler = mock(SpecCompiler.class);
    Mockito.when(mockOrchestrator.getSpecCompiler()).thenReturn(mockCompiler);
    Mockito.doAnswer((Answer<Void>) a -> {
      scheduler.isCompilerHealthy = true;
      return null;
    }).when(mockCompiler).awaitHealthy();

    scheduler.setActive(true);

    AssertWithBackoff.create().timeoutMs(20000).maxSleepMs(2000).backoffFactor(2)
        .assertTrue(input -> {
          Map<String, FlowSpec> scheduledFlowSpecs = scheduler.scheduledFlowSpecs;
          if (scheduledFlowSpecs != null && scheduledFlowSpecs.size() == 2) {
            return scheduler.scheduledFlowSpecs.containsKey("spec0") &&
                scheduler.scheduledFlowSpecs.containsKey("spec1");
          } else {
            return false;
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
    ServiceBasedAppLauncher serviceLauncher = closer.register(new ServiceBasedAppLauncher(properties, "GaaSJobSchedulerTest"));

    // Assume that the catalog can store corrupted flows
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

    Orchestrator mockOrchestrator = mock(Orchestrator.class);

    // Mock a GaaS scheduler.
    TestGobblinServiceJobScheduler scheduler =
        new TestGobblinServiceJobScheduler("testscheduler", ConfigFactory.empty(), flowCatalog,
            mockOrchestrator, new InMemoryUserQuotaManager(quotaConfig), null);

    SpecCompiler mockCompiler = mock(SpecCompiler.class);
    Mockito.when(mockOrchestrator.getSpecCompiler()).thenReturn(mockCompiler);
    Mockito.doAnswer((Answer<Void>) a -> {
      scheduler.isCompilerHealthy = true;
      return null;
    }).when(mockCompiler).awaitHealthy();

    scheduler.setActive(true);

    AssertWithBackoff.create().timeoutMs(20000).maxSleepMs(2000).backoffFactor(2)
        .assertTrue(input -> {
          Map<String, FlowSpec> scheduledFlowSpecs = scheduler.scheduledFlowSpecs;
          if (scheduledFlowSpecs != null && scheduledFlowSpecs.size() == 2) {
            return scheduler.scheduledFlowSpecs.containsKey("spec1") &&
                scheduler.scheduledFlowSpecs.containsKey("spec2");
          } else {
            return false;
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
    ServiceBasedAppLauncher serviceLauncher = closer.register(new ServiceBasedAppLauncher(properties, "GaaSJobSchedulerTest"));

    // Assume that the catalog can store corrupted flows
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

    Orchestrator mockOrchestrator = mock(Orchestrator.class);
    SchedulerService schedulerService = new SchedulerService(new Properties());
    // Mock a GaaS scheduler.
    TestGobblinServiceJobScheduler scheduler =
        new TestGobblinServiceJobScheduler("testscheduler", ConfigFactory.empty(), flowCatalog,
            mockOrchestrator, new InMemoryUserQuotaManager(quotaConfig), schedulerService);

    schedulerService.startAsync().awaitRunning();
    scheduler.startUp();
    SpecCompiler mockCompiler = mock(SpecCompiler.class);
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
            Map<String, FlowSpec> scheduledFlowSpecs = scheduler.scheduledFlowSpecs;
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
      Assert.assertNotEquals(invocation.getMethod().getName(), "remove");
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
    ServiceBasedAppLauncher serviceLauncher = closer.register(new ServiceBasedAppLauncher(properties, "GaaSJobSchedulerTest"));

    serviceLauncher.addService(flowCatalog);
    serviceLauncher.start();

    // We need to test adhoc flows since scheduled flows do not have a quota check in the scheduler
    FlowSpec flowSpec0 = FlowCatalogTest.initFlowSpec(specDir.getAbsolutePath(), URI.create("spec0"), "flowName0", "group1",
        ConfigFactory.empty(), true);
    FlowSpec flowSpec1 = FlowCatalogTest.initFlowSpec(specDir.getAbsolutePath(), URI.create("spec1"), "flowName1", "group1",
        ConfigFactory.empty(), true);

    Orchestrator mockOrchestrator = mock(Orchestrator.class);
    SpecCompiler mockSpecCompiler = mock(SpecCompiler.class);
    when(mockOrchestrator.getSpecCompiler()).thenReturn(mockSpecCompiler);
    Dag<JobExecutionPlan> mockDag0 = this.buildDag(flowSpec0.getConfig(), "0");
    Dag<JobExecutionPlan> mockDag1 = this.buildDag(flowSpec1.getConfig(), "1");
    when(mockSpecCompiler.compileFlow(flowSpec0)).thenReturn(mockDag0);
    when(mockSpecCompiler.compileFlow(flowSpec1)).thenReturn(mockDag1);

    SchedulerService schedulerService = new SchedulerService(new Properties());
    schedulerService.startAsync().awaitRunning();

    GobblinServiceJobScheduler scheduler = new GobblinServiceJobScheduler("testscheduler",
        ConfigFactory.empty(), flowCatalog, mockOrchestrator, schedulerService,
        new InMemoryUserQuotaManager(quotaConfig), Optional.absent(),
        Mockito.mock(FlowLaunchHandler.class));

    scheduler.startUp();
    scheduler.setActive(true);

    MockitoAnnotations.openMocks(this);
    Map<String, FlowSpec> mockMap = spy(new HashMap<>());

    // Use reflection to set the private map field to the spied one, so we can check the invocations
    Field mapField = scheduler.getClass().getDeclaredField("scheduledFlowSpecs");
    mapField.setAccessible(true);
    mapField.set(scheduler, mockMap);

    scheduler.onAddSpec(flowSpec0); //Ignore the response for this request
    Mockito.verify(mockMap, Mockito.times(1)).put(eq(flowSpec0.getUri().toString()), eq(flowSpec0));
    scheduler.onAddSpec(flowSpec1);
    // Second flow should be added to scheduled flows since no quota check in this case
    // we need to check if map's put is called to check if the spec was ever scheduled. we cannot just check the size
    // because the spec is removed by NonScheduledJobRunner
    Mockito.verify(mockMap, Mockito.times(1)).put(eq(flowSpec1.getUri().toString()), eq(flowSpec1));
    // set scheduler to be inactive and unschedule flows
    scheduler.setActive(false);
    Assert.assertEquals(scheduler.scheduledFlowSpecs.size(), 0);
  }

  static class TestGobblinServiceJobScheduler extends GobblinServiceJobScheduler {
    public boolean isCompilerHealthy = false;
    private boolean hasScheduler = false;

    public TestGobblinServiceJobScheduler(String serviceName, Config config, FlowCatalog flowCatalog,
        Orchestrator orchestrator, UserQuotaManager quotaManager, SchedulerService schedulerService) throws Exception {
      super(serviceName, config, flowCatalog, orchestrator, schedulerService, quotaManager,
          Optional.absent(), mock(FlowLaunchHandler.class));
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
      super.scheduledFlowSpecs.put(addedSpec.getUri().toString(), (FlowSpec) addedSpec);
      if (hasScheduler) {
        try {
          scheduleJob(((FlowSpec) addedSpec).getConfigAsProperties(), null);
        } catch (JobException e) {
          throw new RuntimeException(e);
        }
      }
      // Check that compiler is healthy at time of scheduling flows
      Assert.assertTrue(isCompilerHealthy);
      return new AddSpecResponse<>(addedSpec.getDescription());
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
