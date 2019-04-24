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
import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.app.ServiceBasedAppLauncher;
import org.apache.gobblin.runtime.spec_catalog.AddSpecResponse;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.runtime.spec_catalog.TopologyCatalog;
import org.apache.gobblin.scheduler.SchedulerService;
import org.apache.gobblin.service.modules.orchestration.Orchestrator;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalogTest;
import org.apache.gobblin.testing.AssertWithBackoff;
import org.apache.gobblin.util.ConfigUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.gobblin.runtime.spec_catalog.FlowCatalog.*;


public class GobblinServiceJobSchedulerTest {
  private static final String TEST_GROUP_NAME = "testGroup";
  private static final String TEST_FLOW_NAME = "testFlow";
  private static final String TEST_SCHEDULE = "0 1/0 * ? * *";
  private static final String TEST_TEMPLATE_URI = "FS:///templates/test.template";

  /**
   * Test whenever JobScheduler is calling setActive, the FlowSpec is loading into scheduledFlowSpecs (eventually)
   */
  @Test
  public void testJobSchedulerInit() throws Exception {
    // Mock a FlowCatalog.
    File specDir = Files.createTempDir();

    Properties properties = new Properties();
    properties.setProperty(FLOWSPEC_STORE_DIR_KEY, specDir.getAbsolutePath());
    FlowCatalog flowCatalog = new FlowCatalog(ConfigUtils.propertiesToConfig(properties));
    ServiceBasedAppLauncher serviceLauncher = new ServiceBasedAppLauncher(properties, "GaaSJobSchedulerTest");

    serviceLauncher.addService(flowCatalog);
    serviceLauncher.start();

    FlowSpec flowSpec0 = FlowCatalogTest.initFlowSpec(specDir.getAbsolutePath(), URI.create("spec0"));
    FlowSpec flowSpec1 = FlowCatalogTest.initFlowSpec(specDir.getAbsolutePath(), URI.create("spec1"));

    flowCatalog.put(flowSpec0, false);
    flowCatalog.put(flowSpec1, false);

    Assert.assertEquals(flowCatalog.getSpecs().size(), 2);

    // Mock a GaaS scheduler.
    TestGobblinServiceJobScheduler scheduler = new TestGobblinServiceJobScheduler("testscheduler",
        ConfigFactory.empty(), Optional.of(flowCatalog), null, null, null);

    scheduler.setActive(true);

    AssertWithBackoff.create().timeoutMs(6000).maxSleepMs(2000).backoffFactor(2)
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

  class TestGobblinServiceJobScheduler extends GobblinServiceJobScheduler {
    public TestGobblinServiceJobScheduler(String serviceName, Config config,
        Optional<FlowCatalog> flowCatalog, Optional<TopologyCatalog> topologyCatalog, Orchestrator orchestrator,
        SchedulerService schedulerService) throws Exception {
      super(serviceName, config, Optional.absent(), flowCatalog, topologyCatalog, orchestrator, schedulerService, Optional.absent());
    }

    /**
     * Override super method to only add spec into in-memory containers but not scheduling anything to simplify testing.
     */
    @Override
    public AddSpecResponse onAddSpec(Spec addedSpec) {
      super.scheduledFlowSpecs.put(addedSpec.getUri().toString(), addedSpec);
      return new AddSpecResponse(addedSpec.getDescription());
    }
  }
}