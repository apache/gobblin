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

package org.apache.gobblin.service.modules.utils;

import com.google.common.base.Optional;
import java.io.IOException;
import java.util.Properties;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.service.modules.core.IdentityFlowToJobSpecCompilerTest;
import org.apache.gobblin.service.modules.flow.IdentityFlowToJobSpecCompiler;
import org.apache.gobblin.service.modules.flow.SpecCompiler;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.UserQuotaManager;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.FlowStatusGenerator;
import org.apache.gobblin.util.ConfigUtils;
import org.junit.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


/**
 * Test functionality provided by the helper class re-used between the DagManager and Orchestrator for flow compilation.
 */
public class FlowCompilationValidationHelperTest {

  class MockFlowCompilationValidationHelper extends FlowCompilationValidationHelper {

    public MockFlowCompilationValidationHelper(SharedFlowMetricsSingleton sharedFlowMetricsSingleton,
        SpecCompiler specCompiler, UserQuotaManager quotaManager, Optional<EventSubmitter> eventSubmitter,
        FlowStatusGenerator flowStatusGenerator, boolean isFlowConcurrencyEnabled) {
      super(sharedFlowMetricsSingleton, specCompiler, quotaManager, eventSubmitter, flowStatusGenerator,
          isFlowConcurrencyEnabled);
    }

    /*
      In overriden function simply return if concurrent execution is allowed or not because flowStatusGenerator will be
      mocked
     */
    @Override
    protected boolean isExecutionPermitted(FlowStatusGenerator flowStatusGenerator, String flowName, String flowGroup,
        boolean allowConcurrentExecution) {
      return allowConcurrentExecution;
    }

  }

  /*
  Creates a mock {@link FlowCompilationValidationHelper} which has a valid {@link SpecCompiler} but mocks other
  components.
   */
  MockFlowCompilationValidationHelper createMockFlowCompilationValidationHelper(boolean isFlowConcurrencyEnabled) {
    SpecCompiler specCompiler = new IdentityFlowToJobSpecCompiler(ConfigUtils.propertiesToConfig(new Properties()));
    specCompiler.onAddSpec(IdentityFlowToJobSpecCompilerTest.initTopologySpec());
    return new MockFlowCompilationValidationHelper(mock(SharedFlowMetricsSingleton.class), specCompiler,
        mock(UserQuotaManager.class), Optional.of(mock(EventSubmitter.class)), mock(FlowStatusGenerator.class),
        isFlowConcurrencyEnabled);
  }

  /**
   * Verifies that a flow spec that compiles to create a valid job execution plan dag will also generate one after
   * having a flow execution id key-value pair added to its config
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void compileFlowSpec() throws IOException, InterruptedException {
    MockFlowCompilationValidationHelper mockFlowCompilationValidationHelper = createMockFlowCompilationValidationHelper(true);
    FlowSpec flowSpec = IdentityFlowToJobSpecCompilerTest.initFlowSpec();
    Optional<Dag<JobExecutionPlan>> dagOptional = mockFlowCompilationValidationHelper.createExecutionPlanIfValid(flowSpec);
    // Assert FlowSpec compilation results in non-null or empty dag
    Assert.assertTrue(dagOptional.isPresent());
    Assert.assertNotNull(dagOptional.get());
    Assert.assertTrue(dagOptional.get().getNodes().size() == 1);
    Assert.assertEquals(dagOptional.get().getStartNodes().size(), 1);

    // Update flow spec and check if still compiles and passes checks
    flowSpec.updateConfigAndPropertiesWithProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "54321");
    dagOptional = mockFlowCompilationValidationHelper.createExecutionPlanIfValid(flowSpec);
    // Assert FlowSpec compilation results in non-null or empty dag
    Assert.assertTrue(dagOptional.isPresent());
    Assert.assertNotNull(dagOptional.get());
    Assert.assertTrue(dagOptional.get().getNodes().size() == 1);
    Assert.assertEquals(dagOptional.get().getStartNodes().size(), 1);
  }

}
