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
import java.net.URISyntaxException;
import java.util.HashMap;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagTestUtils;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Test functionality provided by the helper class re-used between the DagManager and Orchestrator for flow compilation.
 */
public class FlowCompilationValidationHelperTest {
  private String dagId = "testDag";
  private Long jobSpecFlowExecutionId = 1234L;
  private String newFlowExecutionId = "5678";
  private String existingFlowExecutionId = "9999";
  private Dag<JobExecutionPlan> jobExecutionPlanDag;

  @BeforeClass
  public void setup() throws URISyntaxException {
    jobExecutionPlanDag =  DagTestUtils.buildDag(dagId, jobSpecFlowExecutionId);

  }

  /*
    Tests that addFlowExecutionIdIfAbsent adds flowExecutionId to a flowMetadata object when it is absent, prioritizing
    the optional flowExecutionId over the one from the job spec
   */
  @Test
  public void testAddFlowExecutionIdWhenAbsent() {
    HashMap<String, String> flowMetadata = new HashMap<>();
    FlowCompilationValidationHelper.addFlowExecutionIdIfAbsent(flowMetadata, Optional.of(newFlowExecutionId), jobExecutionPlanDag);
    Assert.assertEquals(flowMetadata.get(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD), newFlowExecutionId);
  }

  /*
    Tests that addFlowExecutionIdIfAbsent does not update an existing flowExecutionId in a flowMetadata object
   */
  @Test
  public void testSkipAddingFlowExecutionIdWhenPresent() {
    HashMap<String, String> flowMetadata = new HashMap<>();
    flowMetadata.put(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, existingFlowExecutionId);
    FlowCompilationValidationHelper.addFlowExecutionIdIfAbsent(flowMetadata, Optional.of(newFlowExecutionId), jobExecutionPlanDag);
    Assert.assertEquals(flowMetadata.get(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD), existingFlowExecutionId);
  }
}
