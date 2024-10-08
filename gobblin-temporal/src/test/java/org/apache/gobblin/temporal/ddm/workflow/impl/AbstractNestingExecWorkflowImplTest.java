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
package org.apache.gobblin.temporal.ddm.workflow.impl;

import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.history.v1.History;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.workflowservice.v1.GetWorkflowExecutionHistoryReverseRequest;
import io.temporal.api.workflowservice.v1.GetWorkflowExecutionHistoryReverseResponse;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.api.enums.v1.EventType;


import org.apache.gobblin.temporal.loadgen.activity.IllustrationItemActivity;
import org.apache.gobblin.temporal.loadgen.work.IllustrationItem;
import org.apache.gobblin.temporal.loadgen.work.SimpleGeneratedWorkload;
import org.apache.gobblin.temporal.loadgen.workflow.impl.NestingExecOfIllustrationItemActivityWorkflowImpl;
import org.apache.gobblin.temporal.util.nesting.work.WorkflowAddr;
import org.apache.gobblin.temporal.util.nesting.work.Workload;
import org.apache.gobblin.temporal.util.nesting.workflow.NestingExecWorkflow;


public class AbstractNestingExecWorkflowImplTest {

  private static final Logger logger = LoggerFactory.getLogger(AbstractNestingExecWorkflowImplTest.class);

  private WorkflowServiceStubs service;
  private WorkflowClient client;
  private WorkerFactory factory;
  private Worker worker;
  private final String TEMPORAL_TASK_QUEUE = "test-task-queue";

  @BeforeMethod
  public void setUp() throws Exception {
    // Connect to the Temporal service running in Docker
    service = WorkflowServiceStubs.newInstance();
    client = WorkflowClient.newInstance(service);
    factory = WorkerFactory.newInstance(client);
    worker = factory.newWorker(TEMPORAL_TASK_QUEUE);
    worker.registerWorkflowImplementationTypes(NestingExecOfIllustrationItemActivityWorkflowImpl.class);
    worker.registerActivitiesImplementations(new MockHandleItemActivityImpl());
    factory.start();
  }

  @AfterMethod
  public void tearDown() {
    factory.shutdown();
    service.shutdown();
  }

  @Test
  public void testPerformWorkloadWithEmptyWorkload() {
    final String workFlowId = UUID.randomUUID().toString();
    final NestingExecWorkflow workflow = client.newWorkflowStub(NestingExecWorkflow.class,
        WorkflowOptions.newBuilder().setTaskQueue(TEMPORAL_TASK_QUEUE).setWorkflowId(workFlowId).build());

    Workload<IllustrationItem> workload = SimpleGeneratedWorkload.createAs(0);

    int result = workflow.performWorkload(WorkflowAddr.ROOT, workload, 0, 900, 30, Optional.empty());
    Assert.assertEquals(0, result);
    Assert.assertEquals(0, getDepthLevelOfWorkFlowNesting(workFlowId));
  }

  @Test
  public void testPerformWorkloadWithPartialSpan() {
    final String workFlowId = UUID.randomUUID().toString();
    final NestingExecWorkflow workflow = client.newWorkflowStub(NestingExecWorkflow.class,
        WorkflowOptions.newBuilder().setTaskQueue(TEMPORAL_TASK_QUEUE).setWorkflowId(workFlowId).build());

    Workload workload = SimpleGeneratedWorkload.createAs(500);

    int result = workflow.performWorkload(new WorkflowAddr(0), workload, 0, 900, 30, Optional.empty());
    Assert.assertEquals(500, result);
    Assert.assertEquals(0, getDepthLevelOfWorkFlowNesting(workFlowId));
  }

  @Test
  public void testPerformWorkloadWithTwoLevelsOfNesting() {
    final String workFlowId = UUID.randomUUID().toString();
    final NestingExecWorkflow workflow = client.newWorkflowStub(NestingExecWorkflow.class,
        WorkflowOptions.newBuilder().setTaskQueue(TEMPORAL_TASK_QUEUE).setWorkflowId(workFlowId).build());

    Workload workload = SimpleGeneratedWorkload.createAs(2048);

    int result = workflow.performWorkload(new WorkflowAddr(0), workload, 0, 900, 30, Optional.empty());
    Assert.assertEquals(2048, result);
    Assert.assertEquals(2, getDepthLevelOfWorkFlowNesting(workFlowId));
  }

  @Test
  public void testPerformWorkloadWithMaxBranchesExceeded_WithTreeDepth_1() {
    final String workFlowId = UUID.randomUUID().toString();
    final NestingExecWorkflow workflow = client.newWorkflowStub(NestingExecWorkflow.class,
        WorkflowOptions.newBuilder().setTaskQueue(TEMPORAL_TASK_QUEUE).setWorkflowId(workFlowId).build());

    Workload workload = SimpleGeneratedWorkload.createAs(1024);

    int result = workflow.performWorkload(new WorkflowAddr(0), workload, 0, 1025, 30, Optional.empty());

    logger.info("PerformWorkload method returned");
    Assert.assertEquals(1024, result);
    Assert.assertEquals(1, getDepthLevelOfWorkFlowNesting(workFlowId));
  }

  @Test
  public void testPerformWorkloadWithMaxSubTreesOverride_WithTreeDepth_0() {
    final String workFlowId = UUID.randomUUID().toString();
    final NestingExecWorkflow workflow = client.newWorkflowStub(NestingExecWorkflow.class,
        WorkflowOptions.newBuilder().setTaskQueue(TEMPORAL_TASK_QUEUE).setWorkflowId(workFlowId).build());

    logger.info("Calling performWorkload method on workflow with max sub-trees override");
    Workload workload = SimpleGeneratedWorkload.createAs(1024);

    int result = workflow.performWorkload(new WorkflowAddr(0), workload, 0, 1024, 30, Optional.of(0));
    Assert.assertEquals(1024, result);
    Assert.assertEquals(0, getDepthLevelOfWorkFlowNesting(workFlowId));
  }

  private int getDepthLevelOfWorkFlowNesting(String workFlowId) {
    final GetWorkflowExecutionHistoryReverseResponse workflowExecutionHistoryResponse = client.getWorkflowServiceStubs()
        .blockingStub()
        .getWorkflowExecutionHistoryReverse(GetWorkflowExecutionHistoryReverseRequest.newBuilder()
            .setNamespace("default")
            .setExecution(WorkflowExecution.newBuilder().setWorkflowId(workFlowId).build())
            .build());
    int depth = 0;
    final History history = workflowExecutionHistoryResponse.getHistory();
    for (HistoryEvent historyEvent : history.getEventsList()) {
      if (historyEvent.getEventType().equals(EventType.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED)) {
        depth++;
      }
    }
    return depth;
  }

  private class MockHandleItemActivityImpl implements IllustrationItemActivity {
    @Override
    public String handleItem(IllustrationItem item) {
      return null;
    }
  }
}
