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
package org.apache.gobblin.service.monitoring;

import java.util.Iterator;

import org.junit.Assert;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;


public class FlowStatusGeneratorTest {

  @Test
  public void testIsFlowRunning() {
    JobStatusRetriever jobStatusRetriever = Mockito.mock(JobStatusRetriever.class);
    String flowName = "testName";
    String flowGroup = "testGroup";
    Mockito.when(jobStatusRetriever.getLatestExecutionIdsForFlow(flowName, flowGroup, 1)).thenReturn(null);

    FlowStatusGenerator flowStatusGenerator = FlowStatusGenerator.builder().jobStatusRetriever(jobStatusRetriever).build();
    Assert.assertFalse(flowStatusGenerator.isFlowRunning(flowName, flowGroup));

    //If a flow is COMPILED, isFlowRunning() should return true.
    Long flowExecutionId = 1234L;
    Mockito.when(jobStatusRetriever.getLatestExecutionIdsForFlow(flowName, flowGroup, 1)).thenReturn(
        Lists.newArrayList(flowExecutionId));
    JobStatus jobStatus = JobStatus.builder().flowGroup(flowGroup).flowName(flowName).flowExecutionId(flowExecutionId)
        .jobName(JobStatusRetriever.NA_KEY).jobGroup(JobStatusRetriever.NA_KEY).eventName("COMPILED").build();
    Iterator<JobStatus> jobStatusIterator = Lists.newArrayList(jobStatus).iterator();
    Mockito.when(jobStatusRetriever.getJobStatusesForFlowExecution(flowName, flowGroup, flowExecutionId)).thenReturn(jobStatusIterator);
    Assert.assertTrue(flowStatusGenerator.isFlowRunning(flowName, flowGroup));

    //JobStatuses should be ignored, only the flow level status matters.
    String job1 = "job1";
    String job2 = "job2";
    String job3 = "job3";
    JobStatus jobStatus1 = JobStatus.builder().flowGroup(flowGroup).flowName(flowName).flowExecutionId(flowExecutionId)
        .jobName(job1).eventName("COMPLETE").build();
    JobStatus jobStatus2 = JobStatus.builder().flowGroup(flowGroup).flowName(flowName).flowExecutionId(flowExecutionId)
        .jobName(job2).eventName("FAILED").build();
    JobStatus jobStatus3 = JobStatus.builder().flowGroup(flowGroup).flowName(flowName).flowExecutionId(flowExecutionId)
        .jobName(job3).eventName("CANCELLED").build();
    JobStatus flowStatus = JobStatus.builder().flowGroup(flowGroup).flowName(flowName).flowExecutionId(flowExecutionId)
        .jobName(JobStatusRetriever.NA_KEY).jobGroup(JobStatusRetriever.NA_KEY).eventName("CANCELLED").build();
    jobStatusIterator = Lists.newArrayList(jobStatus1, jobStatus2, jobStatus3, flowStatus).iterator();
    Mockito.when(jobStatusRetriever.getJobStatusesForFlowExecution(flowName, flowGroup, flowExecutionId)).thenReturn(jobStatusIterator);
    Assert.assertFalse(flowStatusGenerator.isFlowRunning(flowName, flowGroup));

    flowStatus = JobStatus.builder().flowGroup(flowGroup).flowName(flowName).flowExecutionId(flowExecutionId)
        .jobName(JobStatusRetriever.NA_KEY).jobGroup(JobStatusRetriever.NA_KEY).eventName("RUNNING").build();
    jobStatusIterator = Lists.newArrayList(jobStatus1, jobStatus2, jobStatus3, flowStatus).iterator();
    Mockito.when(jobStatusRetriever.getJobStatusesForFlowExecution(flowName, flowGroup, flowExecutionId)).thenReturn(jobStatusIterator);
    Assert.assertTrue(flowStatusGenerator.isFlowRunning(flowName, flowGroup));
  }
}