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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.test.matchers.service.monitoring.FlowStatusMatch;
import org.apache.gobblin.test.matchers.service.monitoring.JobStatusMatch;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;


public class FlowStatusGeneratorTest {

  @Test
  public void testIsFlowRunning() {
    JobStatusRetriever jobStatusRetriever = Mockito.mock(JobStatusRetriever.class);
    String flowName = "testName";
    String flowGroup = "testGroup";
    when(jobStatusRetriever.getLatestExecutionIdsForFlow(flowName, flowGroup, 1)).thenReturn(null);

    FlowStatusGenerator flowStatusGenerator = new FlowStatusGenerator(jobStatusRetriever);
    Assert.assertFalse(flowStatusGenerator.isFlowRunning(flowName, flowGroup));

    //If a flow is COMPILED, isFlowRunning() should return true.
    long flowExecutionId = 1234L;
    when(jobStatusRetriever.getLatestExecutionIdsForFlow(flowName, flowGroup, 1)).thenReturn(
        Lists.newArrayList(flowExecutionId));
    JobStatus jobStatus = JobStatus.builder().flowGroup(flowGroup).flowName(flowName).flowExecutionId(flowExecutionId)
        .jobName(JobStatusRetriever.NA_KEY).jobGroup(JobStatusRetriever.NA_KEY).eventName("COMPILED").build();
    Iterator<JobStatus> jobStatusIterator = Lists.newArrayList(jobStatus).iterator();
    when(jobStatusRetriever.getJobStatusesForFlowExecution(flowName, flowGroup, flowExecutionId)).thenReturn(jobStatusIterator);
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
    when(jobStatusRetriever.getJobStatusesForFlowExecution(flowName, flowGroup, flowExecutionId)).thenReturn(jobStatusIterator);
    Assert.assertFalse(flowStatusGenerator.isFlowRunning(flowName, flowGroup));

    flowStatus = JobStatus.builder().flowGroup(flowGroup).flowName(flowName).flowExecutionId(flowExecutionId)
        .jobName(JobStatusRetriever.NA_KEY).jobGroup(JobStatusRetriever.NA_KEY).eventName("RUNNING").build();
    jobStatusIterator = Lists.newArrayList(jobStatus1, jobStatus2, jobStatus3, flowStatus).iterator();
    when(jobStatusRetriever.getJobStatusesForFlowExecution(flowName, flowGroup, flowExecutionId)).thenReturn(jobStatusIterator);
    when(jobStatusRetriever.getDagManagerEnabled()).thenReturn(true);
    Assert.assertTrue(flowStatusGenerator.isFlowRunning(flowName, flowGroup));
  }

  @Test
  public void testGetFlowStatusesAcrossGroup() {
    final long JOB_EXEC_ID = 987L;

    JobStatusRetriever jobStatusRetriever = Mockito.mock(JobStatusRetriever.class);
    // setup: one flow...
    String flowGroup = "myFlowGroup";
    int countPerFlowName = 2;
    String flowName1 = "flowName1";
    long flowExecutionId1 = 111L;
    ExecutionStatus flowStatus1 = ExecutionStatus.ORCHESTRATED;
    // ...with two jobs, each (differently) tagged.
    String f0Js1Status = ExecutionStatus.COMPLETE.name();
    String f0Js1Tag = "step-1";
    String f0Js1JobGroup1 = "job-group-x";
    String f0Js1JobName1 = "job-name-a";
    JobStatus f1Js0 = createFlowJobStatus(flowGroup, flowName1, flowExecutionId1, flowStatus1);
    JobStatus f1Js1 = createJobStatus(flowGroup, flowName1, flowExecutionId1,
        f0Js1Status, f0Js1Tag, f0Js1JobGroup1, f0Js1JobName1, JOB_EXEC_ID);
    String f0Js2Status = ExecutionStatus.FAILED.name();
    String f0Js2Tag = "step-2";
    String f0Js2JobGroup1 = "job-group-y";
    String f0Js2JobName1 = "job-name-b";
    JobStatus f1Js2 = createJobStatus(flowGroup, flowName1, flowExecutionId1,
        f0Js2Status, f0Js2Tag, f0Js2JobGroup1, f0Js2JobName1, JOB_EXEC_ID);

    // IMPORTANT: result invariants to honor - ordered by ascending flowName, all of same flowName adjacent, therein descending flowExecutionId
    // NOTE: Three copies of FlowStatus are needed for repeated use, due to mutable, non-rewinding `Iterator FlowStatus.getJobStatusIterator`
    FlowStatus flowStatus = createFlowStatus(flowGroup, flowName1, flowExecutionId1, Arrays.asList(f1Js0, f1Js1, f1Js2), jobStatusRetriever);
    FlowStatus flowStatus2 = createFlowStatus(flowGroup, flowName1, flowExecutionId1, Arrays.asList(f1Js0, f1Js1, f1Js2), jobStatusRetriever);
    FlowStatus flowStatus3 = createFlowStatus(flowGroup, flowName1, flowExecutionId1, Arrays.asList(f1Js0, f1Js1, f1Js2), jobStatusRetriever);
    Mockito.when(jobStatusRetriever.getFlowStatusesForFlowGroupExecutions("myFlowGroup", 2))
        .thenReturn(Collections.singletonList(flowStatus), Collections.singletonList(flowStatus2), Collections.singletonList(flowStatus3)); // (for three invocations)

    FlowStatusGenerator flowStatusGenerator = new FlowStatusGenerator(jobStatusRetriever);

    JobStatusMatch.Dependent f0jsmDep1 = JobStatusMatch.Dependent.ofTagged(f0Js1JobGroup1, f0Js1JobName1, JOB_EXEC_ID, f0Js1Status, f0Js1Tag);
    JobStatusMatch.Dependent f0jsmDep2 = JobStatusMatch.Dependent.ofTagged(f0Js2JobGroup1, f0Js2JobName1, JOB_EXEC_ID, f0Js2Status, f0Js2Tag);
    // verify all jobs returned when no tag constraint
    List<FlowStatus> flowStatusesResult = flowStatusGenerator.getFlowStatusesAcrossGroup(flowGroup, countPerFlowName, null);
    Assert.assertEquals(flowStatusesResult.size(), 1);
    assertThat(flowStatusesResult.get(0), FlowStatusMatch.withDependentJobStatuses(flowGroup, flowName1, flowExecutionId1, flowStatus1,
        Arrays.asList(f0jsmDep1, f0jsmDep2)));

    // verify 'flow pseudo status' plus first job returned against first job's tag
    List<FlowStatus> flowStatusesResult2 = flowStatusGenerator.getFlowStatusesAcrossGroup(flowGroup, countPerFlowName, f0Js1Tag);
    Assert.assertEquals(flowStatusesResult2.size(), 1);
    assertThat(flowStatusesResult2.get(0), FlowStatusMatch.withDependentJobStatuses(flowGroup, flowName1, flowExecutionId1, flowStatus1,
        Arrays.asList(f0jsmDep1)));

    // verify 'flow pseudo status' plus second job returned against second job's tag
    List<FlowStatus> flowStatusesResult3 = flowStatusGenerator.getFlowStatusesAcrossGroup(flowGroup, countPerFlowName, f0Js2Tag);
    Assert.assertEquals(flowStatusesResult3.size(), 1);
    assertThat(flowStatusesResult3.get(0), FlowStatusMatch.withDependentJobStatuses(flowGroup, flowName1, flowExecutionId1, flowStatus1,
        Arrays.asList(f0jsmDep2)));
  }

  private FlowStatus createFlowStatus(String flowGroup, String flowName, long flowExecutionId, List<JobStatus> jobStatuses, JobStatusRetriever jobStatusRetriever) {
    return new FlowStatus(flowName, flowGroup, flowExecutionId, jobStatuses.iterator(),
        JobStatusRetriever.getFlowStatusFromJobStatuses(jobStatusRetriever.getDagManagerEnabled(), jobStatuses.iterator()));
  }

  private JobStatus createFlowJobStatus(String flowGroup, String flowName, long flowExecutionId, ExecutionStatus status) {
    return createJobStatus(flowGroup, flowName, flowExecutionId, status.name(), null,
        JobStatusRetriever.NA_KEY, JobStatusRetriever.NA_KEY, 0L);
  }

  private JobStatus createJobStatus(String flowGroup, String flowName, long flowExecutionId, String eventName,
      String jobTag, String jobGroup, String jobName, long jobExecutionId) {
    return JobStatus.builder().flowGroup(flowGroup).flowName(flowName).flowExecutionId(flowExecutionId)
        .eventName(eventName).jobTag(jobTag)
        .jobGroup(jobGroup).jobName(jobName).jobExecutionId(jobExecutionId).build();
  }
}