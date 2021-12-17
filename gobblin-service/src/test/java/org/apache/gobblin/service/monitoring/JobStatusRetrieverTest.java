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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.test.matchers.service.monitoring.FlowStatusMatch;
import org.apache.gobblin.test.matchers.service.monitoring.JobStatusMatch;

import static org.hamcrest.MatcherAssert.assertThat;


public abstract class JobStatusRetrieverTest {

  protected static final String FLOW_GROUP = "myFlowGroup";
  protected static final String FLOW_NAME = "myFlowName";
  protected static final String FLOW_GROUP_ALT_A = "myFlowGroup-alt-A";
  protected static final String FLOW_GROUP_ALT_B = "myFlowGroup-alt-B";
  protected static final String FLOW_NAME_ALT_1 = "myFlowName-alt-1";
  protected static final String FLOW_NAME_ALT_2 = "myFlowName-alt-2";
  protected static final String FLOW_NAME_ALT_3 = "myFlowName-alt-3";
  protected String jobGroup;
  private static final String MY_JOB_GROUP = "myJobGroup";
  protected static final String MY_JOB_NAME_1 = "myJobName1";
  private static final String MY_JOB_NAME_2 = "myJobName2";
  private static final long JOB_EXECUTION_ID = 1111L;
  private static final String MESSAGE = "https://myServer:8143/1234/1111";
  protected static final long JOB_ORCHESTRATED_TIME = 3;
  protected static final long JOB_START_TIME = 5;
  protected static final long JOB_END_TIME = 15;
  JobStatusRetriever jobStatusRetriever;

  abstract void setUp() throws Exception;

  protected void addJobStatusToStateStore(long flowExecutionId, String jobName, String status) throws IOException {
    addFlowIdJobStatusToStateStore(FLOW_GROUP, FLOW_NAME, flowExecutionId, jobName, status, 0, 0, new Properties());
  }

  protected void addFlowIdJobStatusToStateStore(String flowGroup, String flowName, long flowExecutionId, String jobName, String status) throws IOException {
    addFlowIdJobStatusToStateStore(flowGroup, flowName, flowExecutionId, jobName, status, 0, 0, new Properties());
  }

  protected void addJobStatusToStateStore(long flowExecutionId, String jobName, String status, long startTime, long endTime) throws IOException {
    addFlowIdJobStatusToStateStore(FLOW_GROUP, FLOW_NAME, flowExecutionId, jobName, status, startTime, endTime, new Properties());
  }
  protected void addJobStatusToStateStore(long flowExecutionId, String jobName, String status, long startTime, long endTime, Properties properties) throws IOException {
    addFlowIdJobStatusToStateStore(FLOW_GROUP, FLOW_NAME, flowExecutionId, jobName, status, startTime, endTime, properties);
  }

  protected void addFlowIdJobStatusToStateStore(String flowGroup, String flowName, long flowExecutionId, String jobName, String status, long startTime, long endTime, Properties properties) throws IOException {
    properties.setProperty(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, flowGroup);
    properties.setProperty(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, flowName);
    properties.setProperty(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, String.valueOf(flowExecutionId));
    properties.setProperty(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, jobName);
    if (!jobName.equals(JobStatusRetriever.NA_KEY)) {
      jobGroup = MY_JOB_GROUP;
      properties.setProperty(TimingEvent.FlowEventConstants.JOB_EXECUTION_ID_FIELD, String.valueOf(JOB_EXECUTION_ID));
      properties.setProperty(TimingEvent.METADATA_MESSAGE, MESSAGE);
    } else {
      jobGroup = JobStatusRetriever.NA_KEY;
    }
    properties.setProperty(JobStatusRetriever.EVENT_NAME_FIELD, status);
    properties.setProperty(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD, jobGroup);
    if (status.equals(ExecutionStatus.RUNNING.name())) {
      properties.setProperty(TimingEvent.JOB_START_TIME, String.valueOf(startTime));
    } else if (status.equals(ExecutionStatus.COMPLETE.name())) {
      properties.setProperty(TimingEvent.JOB_END_TIME, String.valueOf(endTime));
    } else if (status.equals(ExecutionStatus.ORCHESTRATED.name())) {
      properties.setProperty(TimingEvent.JOB_ORCHESTRATED_TIME, String.valueOf(endTime));
    }
    State jobStatus = new State(properties);

    KafkaJobStatusMonitor.addJobStatusToStateStore(jobStatus, this.jobStatusRetriever.getStateStore());
  }

  static Properties createAttemptsProperties(int currGen, int currAttempts, boolean shouldRetry) {
    Properties properties = new Properties();
    properties.setProperty(TimingEvent.FlowEventConstants.CURRENT_GENERATION_FIELD, String.valueOf(currGen));
    properties.setProperty(TimingEvent.FlowEventConstants.CURRENT_ATTEMPTS_FIELD, String.valueOf(currAttempts));
    properties.setProperty(TimingEvent.FlowEventConstants.SHOULD_RETRY_FIELD, String.valueOf(shouldRetry));
    return properties;
  }
  @Test (dependsOnMethods = "testGetLatestExecutionIdsForFlow")
  public void testOutOfOrderJobTimingEventsForRetryingJob() throws IOException {
    long flowExecutionId = 1240L;
    Properties properties = createAttemptsProperties(1, 0, false);
    addJobStatusToStateStore(flowExecutionId, MY_JOB_NAME_1, ExecutionStatus.RUNNING.name(), JOB_START_TIME, JOB_START_TIME, properties);
    addJobStatusToStateStore(flowExecutionId, MY_JOB_NAME_1, ExecutionStatus.ORCHESTRATED.name(), JOB_ORCHESTRATED_TIME, JOB_ORCHESTRATED_TIME, properties);
    addJobStatusToStateStore(flowExecutionId, MY_JOB_NAME_1, ExecutionStatus.FAILED.name(), 0, 0, properties);
    Iterator<JobStatus>
        jobStatusIterator = this.jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId);
    JobStatus jobStatus = jobStatusIterator.next();
    if (jobStatus.getJobName().equals(JobStatusRetriever.NA_KEY)) {
      jobStatus = jobStatusIterator.next();
    }
    Assert.assertEquals(jobStatus.getEventName(), ExecutionStatus.PENDING_RETRY.name());
    Assert.assertEquals(jobStatus.isShouldRetry(), true);
    properties = createAttemptsProperties(1, 1, false);
    addJobStatusToStateStore(flowExecutionId, MY_JOB_NAME_1, ExecutionStatus.RUNNING.name(), JOB_START_TIME, JOB_START_TIME, properties);
    addJobStatusToStateStore(flowExecutionId, MY_JOB_NAME_1, ExecutionStatus.ORCHESTRATED.name(), JOB_ORCHESTRATED_TIME, JOB_ORCHESTRATED_TIME, properties);
    jobStatusIterator = this.jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId);
    jobStatus = jobStatusIterator.next();
    if (jobStatus.getJobName().equals(JobStatusRetriever.NA_KEY)) {
      jobStatus = jobStatusIterator.next();
    }
    Assert.assertEquals(jobStatus.getEventName(), ExecutionStatus.RUNNING.name());
    Assert.assertEquals(jobStatus.isShouldRetry(), false);
    Assert.assertEquals(jobStatus.getCurrentAttempts(), 1);
    Properties properties_new = createAttemptsProperties(2, 0, false);
    addJobStatusToStateStore(flowExecutionId, MY_JOB_NAME_1, ExecutionStatus.PENDING_RESUME.name(), JOB_START_TIME, JOB_START_TIME, properties_new);
    addJobStatusToStateStore(flowExecutionId, MY_JOB_NAME_1, ExecutionStatus.COMPLETE.name(), JOB_END_TIME, JOB_END_TIME, properties);
    jobStatusIterator = this.jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId);
    jobStatus = jobStatusIterator.next();
    if (jobStatus.getJobName().equals(JobStatusRetriever.NA_KEY)) {
      jobStatus = jobStatusIterator.next();
    }
    Assert.assertEquals(jobStatus.getEventName(), ExecutionStatus.PENDING_RESUME.name());
    addJobStatusToStateStore(flowExecutionId, MY_JOB_NAME_1, ExecutionStatus.COMPLETE.name(), JOB_END_TIME, JOB_END_TIME, properties_new);
    jobStatusIterator = this.jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId);
    jobStatus = jobStatusIterator.next();
    if (jobStatus.getJobName().equals(JobStatusRetriever.NA_KEY)) {
      jobStatus = jobStatusIterator.next();
    }
    Assert.assertEquals(jobStatus.getEventName(), ExecutionStatus.COMPLETE.name());
  }

  @Test
  public void testGetJobStatusesForFlowExecution() throws IOException {
    long flowExecutionId = 1234L;
    addJobStatusToStateStore(flowExecutionId, JobStatusRetriever.NA_KEY, ExecutionStatus.COMPILED.name());

    List<JobStatus> jobStatuses = ImmutableList.copyOf(this.jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId));
    Iterator<JobStatus> jobStatusIterator = jobStatuses.iterator();
    Assert.assertTrue(jobStatusIterator.hasNext());
    JobStatus jobStatus = jobStatusIterator.next();
    Assert.assertEquals(jobStatus.getEventName(), ExecutionStatus.COMPILED.name());
    Assert.assertEquals(jobStatus.getJobName(), JobStatusRetriever.NA_KEY);
    Assert.assertEquals(jobStatus.getJobGroup(), JobStatusRetriever.NA_KEY);
    Assert.assertEquals(jobStatus.getProcessedCount(), 0);
    Assert.assertEquals(jobStatus.getLowWatermark(), "");
    Assert.assertEquals(jobStatus.getHighWatermark(), "");

    addJobStatusToStateStore(flowExecutionId, JobStatusRetriever.NA_KEY, ExecutionStatus.RUNNING.name());
    addJobStatusToStateStore(flowExecutionId, MY_JOB_NAME_1, ExecutionStatus.RUNNING.name(), JOB_START_TIME, JOB_START_TIME);
    jobStatusIterator = this.jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId, MY_JOB_NAME_1, MY_JOB_GROUP);
    jobStatus = jobStatusIterator.next();
    Assert.assertEquals(jobStatus.getEventName(), ExecutionStatus.RUNNING.name());
    Assert.assertEquals(jobStatus.getJobName(), MY_JOB_NAME_1);
    Assert.assertEquals(jobStatus.getJobGroup(), jobGroup);
    Assert.assertFalse(jobStatusIterator.hasNext());
    Assert.assertEquals(ExecutionStatus.RUNNING,
        this.jobStatusRetriever.getFlowStatusFromJobStatuses(this.jobStatusRetriever.dagManagerEnabled, this.jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId)));

    addJobStatusToStateStore(flowExecutionId, MY_JOB_NAME_2, ExecutionStatus.RUNNING.name());
    jobStatusIterator = this.jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId);
    Assert.assertTrue(jobStatusIterator.hasNext());
    jobStatus = jobStatusIterator.next();
    if (JobStatusRetriever.isFlowStatus(jobStatus)) {
      jobStatus = jobStatusIterator.next();
    }
    Assert.assertTrue(jobStatus.getJobName().equals(MY_JOB_NAME_1) || jobStatus.getJobName().equals(MY_JOB_NAME_2));

    String jobName = jobStatus.getJobName();
    String nextExpectedJobName = (MY_JOB_NAME_1.equals(jobName)) ? MY_JOB_NAME_2 : MY_JOB_NAME_1;
    Assert.assertTrue(jobStatusIterator.hasNext());
    jobStatus = jobStatusIterator.next();
    if (JobStatusRetriever.isFlowStatus(jobStatus)) {
      Assert.assertTrue(jobStatusIterator.hasNext());
      jobStatus = jobStatusIterator.next();
    }

    Assert.assertEquals(jobStatus.getJobName(), nextExpectedJobName);
  }

  @Test (dependsOnMethods = "testGetJobStatusesForFlowExecution")
  public void testJobTiming() throws Exception {
    long flowExecutionId = 1233L;

    addJobStatusToStateStore(flowExecutionId, MY_JOB_NAME_1, ExecutionStatus.ORCHESTRATED.name(), JOB_ORCHESTRATED_TIME, JOB_ORCHESTRATED_TIME);
    addJobStatusToStateStore(flowExecutionId, MY_JOB_NAME_1, ExecutionStatus.RUNNING.name(), JOB_START_TIME, JOB_START_TIME);
    addJobStatusToStateStore(flowExecutionId, MY_JOB_NAME_1, ExecutionStatus.COMPLETE.name(), JOB_END_TIME, JOB_END_TIME);
    Iterator<JobStatus>
        jobStatusIterator = this.jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId, MY_JOB_NAME_1, MY_JOB_GROUP);
    JobStatus jobStatus = jobStatusIterator.next();

    Assert.assertEquals(jobStatus.getEventName(), ExecutionStatus.COMPLETE.name());
    Assert.assertEquals(jobStatus.getStartTime(), JOB_START_TIME);
    Assert.assertEquals(jobStatus.getEndTime(), JOB_END_TIME);
    Assert.assertEquals(jobStatus.getOrchestratedTime(), JOB_ORCHESTRATED_TIME);
  }

  @Test (dependsOnMethods = "testJobTiming")
  public void testOutOfOrderJobTimingEvents() throws IOException {
    long flowExecutionId = 1232L;
    addJobStatusToStateStore(flowExecutionId, MY_JOB_NAME_1, ExecutionStatus.RUNNING.name(), JOB_START_TIME, JOB_START_TIME);
    addJobStatusToStateStore(flowExecutionId, MY_JOB_NAME_1, ExecutionStatus.ORCHESTRATED.name(), JOB_ORCHESTRATED_TIME, JOB_ORCHESTRATED_TIME);
    addJobStatusToStateStore(flowExecutionId, MY_JOB_NAME_1, ExecutionStatus.COMPLETE.name(), JOB_END_TIME, JOB_END_TIME);
    Iterator<JobStatus>
        jobStatusIterator = this.jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId);
    JobStatus jobStatus = jobStatusIterator.next();
    if (jobStatus.getJobName().equals(JobStatusRetriever.NA_KEY)) {
      jobStatus = jobStatusIterator.next();
    }
    Assert.assertEquals(jobStatus.getEventName(), ExecutionStatus.COMPLETE.name());
    Assert.assertEquals(jobStatus.getStartTime(), JOB_START_TIME);
    Assert.assertEquals(jobStatus.getEndTime(), JOB_END_TIME);
    Assert.assertEquals(jobStatus.getOrchestratedTime(), JOB_ORCHESTRATED_TIME);
  }
  @Test (dependsOnMethods = "testJobTiming")
  public void testGetJobStatusesForFlowExecution1() {
    long flowExecutionId = 1234L;
    Iterator<JobStatus> jobStatusIterator = this.jobStatusRetriever.
        getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId, MY_JOB_NAME_1, MY_JOB_GROUP);

    Assert.assertTrue(jobStatusIterator.hasNext());
    JobStatus jobStatus = jobStatusIterator.next();
    Assert.assertEquals(jobStatus.getJobName(), MY_JOB_NAME_1);
    Assert.assertEquals(jobStatus.getJobGroup(), MY_JOB_GROUP);
    Assert.assertEquals(jobStatus.getJobExecutionId(), JOB_EXECUTION_ID);
    Assert.assertEquals(jobStatus.getFlowName(), FLOW_NAME);
    Assert.assertEquals(jobStatus.getFlowGroup(), FLOW_GROUP);
    Assert.assertEquals(jobStatus.getFlowExecutionId(), flowExecutionId);
    Assert.assertEquals(jobStatus.getMessage(), MESSAGE);
  }

  @Test (dependsOnMethods = "testGetJobStatusesForFlowExecution1")
  public void testGetLatestExecutionIdsForFlow() throws Exception {
    //Add new flow execution to state store
    long flowExecutionId1 = 1235L;
    addJobStatusToStateStore(flowExecutionId1, MY_JOB_NAME_1, ExecutionStatus.RUNNING.name());
    long latestExecutionIdForFlow = this.jobStatusRetriever.getLatestExecutionIdForFlow(FLOW_NAME, FLOW_GROUP);
    Assert.assertEquals(latestExecutionIdForFlow, flowExecutionId1);

    long flowExecutionId2 = 1236L;
    //IMPORTANT: multiple jobs for latest flow verifies that flow executions counted exactly once, not once per constituent job
    addJobStatusToStateStore(flowExecutionId2, MY_JOB_NAME_1, ExecutionStatus.COMPLETE.name());
    addJobStatusToStateStore(flowExecutionId2, MY_JOB_NAME_2, ExecutionStatus.RUNNING.name());

    //State store now has 3 flow executions - 1234, 1235, 1236. Get the latest 2 executions i.e. 1235 and 1236.
    List<Long> latestFlowExecutionIds = this.jobStatusRetriever.getLatestExecutionIdsForFlow(FLOW_NAME, FLOW_GROUP, 2);
    Assert.assertEquals(latestFlowExecutionIds.size(), 2);
    Assert.assertEquals(latestFlowExecutionIds, ImmutableList.of(flowExecutionId2, flowExecutionId1));

    //Remove all flow executions from state store
    cleanUpDir();
    Assert.assertEquals(this.jobStatusRetriever.getLatestExecutionIdsForFlow(FLOW_NAME, FLOW_GROUP, 1).size(), 0);
    Assert.assertEquals(this.jobStatusRetriever.getLatestExecutionIdForFlow(FLOW_NAME, FLOW_GROUP), -1L);
  }

  @Test
  public void testGetFlowStatusesForFlowGroupExecutions() throws IOException {
    // a.) simplify to begin, in `FLOW_GROUP_ALT_A`, leaving out job-level status
    addFlowIdJobStatusToStateStore(FLOW_GROUP_ALT_A, FLOW_NAME, 101L, JobStatusRetriever.NA_KEY, ExecutionStatus.COMPILED.name());
    addFlowIdJobStatusToStateStore(FLOW_GROUP_ALT_A, FLOW_NAME, 102L, JobStatusRetriever.NA_KEY, ExecutionStatus.RUNNING.name());

    addFlowIdJobStatusToStateStore(FLOW_GROUP_ALT_A, FLOW_NAME_ALT_1, 111L, JobStatusRetriever.NA_KEY, ExecutionStatus.COMPILED.name());

    addFlowIdJobStatusToStateStore(FLOW_GROUP_ALT_A, FLOW_NAME_ALT_2, 121L, JobStatusRetriever.NA_KEY, ExecutionStatus.COMPLETE.name());
    addFlowIdJobStatusToStateStore(FLOW_GROUP_ALT_A, FLOW_NAME_ALT_2, 122L, JobStatusRetriever.NA_KEY, ExecutionStatus.COMPILED.name());

    addFlowIdJobStatusToStateStore(FLOW_GROUP_ALT_A, FLOW_NAME_ALT_3, 131L, JobStatusRetriever.NA_KEY, ExecutionStatus.FAILED.name());
    addFlowIdJobStatusToStateStore(FLOW_GROUP_ALT_A, FLOW_NAME_ALT_3, 132L, JobStatusRetriever.NA_KEY, ExecutionStatus.COMPLETE.name());
    addFlowIdJobStatusToStateStore(FLOW_GROUP_ALT_A, FLOW_NAME_ALT_3, 133L, JobStatusRetriever.NA_KEY, ExecutionStatus.PENDING_RESUME.name());

    // b.) include job-level status, in `FLOW_GROUP_ALT_B`
    addFlowIdJobStatusToStateStore(FLOW_GROUP_ALT_B, FLOW_NAME_ALT_1, 211L, JobStatusRetriever.NA_KEY, ExecutionStatus.FAILED.name());
    addFlowIdJobStatusToStateStore(FLOW_GROUP_ALT_B, FLOW_NAME_ALT_1, 211L, MY_JOB_NAME_2, ExecutionStatus.ORCHESTRATED.name());

    addFlowIdJobStatusToStateStore(FLOW_GROUP_ALT_B, FLOW_NAME_ALT_3, 231L, JobStatusRetriever.NA_KEY, ExecutionStatus.COMPLETE.name());
    addFlowIdJobStatusToStateStore(FLOW_GROUP_ALT_B, FLOW_NAME_ALT_3, 231L, MY_JOB_NAME_1, ExecutionStatus.FAILED.name());
    addFlowIdJobStatusToStateStore(FLOW_GROUP_ALT_B, FLOW_NAME_ALT_3, 231L, MY_JOB_NAME_2, ExecutionStatus.COMPLETE.name());
    addFlowIdJobStatusToStateStore(FLOW_GROUP_ALT_B, FLOW_NAME_ALT_3, 232L, JobStatusRetriever.NA_KEY, ExecutionStatus.FAILED.name());
    addFlowIdJobStatusToStateStore(FLOW_GROUP_ALT_B, FLOW_NAME_ALT_3, 233L, JobStatusRetriever.NA_KEY, ExecutionStatus.ORCHESTRATED.name());
    addFlowIdJobStatusToStateStore(FLOW_GROUP_ALT_B, FLOW_NAME_ALT_3, 233L, MY_JOB_NAME_1, ExecutionStatus.COMPLETE.name());
    addFlowIdJobStatusToStateStore(FLOW_GROUP_ALT_B, FLOW_NAME_ALT_3, 233L, MY_JOB_NAME_2, ExecutionStatus.ORCHESTRATED.name());

    List<FlowStatus> flowStatusesForGroupAltA = this.jobStatusRetriever.getFlowStatusesForFlowGroupExecutions(FLOW_GROUP_ALT_A, 2);
    Assert.assertEquals(flowStatusesForGroupAltA.size(), 2 + 1 + 2 + 2);

    assertThat(flowStatusesForGroupAltA.get(0), FlowStatusMatch.of(FLOW_GROUP_ALT_A, FLOW_NAME, 102L, ExecutionStatus.RUNNING));
    assertThat(flowStatusesForGroupAltA.get(1), FlowStatusMatch.of(FLOW_GROUP_ALT_A, FLOW_NAME, 101L, ExecutionStatus.COMPILED));

    assertThat(flowStatusesForGroupAltA.get(2), FlowStatusMatch.of(FLOW_GROUP_ALT_A, FLOW_NAME_ALT_1, 111L, ExecutionStatus.COMPILED));

    assertThat(flowStatusesForGroupAltA.get(3), FlowStatusMatch.of(FLOW_GROUP_ALT_A, FLOW_NAME_ALT_2, 122L, ExecutionStatus.COMPILED));
    assertThat(flowStatusesForGroupAltA.get(4), FlowStatusMatch.of(FLOW_GROUP_ALT_A, FLOW_NAME_ALT_2, 121L, ExecutionStatus.COMPLETE));

    assertThat(flowStatusesForGroupAltA.get(5), FlowStatusMatch.of(FLOW_GROUP_ALT_A, FLOW_NAME_ALT_3, 133L, ExecutionStatus.PENDING_RESUME));
    assertThat(flowStatusesForGroupAltA.get(6), FlowStatusMatch.of(FLOW_GROUP_ALT_A, FLOW_NAME_ALT_3, 132L, ExecutionStatus.COMPLETE));


    List<FlowStatus> flowStatusesForGroupAltB = this.jobStatusRetriever.getFlowStatusesForFlowGroupExecutions(FLOW_GROUP_ALT_B, 1);
    Assert.assertEquals(flowStatusesForGroupAltB.size(), 1 + 1);

    assertThat(flowStatusesForGroupAltB.get(0), FlowStatusMatch.withDependentJobStatuses(FLOW_GROUP_ALT_B, FLOW_NAME_ALT_1, 211L, ExecutionStatus.FAILED,
        ImmutableList.of(JobStatusMatch.Dependent.of(MY_JOB_GROUP, MY_JOB_NAME_2, 1111L, ExecutionStatus.ORCHESTRATED.name()))));

    assertThat(flowStatusesForGroupAltB.get(1), FlowStatusMatch.withDependentJobStatuses(FLOW_GROUP_ALT_B, FLOW_NAME_ALT_3, 233L, ExecutionStatus.ORCHESTRATED,
        ImmutableList.of(
            JobStatusMatch.Dependent.of(MY_JOB_GROUP, MY_JOB_NAME_1, 1111L, ExecutionStatus.COMPLETE.name()),
            JobStatusMatch.Dependent.of(MY_JOB_GROUP, MY_JOB_NAME_2, 1111L, ExecutionStatus.ORCHESTRATED.name()))));
  }

  abstract void cleanUpDir() throws Exception;

  @AfterClass
  public void tearDown() throws Exception {
    cleanUpDir();
  }
}
