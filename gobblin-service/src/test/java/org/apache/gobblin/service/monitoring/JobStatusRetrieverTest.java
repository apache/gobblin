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

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.service.ExecutionStatus;


public abstract class JobStatusRetrieverTest {
  protected static final String FLOW_GROUP = "myFlowGroup";
  protected static final String FLOW_NAME = "myFlowName";
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

  protected void addJobStatusToStateStore(Long flowExecutionId, String jobName, String status) throws IOException {
    addJobStatusToStateStore(flowExecutionId, jobName, status, 0, 0);
  }

  protected void addJobStatusToStateStore(Long flowExecutionId, String jobName, String status, long startTime, long endTime) throws IOException {
    Properties properties = new Properties();
    properties.setProperty(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, FLOW_GROUP);
    properties.setProperty(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, FLOW_NAME);
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

  @Test
  public void testGetJobStatusesForFlowExecution() throws IOException {
    long flowExecutionId = 1234L;
    addJobStatusToStateStore(flowExecutionId, JobStatusRetriever.NA_KEY, ExecutionStatus.COMPILED.name());

    Iterator<JobStatus>
        jobStatusIterator = this.jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId);
    Assert.assertTrue(jobStatusIterator.hasNext());
    JobStatus jobStatus = jobStatusIterator.next();
    Assert.assertEquals(jobStatus.getEventName(), ExecutionStatus.COMPILED.name());
    Assert.assertEquals(jobStatus.getJobName(), JobStatusRetriever.NA_KEY);
    Assert.assertEquals(jobStatus.getJobGroup(), JobStatusRetriever.NA_KEY);
    Assert.assertEquals(jobStatus.getProcessedCount(), 0);
    Assert.assertEquals(jobStatus.getLowWatermark(), "");
    Assert.assertEquals(jobStatus.getHighWatermark(), "");

    addJobStatusToStateStore(flowExecutionId, MY_JOB_NAME_1, ExecutionStatus.RUNNING.name(), JOB_START_TIME, JOB_START_TIME);
    jobStatusIterator = this.jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId, MY_JOB_NAME_1, MY_JOB_GROUP);
    jobStatus = jobStatusIterator.next();
    Assert.assertEquals(jobStatus.getEventName(), ExecutionStatus.RUNNING.name());
    Assert.assertEquals(jobStatus.getJobName(), MY_JOB_NAME_1);
    Assert.assertEquals(jobStatus.getJobGroup(), jobGroup);
    Assert.assertFalse(jobStatusIterator.hasNext());

    addJobStatusToStateStore(flowExecutionId, MY_JOB_NAME_2, ExecutionStatus.RUNNING.name());
    jobStatusIterator = this.jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId);
    Assert.assertTrue(jobStatusIterator.hasNext());
    jobStatus = jobStatusIterator.next();
    if (jobStatus.getJobName().equals(JobStatusRetriever.NA_KEY)) {
      jobStatus = jobStatusIterator.next();
    }
    Assert.assertTrue(jobStatus.getJobName().equals(MY_JOB_NAME_1) || jobStatus.getJobName().equals(MY_JOB_NAME_2));

    String jobName = jobStatus.getJobName();
    String nextExpectedJobName = (MY_JOB_NAME_1.equals(jobName)) ? MY_JOB_NAME_2 : MY_JOB_NAME_1;
    Assert.assertTrue(jobStatusIterator.hasNext());
    jobStatus = jobStatusIterator.next();
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
    addJobStatusToStateStore(flowExecutionId2, MY_JOB_NAME_1, ExecutionStatus.RUNNING.name());

    //State store now has 3 flow executions - 1234, 1235, 1236. Get the latest 2 executions i.e. 1235 and 1236.
    List<Long> latestFlowExecutionIds = this.jobStatusRetriever.getLatestExecutionIdsForFlow(FLOW_NAME, FLOW_GROUP, 2);
    Assert.assertEquals(latestFlowExecutionIds.size(), 2);
    Assert.assertEquals(latestFlowExecutionIds.get(0), (Long) flowExecutionId2);
    Assert.assertEquals(latestFlowExecutionIds.get(1), (Long) flowExecutionId1);

    //Remove all flow executions from state store
    cleanUpDir();
    Assert.assertEquals(this.jobStatusRetriever.getLatestExecutionIdsForFlow(FLOW_NAME, FLOW_GROUP, 1).size(), 0);
    Assert.assertEquals(this.jobStatusRetriever.getLatestExecutionIdForFlow(FLOW_NAME, FLOW_GROUP), -1L);
  }

  abstract void cleanUpDir() throws Exception;

  @AfterClass
  public void tearDown() throws Exception {
    cleanUpDir();
  }
}
