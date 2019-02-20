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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.FileContextBasedFsStateStore;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.service.ExecutionStatus;


public class FsJobStatusRetrieverTest {
  private FsJobStatusRetriever jobStatusRetriever;
  private FileContextBasedFsStateStore fsStateStore;
  private String stateStoreDir = "/tmp/jobStatusRetrieverTest/statestore";

  private String flowGroup = "myFlowGroup";
  private String flowName = "myFlowName";
  private String jobGroup;
  private String myJobGroup = "myJobGroup";
  private long jobExecutionId = 1111L;
  private String message = "https://myServer:8143/1234/1111";

  @BeforeClass
  public void setUp() throws Exception {
    cleanUpDir(stateStoreDir);
    Config config = ConfigFactory.empty().withValue(FsJobStatusRetriever.CONF_PREFIX + "." + ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY,
        ConfigValueFactory.fromAnyRef(stateStoreDir));
    this.jobStatusRetriever = new FsJobStatusRetriever(config);
    this.fsStateStore = this.jobStatusRetriever.getStateStore();
  }

  private void addJobStatusToStateStore(Long flowExecutionId, String jobName) throws IOException {
    Properties properties = new Properties();
    properties.setProperty(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, this.flowGroup);
    properties.setProperty(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, this.flowName);
    properties.setProperty(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, String.valueOf(flowExecutionId));
    properties.setProperty(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, jobName);
    if (!jobName.equals(JobStatusRetriever.NA_KEY)) {
      this.jobGroup = myJobGroup;
      properties.setProperty(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD, myJobGroup);
      properties.setProperty(TimingEvent.FlowEventConstants.JOB_EXECUTION_ID_FIELD, String.valueOf(this.jobExecutionId));
      properties.setProperty(TimingEvent.METADATA_MESSAGE, this.message);
      properties.setProperty(JobStatusRetriever.EVENT_NAME_FIELD, ExecutionStatus.RUNNING.name());
    } else {
      this.jobGroup = JobStatusRetriever.NA_KEY;
      properties.setProperty(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD, JobStatusRetriever.NA_KEY);
      properties.setProperty(JobStatusRetriever.EVENT_NAME_FIELD, ExecutionStatus.COMPILED.name());
    }
    properties.setProperty(TimingEvent.METADATA_START_TIME, "1");
    properties.setProperty(TimingEvent.METADATA_END_TIME, "2");
    State jobStatus = new State(properties);

    String storeName = Joiner.on(JobStatusRetriever.STATE_STORE_KEY_SEPARATION_CHARACTER).join(flowGroup, flowName);
    String tableName = Joiner.on(JobStatusRetriever.STATE_STORE_KEY_SEPARATION_CHARACTER).join(flowExecutionId, jobGroup, jobName, KafkaJobStatusMonitor.STATE_STORE_TABLE_SUFFIX);

    this.fsStateStore.put(storeName, tableName, jobStatus);
  }

  @Test
  public void testGetJobStatusesForFlowExecution() throws IOException {
    Long flowExecutionId = 1234L;
    addJobStatusToStateStore(flowExecutionId, JobStatusRetriever.NA_KEY);

    Iterator<JobStatus> jobStatusIterator = this.jobStatusRetriever.getJobStatusesForFlowExecution(flowName, flowGroup, flowExecutionId);
    Assert.assertTrue(jobStatusIterator.hasNext());
    JobStatus jobStatus = jobStatusIterator.next();
    Assert.assertEquals(jobStatus.getEventName(), ExecutionStatus.COMPILED.name());
    Assert.assertEquals(jobStatus.getJobName(), (JobStatusRetriever.NA_KEY));
    Assert.assertEquals(jobStatus.getJobGroup(), JobStatusRetriever.NA_KEY);
    Assert.assertEquals(jobStatus.getProcessedCount(), 0);
    Assert.assertEquals(jobStatus.getLowWatermark(), "");
    Assert.assertEquals(jobStatus.getHighWatermark(), "");

    addJobStatusToStateStore(flowExecutionId,"myJobName1");
    jobStatusIterator = this.jobStatusRetriever.getJobStatusesForFlowExecution(flowName, flowGroup, flowExecutionId);
    jobStatus = jobStatusIterator.next();
    Assert.assertEquals(jobStatus.getEventName(), ExecutionStatus.RUNNING.name());
    Assert.assertEquals(jobStatus.getJobName(), "myJobName1");
    Assert.assertEquals(jobStatus.getJobGroup(), jobGroup);
    Assert.assertFalse(jobStatusIterator.hasNext());

    addJobStatusToStateStore(flowExecutionId,"myJobName2");
    jobStatusIterator = this.jobStatusRetriever.getJobStatusesForFlowExecution(flowName, flowGroup, flowExecutionId);
    Assert.assertTrue(jobStatusIterator.hasNext());
    jobStatus = jobStatusIterator.next();
    Assert.assertTrue(jobStatus.getJobName().equals("myJobName1") || jobStatus.getJobName().equals("myJobName2"));

    String jobName = jobStatus.getJobName();
    String nextExpectedJobName = ("myJobName1".equals(jobName)) ? "myJobName2" : "myJobName1";
    Assert.assertTrue(jobStatusIterator.hasNext());
    jobStatus = jobStatusIterator.next();
    Assert.assertEquals(jobStatus.getJobName(), nextExpectedJobName);
  }

  @Test (dependsOnMethods = "testGetJobStatusesForFlowExecution")
  public void testGetJobStatusesForFlowExecution1() {
    long flowExecutionId = 1234L;
    String jobName = "myJobName1";
    String jobGroup = "myJobGroup";
    Iterator<JobStatus> jobStatusIterator = this.jobStatusRetriever.getJobStatusesForFlowExecution(flowName, flowGroup, flowExecutionId, jobName, jobGroup);

    Assert.assertTrue(jobStatusIterator.hasNext());
    JobStatus jobStatus = jobStatusIterator.next();
    Assert.assertEquals(jobStatus.getJobName(), jobName);
    Assert.assertEquals(jobStatus.getJobGroup(), jobGroup);
    Assert.assertEquals(jobStatus.getJobExecutionId(), jobExecutionId);
    Assert.assertEquals(jobStatus.getFlowName(), flowName);
    Assert.assertEquals(jobStatus.getFlowGroup(), flowGroup);
    Assert.assertEquals(jobStatus.getFlowExecutionId(), flowExecutionId);
    Assert.assertEquals(jobStatus.getMessage(), message);
  }

  @Test (dependsOnMethods = "testGetJobStatusesForFlowExecution1")
  public void testGetLatestExecutionIdForFlow() throws Exception {
    //Add new flow execution to state store
    long flowExecutionId = 1235L;
    addJobStatusToStateStore(flowExecutionId, "myJobName1");
    long latestExecutionIdForFlow = this.jobStatusRetriever.getLatestExecutionIdForFlow(flowName, flowGroup);
    Assert.assertEquals(latestExecutionIdForFlow, flowExecutionId);

    //Remove all flow executions from state store
    cleanUpDir(stateStoreDir);
    latestExecutionIdForFlow = this.jobStatusRetriever.getLatestExecutionIdForFlow(flowName, flowGroup);
    Assert.assertEquals(latestExecutionIdForFlow, -1L);
  }

  private void cleanUpDir(String dir) throws Exception {
    File specStoreDir = new File(dir);
    if (specStoreDir.exists()) {
      FileUtils.deleteDirectory(specStoreDir);
    }
  }

  @AfterClass
  public void tearDown() throws Exception {
    cleanUpDir(stateStoreDir);
  }
}