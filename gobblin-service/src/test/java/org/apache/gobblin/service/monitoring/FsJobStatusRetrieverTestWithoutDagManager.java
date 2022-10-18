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

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.troubleshooter.MultiContextIssueRepository;
import org.apache.gobblin.service.ExecutionStatus;

import static org.mockito.Mockito.mock;


public class FsJobStatusRetrieverTestWithoutDagManager extends JobStatusRetrieverTest {

  private String stateStoreDir = "/tmp/jobStatusRetrieverTest/statestore";

  @BeforeClass
  public void setUp() throws Exception {
    cleanUpDir();
    Config config = ConfigFactory.empty().withValue(FsJobStatusRetriever.CONF_PREFIX + "." + ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY,
        ConfigValueFactory.fromAnyRef(stateStoreDir));
    this.jobStatusRetriever = new FsJobStatusRetriever(config, mock(MultiContextIssueRepository.class));
  }

  @Test
  public void testGetJobStatusesForFlowExecution() throws IOException {
    super.testGetJobStatusesForFlowExecution();
  }

  @Test (dependsOnMethods = "testGetJobStatusesForFlowExecution")
  public void testJobTiming() throws Exception {
    super.testJobTiming();
  }

  @Test (dependsOnMethods = "testJobTiming")
  public void testOutOfOrderJobTimingEvents() throws IOException {
    super.testOutOfOrderJobTimingEvents();
  }

  @Test (dependsOnMethods = "testJobTiming")
  public void testGetJobStatusesForFlowExecution1() {
    super.testGetJobStatusesForFlowExecution1();
  }

  @Test (dependsOnMethods = "testGetJobStatusesForFlowExecution1")
  public void testGetLatestExecutionIdsForFlow() throws Exception {
    super.testGetLatestExecutionIdsForFlow();
  }

  @Test (dependsOnMethods = "testGetLatestExecutionIdsForFlow")
  public void testGetFlowStatusFromJobStatuses() throws Exception {
    long flowExecutionId = 1237L;

    addJobStatusToStateStore(flowExecutionId, JobStatusRetriever.NA_KEY, ExecutionStatus.COMPILED.name());
    Assert.assertEquals(ExecutionStatus.$UNKNOWN,
        jobStatusRetriever.getFlowStatusFromJobStatuses(jobStatusRetriever.dagManagerEnabled, jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId)));

    addJobStatusToStateStore(flowExecutionId, JobStatusRetriever.NA_KEY, ExecutionStatus.ORCHESTRATED.name());
    Assert.assertEquals(ExecutionStatus.$UNKNOWN,
        jobStatusRetriever.getFlowStatusFromJobStatuses(jobStatusRetriever.dagManagerEnabled, jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId)));

    addJobStatusToStateStore(flowExecutionId, MY_JOB_NAME_1, ExecutionStatus.ORCHESTRATED.name(), JOB_ORCHESTRATED_TIME, JOB_ORCHESTRATED_TIME);
    Assert.assertEquals(ExecutionStatus.ORCHESTRATED,
        jobStatusRetriever.getFlowStatusFromJobStatuses(jobStatusRetriever.dagManagerEnabled, jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId)));

    addJobStatusToStateStore(flowExecutionId, JobStatusRetriever.NA_KEY, ExecutionStatus.RUNNING.name());
    Assert.assertEquals(ExecutionStatus.ORCHESTRATED,
        jobStatusRetriever.getFlowStatusFromJobStatuses(jobStatusRetriever.dagManagerEnabled, jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId)));

    addJobStatusToStateStore(flowExecutionId, MY_JOB_NAME_1, ExecutionStatus.RUNNING.name(), JOB_ORCHESTRATED_TIME, JOB_ORCHESTRATED_TIME);
    Assert.assertEquals(ExecutionStatus.RUNNING,
        jobStatusRetriever.getFlowStatusFromJobStatuses(jobStatusRetriever.dagManagerEnabled, jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId)));

    addJobStatusToStateStore(flowExecutionId, JobStatusRetriever.NA_KEY, ExecutionStatus.COMPLETE.name(), JOB_ORCHESTRATED_TIME, JOB_ORCHESTRATED_TIME);
    Assert.assertEquals(ExecutionStatus.RUNNING,
        jobStatusRetriever.getFlowStatusFromJobStatuses(jobStatusRetriever.dagManagerEnabled, jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId)));

    addJobStatusToStateStore(flowExecutionId, MY_JOB_NAME_1, ExecutionStatus.COMPLETE.name());
    Assert.assertEquals(ExecutionStatus.COMPLETE,
        jobStatusRetriever.getFlowStatusFromJobStatuses(jobStatusRetriever.dagManagerEnabled, jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId)));
  }

  @Override
  protected void cleanUpDir() throws Exception {
    File specStoreDir = new File(this.stateStoreDir);
    if (specStoreDir.exists()) {
      FileUtils.deleteDirectory(specStoreDir);
    }
  }
}