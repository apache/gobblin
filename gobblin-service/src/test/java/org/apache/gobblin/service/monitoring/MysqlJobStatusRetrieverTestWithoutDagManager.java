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

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.MysqlJobStatusStateStore;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.runtime.troubleshooter.MultiContextIssueRepository;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.ServiceConfigKeys;

import static org.mockito.Mockito.mock;


/**
 * Flow status can be different when DagManager is not being used. So we need separate unit tests for testing job/flow
 * status when DagManager is disabled.
 */
public class MysqlJobStatusRetrieverTestWithoutDagManager extends JobStatusRetrieverTest {
  private MysqlJobStatusStateStore<State> dbJobStateStore;
  private static final String TEST_USER = "testUser";
  private static final String TEST_PASSWORD = "testPassword";

  @BeforeClass
  @Override
  public void setUp() throws Exception {
    ITestMetastoreDatabase testMetastoreDatabase = TestMetastoreDatabaseFactory.get();
    String jdbcUrl = testMetastoreDatabase.getJdbcUrl();

    ConfigBuilder configBuilder = ConfigBuilder.create();
    configBuilder.addPrimitive(MysqlJobStatusRetriever.MYSQL_JOB_STATUS_RETRIEVER_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_URL_KEY, jdbcUrl);
    configBuilder.addPrimitive(MysqlJobStatusRetriever.MYSQL_JOB_STATUS_RETRIEVER_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_USER_KEY, TEST_USER);
    configBuilder.addPrimitive(MysqlJobStatusRetriever.MYSQL_JOB_STATUS_RETRIEVER_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, TEST_PASSWORD);

    configBuilder.addPrimitive(ServiceConfigKeys.GOBBLIN_SERVICE_DAG_MANAGER_ENABLED_KEY, "false");
    this.jobStatusRetriever =
        new MysqlJobStatusRetriever(configBuilder.build(), mock(MultiContextIssueRepository.class));
    this.dbJobStateStore = ((MysqlJobStatusRetriever) this.jobStatusRetriever).getStateStore();
    cleanUpDir();
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
        JobStatusRetriever.getFlowStatusFromJobStatuses(jobStatusRetriever.dagManagerEnabled, jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId)));

    addJobStatusToStateStore(flowExecutionId, JobStatusRetriever.NA_KEY, ExecutionStatus.ORCHESTRATED.name());
    Assert.assertEquals(ExecutionStatus.$UNKNOWN,
        JobStatusRetriever.getFlowStatusFromJobStatuses(jobStatusRetriever.dagManagerEnabled, jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId)));

    addJobStatusToStateStore(flowExecutionId, MY_JOB_NAME_1, ExecutionStatus.ORCHESTRATED.name(), JOB_ORCHESTRATED_TIME, JOB_ORCHESTRATED_TIME);
    Assert.assertEquals(ExecutionStatus.ORCHESTRATED,
        JobStatusRetriever.getFlowStatusFromJobStatuses(jobStatusRetriever.dagManagerEnabled, jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId)));

    addJobStatusToStateStore(flowExecutionId, JobStatusRetriever.NA_KEY, ExecutionStatus.RUNNING.name());
    Assert.assertEquals(ExecutionStatus.ORCHESTRATED,
        JobStatusRetriever.getFlowStatusFromJobStatuses(jobStatusRetriever.dagManagerEnabled, jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId)));

    addJobStatusToStateStore(flowExecutionId, MY_JOB_NAME_1, ExecutionStatus.RUNNING.name(), JOB_ORCHESTRATED_TIME, JOB_ORCHESTRATED_TIME);
    Assert.assertEquals(ExecutionStatus.RUNNING,
        JobStatusRetriever.getFlowStatusFromJobStatuses(jobStatusRetriever.dagManagerEnabled, jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId)));

    addJobStatusToStateStore(flowExecutionId, JobStatusRetriever.NA_KEY, ExecutionStatus.COMPLETE.name(), JOB_ORCHESTRATED_TIME, JOB_ORCHESTRATED_TIME);
    Assert.assertEquals(ExecutionStatus.RUNNING,
        JobStatusRetriever.getFlowStatusFromJobStatuses(jobStatusRetriever.dagManagerEnabled, jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId)));

    addJobStatusToStateStore(flowExecutionId, MY_JOB_NAME_1, ExecutionStatus.COMPLETE.name());
    Assert.assertEquals(ExecutionStatus.COMPLETE,
        JobStatusRetriever.getFlowStatusFromJobStatuses(jobStatusRetriever.dagManagerEnabled, jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, flowExecutionId)));
  }

  @Override
  void cleanUpDir() throws Exception {
    this.dbJobStateStore.delete(KafkaJobStatusMonitor.jobStatusStoreName(FLOW_GROUP, FLOW_NAME));
  }
}
