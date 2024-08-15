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
package org.apache.gobblin.service.modules.orchestration;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;

import org.apache.commons.io.FileUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Iterators;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.spec_executorInstance.MockedSpecExecutor;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.spec.JobExecutionPlanDagFactory;
import org.apache.gobblin.service.monitoring.JobStatus;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;
import org.apache.gobblin.util.ConfigUtils;


public class DagManagerTest {
  private final String dagStateStoreDir = "/tmp/dagManagerTest/dagStateStore";
  private JobStatusRetriever _jobStatusRetriever;
  private MetricContext metricContext;

  @BeforeClass
  public void setUp() throws Exception {
    FileUtils.deleteDirectory(new File(this.dagStateStoreDir));
    this._jobStatusRetriever = Mockito.mock(JobStatusRetriever.class);
    this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(ConfigFactory.empty()), getClass());
    DagManagerMetrics dagManagerMetrics = new DagManagerMetrics();
    dagManagerMetrics.activate();
  }

  public static Dag<JobExecutionPlan> buildDag(String id, Long flowExecutionId, String flowFailureOption, int numNodes, String proxyUser, Config additionalConfig)
      throws URISyntaxException {
    if (additionalConfig.hasPath(ConfigurationKeys.JOB_NAME_KEY)) {
      throw new RuntimeException("Please do not set " + ConfigurationKeys.JOB_NAME_KEY + " because this method is "
          + "using hard coded job names in setting " + ConfigurationKeys.JOB_DEPENDENCIES);
    }

    List<JobExecutionPlan> jobExecutionPlans = new ArrayList<>();

    for (int i = 0; i < numNodes; i++) {
      String suffix = Integer.toString(i);
      Config jobConfig = ConfigBuilder.create().
          addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "group" + id).
          addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "flow" + id).
          addPrimitive(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, flowExecutionId).
          addPrimitive(ConfigurationKeys.JOB_GROUP_KEY, "group" + id).
          addPrimitive(ConfigurationKeys.JOB_NAME_KEY, "job" + suffix).
          addPrimitive(ConfigurationKeys.FLOW_FAILURE_OPTION, flowFailureOption).
          addPrimitive(AzkabanProjectConfig.USER_TO_PROXY, proxyUser).build();
      jobConfig = additionalConfig.withFallback(jobConfig);
      if ((i == 1) || (i == 2)) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job0"));
      } else if (i == 3) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job1"));
      } else if (i == 4) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job2"));
      }
      JobSpec js = JobSpec.builder("test_job" + suffix).withVersion(suffix).withConfig(jobConfig).
          withTemplate(new URI("job" + suffix)).build();
      SpecExecutor specExecutor = MockedSpecExecutor.createDummySpecExecutor(new URI(
          ConfigUtils.getString(additionalConfig, ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY,"job" + i)));
      JobExecutionPlan jobExecutionPlan = new JobExecutionPlan(js, specExecutor);
      jobExecutionPlans.add(jobExecutionPlan);
    }
    return new JobExecutionPlanDagFactory().createDag(jobExecutionPlans);
  }

  static Iterator<JobStatus> getMockFlowStatus(String flowName, String flowGroup, Long flowExecutionId, String eventName) {
    return getMockJobStatus(flowName, flowGroup, flowExecutionId, JobStatusRetriever.NA_KEY, JobStatusRetriever.NA_KEY, eventName);
  }

  static Iterator<JobStatus> getMockJobStatus(String flowName, String flowGroup, Long flowExecutionId, String jobGroup, String jobName, String eventName) {
    return getMockJobStatus(flowName, flowGroup, flowExecutionId, jobGroup, jobName, eventName, false, flowExecutionId + 10);
  }

  private static Iterator<JobStatus> getMockJobStatus(Long flowExecutionId, String eventName, boolean shouldRetry) {
    return getMockJobStatus("flow0", "group0", flowExecutionId,
        "job0", "group0", eventName, shouldRetry, flowExecutionId + 10);
  }
  private static Iterator<JobStatus> getMockJobStatus(String flowName, String flowGroup,  Long flowExecutionId, String jobGroup, String jobName, String eventName, boolean shouldRetry, Long orchestratedTime) {
    return Iterators.singletonIterator(JobStatus.builder().flowName(flowName).flowGroup(flowGroup).jobGroup(jobGroup).jobName(jobName).flowExecutionId(flowExecutionId).
        message("Test message").eventName(eventName).startTime(flowExecutionId + 10).shouldRetry(shouldRetry).orchestratedTime(orchestratedTime).build());
  }

  @Test (dependsOnMethods = "testDagManagerWithBadFlowSLAConfig", enabled = false)
  // todo re-write for dag proc
  public void testDagManagerQuotaExceeded() throws URISyntaxException {
    List<Dag<JobExecutionPlan>> dagList = DagTestUtils.buildDagList(2, "user", ConfigFactory.empty());
    //Add a dag to the queue of dags
    Config jobConfig0 = dagList.get(0).getNodes().get(0).getValue().getJobSpec().getConfig();
    Config jobConfig1 = dagList.get(1).getNodes().get(0).getValue().getJobSpec().getConfig();
    Iterator<JobStatus> jobStatusIteratorFlow0_0 =
        getMockJobStatus("flow0", "group0", Long.valueOf(jobConfig0.getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)),
            "job0", "group0", String.valueOf(ExecutionStatus.RUNNING));
    Iterator<JobStatus> jobStatusIteratorFlow1_0 =
        getMockJobStatus("flow1", "group1", Long.valueOf(jobConfig1.getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)),
            "job0", "group1", String.valueOf(ExecutionStatus.FAILED));
    Iterator<JobStatus> jobStatusIteratorFlow1_1 =
        getMockFlowStatus("flow1", "group1", Long.valueOf(jobConfig1.getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)), String.valueOf(ExecutionStatus.FAILED));
    // Cleanup the running job that is scheduled normally
    Iterator<JobStatus> jobStatusIteratorFlow0_1 =
        getMockJobStatus("flow0", "group0", Long.valueOf(jobConfig0.getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)),
            "job0", "group0", String.valueOf(ExecutionStatus.RUNNING));
    Iterator<JobStatus> jobStatusIteratorFlow0_2 =
        getMockJobStatus("flow0", "group0", Long.valueOf(jobConfig0.getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)),
            "job0", "group0", String.valueOf(ExecutionStatus.COMPLETE));
    Iterator<JobStatus> jobStatusIteratorFlow0_3 =
        getMockFlowStatus("flow0", "group0", Long.valueOf(jobConfig0.getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)), String.valueOf(ExecutionStatus.COMPLETE));
    Mockito.when(_jobStatusRetriever
        .getJobStatusesForFlowExecution(Mockito.eq("flow0"), Mockito.eq("group0"), Mockito.anyLong(),
            Mockito.anyString(), Mockito.anyString()))
        .thenReturn(jobStatusIteratorFlow0_0)
        .thenReturn(jobStatusIteratorFlow0_1)
        .thenReturn(jobStatusIteratorFlow0_2)
        .thenReturn(jobStatusIteratorFlow0_3);

    Mockito.when(_jobStatusRetriever
        .getJobStatusesForFlowExecution(Mockito.eq("flow1"), Mockito.eq("group1"), Mockito.anyLong(),
            Mockito.anyString(), Mockito.anyString()))
        .thenReturn(jobStatusIteratorFlow1_0)
        .thenReturn(jobStatusIteratorFlow1_1);

    // dag will not be processed due to exceeding the quota, will log a message and exit out without adding it to dags
    SortedMap<String, Counter> allCounters = metricContext.getParent().get().getCounters();
    Assert.assertEquals(allCounters.get(MetricRegistry.name(
        ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,
        ServiceMetricNames.SERVICE_USERS,
        "user")).getCount(), 1);
  }

  @Test (dependsOnMethods = "testDagManagerQuotaExceeded", enabled = false)
  // todo re-write for dag proc
  public void testQuotaDecrement() throws URISyntaxException {
    List<Dag<JobExecutionPlan>> dagList = DagTestUtils.buildDagList(3, "user", ConfigFactory.empty());
    //Add a dag to the queue of dags
    Config jobConfig0 = dagList.get(0).getNodes().get(0).getValue().getJobSpec().getConfig();
    Config jobConfig1 = dagList.get(1).getNodes().get(0).getValue().getJobSpec().getConfig();
    Config jobConfig2 = dagList.get(1).getNodes().get(0).getValue().getJobSpec().getConfig();

    Iterator<JobStatus> jobStatusIteratorFlow0_0 =
        getMockJobStatus("flow0", "group0", Long.valueOf(jobConfig0.getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)),
            "job0", "group0", String.valueOf(ExecutionStatus.RUNNING));
    Iterator<JobStatus> jobStatusIteratorFlow1_0 =
        getMockJobStatus("flow1", "group1", Long.valueOf(jobConfig1.getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)),
            "job0", "group1", String.valueOf(ExecutionStatus.FAILED));
    Iterator<JobStatus> jobStatusIteratorFlow0_1 =
        getMockJobStatus("flow0", "group0", Long.valueOf(jobConfig0.getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)),
            "job0", "group0", String.valueOf(ExecutionStatus.RUNNING));
    Iterator<JobStatus> jobStatusIteratorFlow0_2 =
        getMockJobStatus("flow0", "group0", Long.valueOf(jobConfig0.getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)),
            "job0", "group0", String.valueOf(ExecutionStatus.COMPLETE));
    Iterator<JobStatus> jobStatusIteratorFlow2_0 =
        getMockJobStatus("flow2", "group2", Long.valueOf(jobConfig2.getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)),
            "job0", "group2", String.valueOf(ExecutionStatus.FAILED));
    Iterator<JobStatus> jobStatusIteratorFlow0_3 =
        getMockFlowStatus("flow0", "group0", Long.valueOf(jobConfig0.getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)), String.valueOf(ExecutionStatus.COMPLETE));
    Iterator<JobStatus> jobStatusIteratorFlow1_1 =
        getMockFlowStatus("flow1", "group2", Long.valueOf(jobConfig0.getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)), String.valueOf(ExecutionStatus.FAILED));
    Iterator<JobStatus> jobStatusIteratorFlow2_1 =
        getMockFlowStatus("flow1", "group2", Long.valueOf(jobConfig0.getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)), String.valueOf(ExecutionStatus.FAILED));
    Mockito.when(_jobStatusRetriever
        .getJobStatusesForFlowExecution(Mockito.eq("flow0"), Mockito.eq("group0"), Mockito.anyLong(),
            Mockito.anyString(), Mockito.anyString()))
        .thenReturn(jobStatusIteratorFlow0_0)
        .thenReturn(jobStatusIteratorFlow0_1)
        .thenReturn(jobStatusIteratorFlow0_2)
        .thenReturn(jobStatusIteratorFlow0_3);
    Mockito.when(_jobStatusRetriever
        .getJobStatusesForFlowExecution(Mockito.eq("flow1"), Mockito.eq("group1"), Mockito.anyLong(),
            Mockito.anyString(), Mockito.anyString()))
        .thenReturn(jobStatusIteratorFlow1_0)
        .thenReturn(jobStatusIteratorFlow1_1);
    Mockito.when(_jobStatusRetriever
        .getJobStatusesForFlowExecution(Mockito.eq("flow2"), Mockito.eq("group2"), Mockito.anyLong(),
            Mockito.anyString(), Mockito.anyString()))
        .thenReturn(jobStatusIteratorFlow2_0)
        .thenReturn(jobStatusIteratorFlow2_1);


    SortedMap<String, Counter> allCounters = metricContext.getParent().get().getCounters();
    Assert.assertEquals(allCounters.get(MetricRegistry.name(
        ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,
        ServiceMetricNames.SERVICE_USERS,
        "user")).getCount(), 1);
    // Test case where a job that exceeded a quota would cause a double decrement after fixing the proxy user name, allowing for more jobs to run

    // Assert that running dag metrics are only counted once
    Assert.assertEquals(allCounters.get(MetricRegistry.name(
        ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,
        ServiceMetricNames.SERVICE_USERS,
        "user")).getCount(), 1);

    Assert.assertEquals(allCounters.get(MetricRegistry.name(
        ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,
        ServiceMetricNames.SERVICE_USERS,
        "user")).getCount(), 0);
  }

  @Test (dependsOnMethods = "testQuotaDecrement", enabled = false)
  // todo re-write for dag proc
  public void testQuotasRetryFlow() throws URISyntaxException {
    List<Dag<JobExecutionPlan>> dagList = DagTestUtils.buildDagList(2, "user", ConfigFactory.empty());
    //Add a dag to the queue of dags
    Config jobConfig0 = dagList.get(0).getNodes().get(0).getValue().getJobSpec().getConfig();
    Config jobConfig1 = dagList.get(1).getNodes().get(0).getValue().getJobSpec().getConfig();
    Iterator<JobStatus> jobStatusIteratorFlow0_0 =
        getMockJobStatus(Long.valueOf(jobConfig0.getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)), String.valueOf(ExecutionStatus.ORCHESTRATED), true);
    // Cleanup the running job that is scheduled normally
    Iterator<JobStatus> jobStatusIteratorFlow0_1 =
        getMockJobStatus(Long.valueOf(jobConfig0.getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)), String.valueOf(ExecutionStatus.RUNNING), true);
    Iterator<JobStatus> jobStatusIteratorFlow0_2 =
        getMockJobStatus("flow0", "group0", Long.valueOf(jobConfig0.getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)),
            "job0", "group0", String.valueOf(ExecutionStatus.ORCHESTRATED));
    Iterator<JobStatus> jobStatusIteratorFlow0_3 =
        getMockJobStatus("flow0", "group0", Long.valueOf(jobConfig0.getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)),
            "job0", "group0", String.valueOf(ExecutionStatus.COMPLETE));
    Iterator<JobStatus> jobStatusIteratorFlow0_4 =
        getMockFlowStatus("flow0", "group0", Long.valueOf(jobConfig0.getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)), String.valueOf(ExecutionStatus.COMPLETE));
    Iterator<JobStatus> jobStatusIteratorFlow1_0 =
        getMockJobStatus("flow1", "group1", Long.valueOf(jobConfig1.getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)),
            "job0", "group1", String.valueOf(ExecutionStatus.ORCHESTRATED));
    Iterator<JobStatus> jobStatusIteratorFlow1_1 =
        getMockJobStatus("flow1", "group1", Long.valueOf(jobConfig1.getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)),
            "job0", "group1", String.valueOf(ExecutionStatus.COMPLETE));
    Iterator<JobStatus> jobStatusIteratorFlow1_2 =
        getMockFlowStatus("flow1", "group1", Long.valueOf(jobConfig1.getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)), String.valueOf(ExecutionStatus.COMPLETE));
    Mockito.when(_jobStatusRetriever
        .getJobStatusesForFlowExecution(Mockito.eq("flow0"), Mockito.eq("group0"), Mockito.anyLong(),
            Mockito.anyString(), Mockito.anyString()))
        .thenReturn(jobStatusIteratorFlow0_0)
        .thenReturn(jobStatusIteratorFlow0_1)
        .thenReturn(jobStatusIteratorFlow0_2)
        .thenReturn(jobStatusIteratorFlow0_3)
        .thenReturn(jobStatusIteratorFlow0_4);

    Mockito.when(_jobStatusRetriever
        .getJobStatusesForFlowExecution(Mockito.eq("flow1"), Mockito.eq("group1"), Mockito.anyLong(),
            Mockito.anyString(), Mockito.anyString()))
        .thenReturn(jobStatusIteratorFlow1_0)
        .thenReturn(jobStatusIteratorFlow1_1)
        .thenReturn(jobStatusIteratorFlow1_2);

    SortedMap<String, Counter> allCounters = metricContext.getParent().get().getCounters();
    Assert.assertEquals(allCounters.get(MetricRegistry.name(
        ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,
        ServiceMetricNames.SERVICE_USERS,
        "user")).getCount(), 1);

    Assert.assertEquals(allCounters.get(MetricRegistry.name(
        ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,
        ServiceMetricNames.SERVICE_USERS,
        "user")).getCount(), 1);

    Assert.assertEquals(allCounters.get(MetricRegistry.name(
        ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,
        ServiceMetricNames.SERVICE_USERS,
        "user")).getCount(), 0);
  }

  @Test (dependsOnMethods = "testEmitFlowMetricOnlyIfNotAdhoc", enabled = false)
  // todo re-write for dag proc
  public void testJobSlaKilledMetrics() throws URISyntaxException {
    long flowExecutionId = System.currentTimeMillis() - Duration.ofMinutes(20).toMillis();
    Config executorOneConfig = ConfigFactory.empty()
        .withValue(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY, ConfigValueFactory.fromAnyRef("executorOne"))
        .withValue(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, ConfigValueFactory.fromAnyRef(flowExecutionId))
        .withValue(ConfigurationKeys.GOBBLIN_FLOW_SLA_TIME, ConfigValueFactory.fromAnyRef(10))
        .withValue(ConfigurationKeys.GOBBLIN_OUTPUT_JOB_LEVEL_METRICS, ConfigValueFactory.fromAnyRef(true));
    Config executorTwoConfig = ConfigFactory.empty()
        .withValue(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY, ConfigValueFactory.fromAnyRef("executorTwo"))
        .withValue(ConfigurationKeys.GOBBLIN_FLOW_SLA_TIME, ConfigValueFactory.fromAnyRef(10))
        .withValue(ConfigurationKeys.GOBBLIN_OUTPUT_JOB_LEVEL_METRICS, ConfigValueFactory.fromAnyRef(true));

    List<Dag<JobExecutionPlan>> dagList = DagTestUtils.buildDagList(2, "newUser", executorOneConfig);
    dagList.add(buildDag("2", flowExecutionId, "FINISH_RUNNING", 1, "newUser", executorTwoConfig));

    String allSlaKilledMeterName = MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,  ServiceMetricNames.SLA_EXCEEDED_FLOWS_METER);
    long previousSlaKilledCount = metricContext.getParent().get().getMeters().get(allSlaKilledMeterName) == null ? 0 :
        metricContext.getParent().get().getMeters().get(allSlaKilledMeterName).getCount();

    //Add a dag to the queue of dags
    // Set orchestration time to be 20 minutes in the past, the job should be marked as SLA killed
    Iterator<JobStatus> jobStatusIteratorFlow0_0 =
        getMockJobStatus("flow0", "group0", flowExecutionId, "job0", "group0", String.valueOf(ExecutionStatus.RUNNING),
            false, flowExecutionId);
    Iterator<JobStatus> jobStatusIteratorFlow1_0 =
        getMockJobStatus("flow1", "group1", flowExecutionId, "job0", "group1", String.valueOf(ExecutionStatus.RUNNING),
            false, flowExecutionId);
    Iterator<JobStatus> jobStatusIteratorFlow2_0 =
        getMockJobStatus("flow2", "group2", flowExecutionId, "job0", "group2", String.valueOf(ExecutionStatus.RUNNING),
            false, flowExecutionId);
    Iterator<JobStatus> jobStatusIteratorFlow0_1 =
        getMockJobStatus("flow0", "group0", flowExecutionId, "job0", "group0", String.valueOf(ExecutionStatus.CANCELLED),
            false, flowExecutionId);
    Iterator<JobStatus> jobStatusIteratorFlow1_1 =
        getMockJobStatus("flow1", "group1", flowExecutionId, "job0", "group1", String.valueOf(ExecutionStatus.CANCELLED),
            false, flowExecutionId);
    Iterator<JobStatus> jobStatusIteratorFlow2_1 =
        getMockJobStatus("flow2", "group2", flowExecutionId, "job0", "group2", String.valueOf(ExecutionStatus.CANCELLED),
            false, flowExecutionId);
    Iterator<JobStatus> jobStatusIteratorFlow0_2 =
        getMockFlowStatus("flow0", "group0", flowExecutionId, String.valueOf(ExecutionStatus.CANCELLED));
    Iterator<JobStatus> jobStatusIteratorFlow1_2 =
        getMockFlowStatus("flow1", "group1", flowExecutionId, String.valueOf(ExecutionStatus.CANCELLED));
    Iterator<JobStatus> jobStatusIteratorFlow2_2 =
        getMockFlowStatus("flow2", "group2", flowExecutionId, String.valueOf(ExecutionStatus.CANCELLED));

    Mockito.when(_jobStatusRetriever
        .getJobStatusesForFlowExecution(Mockito.eq("flow0"), Mockito.eq("group0"), Mockito.anyLong(), Mockito.anyString(), Mockito.anyString())).
        thenReturn(jobStatusIteratorFlow0_0).
        thenReturn(jobStatusIteratorFlow0_1).
        thenReturn(jobStatusIteratorFlow0_2);

    Mockito.when(_jobStatusRetriever
        .getJobStatusesForFlowExecution(Mockito.eq("flow1"), Mockito.eq("group1"), Mockito.anyLong(), Mockito.anyString(), Mockito.anyString())).
        thenReturn(jobStatusIteratorFlow1_0).
        thenReturn(jobStatusIteratorFlow1_1).
        thenReturn(jobStatusIteratorFlow1_2);

    Mockito.when(_jobStatusRetriever
    .getJobStatusesForFlowExecution(Mockito.eq("flow2"), Mockito.eq("group2"), Mockito.anyLong(), Mockito.anyString(), Mockito.anyString())).
        thenReturn(jobStatusIteratorFlow2_0).
        thenReturn(jobStatusIteratorFlow2_1).
        thenReturn(jobStatusIteratorFlow2_2);

    String slakilledMeterName1 = MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX, "executorOne", ServiceMetricNames.SLA_EXCEEDED_FLOWS_METER);
    String slakilledMeterName2 = MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX, "executorTwo", ServiceMetricNames.SLA_EXCEEDED_FLOWS_METER);
    String failedFlowGauge = MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX, "group1","flow1", ServiceMetricNames.RUNNING_STATUS);

    Assert.assertEquals(metricContext.getParent().get().getMeters().get(slakilledMeterName1).getCount(), 2);
    Assert.assertEquals(metricContext.getParent().get().getMeters().get(slakilledMeterName2).getCount(), 1);

    Assert.assertEquals(metricContext.getParent().get().getMeters().get(allSlaKilledMeterName).getCount(), previousSlaKilledCount + 3);
    Assert.assertEquals(metricContext.getParent().get().getGauges().get(failedFlowGauge).getValue(), -1);
  }

  @Test (dependsOnMethods = "testJobSlaKilledMetrics", enabled = false)
  // todo re-write for dag proc
  public void testPerExecutorMetricsSuccessFails() throws URISyntaxException {
    long flowExecutionId = System.currentTimeMillis();
    Config executorOneConfig = ConfigFactory.empty()
        .withValue(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY, ConfigValueFactory.fromAnyRef("executorOne"))
        .withValue(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, ConfigValueFactory.fromAnyRef(flowExecutionId));
    Config executorTwoConfig = ConfigFactory.empty()
        .withValue(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY, ConfigValueFactory.fromAnyRef("executorTwo"));
    List<Dag<JobExecutionPlan>> dagList = DagTestUtils.buildDagList(2, "newUser", executorOneConfig);
    dagList.add(buildDag("2", flowExecutionId, "FINISH_RUNNING", 1, "newUser", executorTwoConfig));
    // Get global metric count before any changes are applied
    String allSuccessfulFlowsMeterName = MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,  ServiceMetricNames.SUCCESSFUL_FLOW_METER);
    long previousSuccessCount = metricContext.getParent().get().getMeters().get(allSuccessfulFlowsMeterName) == null ? 0 :
        metricContext.getParent().get().getMeters().get(allSuccessfulFlowsMeterName).getCount();
    String previousJobSentToExecutorMeterName = MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,  "executorOne", ServiceMetricNames.JOBS_SENT_TO_SPEC_EXECUTOR);
    long previousJobSentToExecutorCount = metricContext.getParent().get().getMeters().get(previousJobSentToExecutorMeterName) == null ? 0 :
        metricContext.getParent().get().getMeters().get(previousJobSentToExecutorMeterName).getCount();

    //Add a dag to the queue of dags
    // The start time should be 16 minutes ago, which is past the start SLA so the job should be cancelled
    Iterator<JobStatus> jobStatusIteratorFlow0_0 =
        getMockJobStatus( "flow0", "group0", flowExecutionId, "job0", "group0", String.valueOf(ExecutionStatus.ORCHESTRATED),
            false, flowExecutionId);
    Iterator<JobStatus> jobStatusIteratorFlow1_0 =
        getMockJobStatus("flow1", "group1", flowExecutionId+1, "job0", "group1", String.valueOf(ExecutionStatus.ORCHESTRATED),
            false, flowExecutionId);
    Iterator<JobStatus> jobStatusIteratorFlow2_0 =
        getMockJobStatus("flow2", "group2", flowExecutionId+1, "job0", "group2", String.valueOf(ExecutionStatus.ORCHESTRATED),
            false, flowExecutionId);
    Iterator<JobStatus> jobStatusIteratorFlow0_1 =
        getMockJobStatus( "flow0", "group0", flowExecutionId+1, "job0", "group0", String.valueOf(ExecutionStatus.COMPLETE),
            false, flowExecutionId);
    Iterator<JobStatus> jobStatusIteratorFlow1_1 =
        getMockJobStatus("flow1", "group1", flowExecutionId+1, "job0", "group1", String.valueOf(ExecutionStatus.FAILED),
            false, flowExecutionId);
    Iterator<JobStatus> jobStatusIteratorFlow2_1 =
        getMockJobStatus("flow2", "group2", flowExecutionId+1, "job0", "group2", String.valueOf(ExecutionStatus.COMPLETE),
            false, flowExecutionId);
    Iterator<JobStatus> jobStatusIteratorFlow0_2 =
        getMockFlowStatus( "flow0", "group0", flowExecutionId+1, String.valueOf(ExecutionStatus.COMPLETE));
    Iterator<JobStatus> jobStatusIteratorFlow1_2 =
        getMockFlowStatus("flow1", "group1", flowExecutionId+1, String.valueOf(ExecutionStatus.FAILED));
    Iterator<JobStatus> jobStatusIteratorFlow2_2 =
        getMockFlowStatus("flow2", "group2", flowExecutionId+1, String.valueOf(ExecutionStatus.COMPLETE));

    Mockito.when(_jobStatusRetriever
        .getJobStatusesForFlowExecution(Mockito.eq("flow0"), Mockito.eq("group0"), Mockito.anyLong(), Mockito.anyString(), Mockito.anyString())).
        thenReturn(jobStatusIteratorFlow0_0).
        thenReturn(jobStatusIteratorFlow0_1).
        thenReturn(jobStatusIteratorFlow0_2);

    Mockito.when(_jobStatusRetriever
        .getJobStatusesForFlowExecution(Mockito.eq("flow1"), Mockito.eq("group1"), Mockito.anyLong(), Mockito.anyString(), Mockito.anyString())).
        thenReturn(jobStatusIteratorFlow1_0).
        thenReturn(jobStatusIteratorFlow1_1).
        thenReturn(jobStatusIteratorFlow1_2);

    Mockito.when(_jobStatusRetriever
        .getJobStatusesForFlowExecution(Mockito.eq("flow2"), Mockito.eq("group2"), Mockito.anyLong(), Mockito.anyString(), Mockito.anyString())).
        thenReturn(jobStatusIteratorFlow2_0).
        thenReturn(jobStatusIteratorFlow2_1).
        thenReturn(jobStatusIteratorFlow2_2);

    String slaSuccessfulFlowsExecutorOneMeterName = MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX, "executorOne", ServiceMetricNames.SUCCESSFUL_FLOW_METER);
    String slaFailedFlowsExecutorOneMeterName = MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX, "executorOne", ServiceMetricNames.FAILED_FLOW_METER);
    String failedFlowGauge = MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX, "group1", "flow1", ServiceMetricNames.RUNNING_STATUS);

    Assert.assertEquals(metricContext.getParent().get().getMeters().get(slaSuccessfulFlowsExecutorOneMeterName).getCount(), 1);
    Assert.assertEquals(metricContext.getParent().get().getMeters().get(slaFailedFlowsExecutorOneMeterName).getCount(), 1);
    Assert.assertEquals(metricContext.getParent().get().getMeters().get(allSuccessfulFlowsMeterName).getCount(), previousSuccessCount + 2);
    Assert.assertEquals(metricContext.getParent().get().getMeters().get(previousJobSentToExecutorMeterName).getCount(), previousJobSentToExecutorCount + 2);
    Assert.assertEquals(metricContext.getParent().get().getGauges().get(failedFlowGauge).getValue(), -1);
    // Cleanup
  }

  @AfterClass
  public void cleanUp() throws Exception {
    FileUtils.deleteDirectory(new File(this.dagStateStoreDir));
  }
}