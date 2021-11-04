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

package org.apache.gobblin.service;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Suppliers;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.restli.EmbeddedRestliServer;
import org.apache.gobblin.runtime.troubleshooter.MultiContextIssueRepository;
import org.apache.gobblin.service.monitoring.FlowStatusGenerator;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;

import static org.mockito.Mockito.mock;


@Test(groups = { "gobblin.service" }, singleThreaded = true)
public class FlowStatusTest {
  private FlowStatusClient _client;
  private EmbeddedRestliServer _server;
  private List<List<org.apache.gobblin.service.monitoring.JobStatus>> _listOfJobStatusLists;

  class TestJobStatusRetriever extends JobStatusRetriever {

    protected TestJobStatusRetriever(MultiContextIssueRepository issueRepository) {
      super(ServiceConfigKeys.DEFAULT_GOBBLIN_SERVICE_DAG_MANAGER_ENABLED, issueRepository);
    }

    @Override
    public Iterator<org.apache.gobblin.service.monitoring.JobStatus> getJobStatusesForFlowExecution(String flowName,
        String flowGroup, long flowExecutionId) {
      return _listOfJobStatusLists.get((int) flowExecutionId).iterator();
    }

    @Override
    public Iterator<org.apache.gobblin.service.monitoring.JobStatus> getJobStatusesForFlowExecution(String flowName,
        String flowGroup, long flowExecutionId, String jobGroup, String jobName) {
      return Iterators.emptyIterator();
    }

    @Override
    public StateStore<State> getStateStore() {
      return null;
    }

    @Override
    public List<Long> getLatestExecutionIdsForFlow(String flowName, String flowGroup, int count) {
      if (_listOfJobStatusLists == null) {
        return null;
      }
      int startIndex = (_listOfJobStatusLists.size() >= count) ? _listOfJobStatusLists.size() - count : 0;
      List<Long> flowExecutionIds = IntStream.range(startIndex, _listOfJobStatusLists.size()).mapToObj(i -> (long) i)
          .collect(Collectors.toList());
      Collections.reverse(flowExecutionIds);
      return flowExecutionIds;
    }

    @Override
    public List<org.apache.gobblin.service.monitoring.FlowStatus> getFlowStatusesForFlowGroupExecutions(String flowGroup,
        int countJobStatusesPerFlowName) {
      return Lists.newArrayList(); // (as this method not exercised within `FlowStatusResource`)
    }
  }

  @BeforeClass
  public void setUp() throws Exception {
    JobStatusRetriever jobStatusRetriever = new TestJobStatusRetriever(mock(MultiContextIssueRepository.class));
    final FlowStatusGenerator flowStatusGenerator = new FlowStatusGenerator(jobStatusRetriever);

    Injector injector = Guice.createInjector(new Module() {
       @Override
       public void configure(Binder binder) {
         binder.bind(FlowStatusGenerator.class)
             .toInstance(flowStatusGenerator);
       }
    });

    _server = EmbeddedRestliServer.builder().resources(
        Lists.newArrayList(FlowStatusResource.class)).injector(injector).build();

    _server.startAsync();
    _server.awaitRunning();

    _client =
        new FlowStatusClient(String.format("http://localhost:%s/", _server.getPort()));
  }

  /**
   * Test finding the latest flow status
   * @throws Exception
   */
  @Test
  public void testFindLatest() throws Exception {
    org.apache.gobblin.service.monitoring.JobStatus js1 =
        org.apache.gobblin.service.monitoring.JobStatus.builder().flowGroup("fgroup1").flowName("flow1")
            .jobGroup("jgroup1").jobName("job1").startTime(1000L).endTime(5000L)
            .eventName(ExecutionStatus.COMPLETE.name()).flowExecutionId(0).message("Test message 1").processedCount(100)
            .jobExecutionId(1).lowWatermark("watermark:1").highWatermark("watermark:2")
            .issues(Suppliers.ofInstance(Collections.emptyList())).build();
    org.apache.gobblin.service.monitoring.JobStatus fs1 =
        org.apache.gobblin.service.monitoring.JobStatus.builder().flowGroup("fgroup1").flowName("flow1")
            .jobGroup(JobStatusRetriever.NA_KEY).jobName(JobStatusRetriever.NA_KEY).endTime(5000L)
            .eventName(ExecutionStatus.COMPLETE.name()).flowExecutionId(0)
            .issues(Suppliers.ofInstance(Collections.emptyList())).build();
    org.apache.gobblin.service.monitoring.JobStatus js2 =
        org.apache.gobblin.service.monitoring.JobStatus.builder().flowGroup("fgroup1").flowName("flow1")
            .jobGroup("jgroup1").jobName("job1").jobTag("dataset1").startTime(2000L).endTime(6000L)
            .eventName(ExecutionStatus.COMPLETE.name()).flowExecutionId(1).message("Test message 2").processedCount(200)
            .jobExecutionId(2).lowWatermark("watermark:2").highWatermark("watermark:3")
            .issues(Suppliers.ofInstance(Collections.emptyList()))
            .build();
    org.apache.gobblin.service.monitoring.JobStatus js3 =
        org.apache.gobblin.service.monitoring.JobStatus.builder().flowGroup("fgroup1").flowName("flow1")
            .jobGroup("jgroup1").jobName("job2").jobTag("dataset2").startTime(2000L).endTime(6000L)
            .eventName(ExecutionStatus.COMPLETE.name()).flowExecutionId(1).message("Test message 3").processedCount(200)
            .jobExecutionId(2).lowWatermark("watermark:2").highWatermark("watermark:3")
            .issues(Suppliers.ofInstance(Collections.emptyList()))
            .build();
    org.apache.gobblin.service.monitoring.JobStatus fs2 =
        org.apache.gobblin.service.monitoring.JobStatus.builder().flowGroup("fgroup1").flowName("flow1")
            .jobGroup(JobStatusRetriever.NA_KEY).jobName(JobStatusRetriever.NA_KEY).endTime(7000L)
            .eventName(ExecutionStatus.COMPLETE.name()).flowExecutionId(1).message("Flow message")
            .issues(Suppliers.ofInstance(Collections.emptyList())).build();
    List<org.apache.gobblin.service.monitoring.JobStatus> jobStatusList1 = Lists.newArrayList(js1, fs1);
    List<org.apache.gobblin.service.monitoring.JobStatus> jobStatusList2 = Lists.newArrayList(js2, js3, fs2);
    _listOfJobStatusLists = Lists.newArrayList();
    _listOfJobStatusLists.add(jobStatusList1);
    _listOfJobStatusLists.add(jobStatusList2);

    FlowId flowId = new FlowId().setFlowGroup("fgroup1").setFlowName("flow1");
    FlowStatus flowStatus = _client.getLatestFlowStatus(flowId);

    Assert.assertEquals(flowStatus.getId().getFlowGroup(), "fgroup1");
    Assert.assertEquals(flowStatus.getId().getFlowName(), "flow1");
    Assert.assertEquals(flowStatus.getExecutionStatistics().getExecutionStartTime().longValue(), 1L);
    Assert.assertEquals(flowStatus.getExecutionStatistics().getExecutionEndTime().longValue(), 7000L);
    Assert.assertEquals(flowStatus.getMessage(), fs2.getMessage());
    Assert.assertEquals(flowStatus.getExecutionStatus(), ExecutionStatus.COMPLETE);

    JobStatusArray jobStatuses = flowStatus.getJobStatuses();

    Assert.assertEquals(jobStatusList2.size(), jobStatuses.size() + 1);

    for (int i = 0; i < jobStatuses.size(); i++) {
      org.apache.gobblin.service.monitoring.JobStatus mjs = jobStatusList2.get(i);
      JobStatus js = jobStatuses.get(i);

      compareJobStatus(js, mjs);
    }

    List<FlowStatus> flowStatusList = _client.getLatestFlowStatus(flowId, 2, null);
    Assert.assertEquals(flowStatusList.size(), 2);
    Assert.assertEquals(flowStatusList.get(0).getId().getFlowExecutionId(), (Long) 1L);
    Assert.assertEquals(flowStatusList.get(1).getId().getFlowExecutionId(), (Long) 0L);
    Assert.assertEquals(flowStatusList.get(0).getJobStatuses().size(), 2);

    List<FlowStatus> flowStatusList2 = _client.getLatestFlowStatus(flowId, 1, "dataset1");
    Assert.assertEquals(flowStatusList2.get(0).getJobStatuses().size(), 1);
    Assert.assertEquals(flowStatusList2.get(0).getJobStatuses().get(0).getJobTag(), "dataset1");
  }

  /**
   * Test a flow that has all jobs completed
   * @throws Exception
   */
  @Test
  public void testGetCompleted() throws Exception {
    org.apache.gobblin.service.monitoring.JobStatus js1 =
        org.apache.gobblin.service.monitoring.JobStatus.builder().flowGroup("fgroup1").flowName("flow1")
            .jobGroup("jgroup1").jobName("job1").startTime(1000L).endTime(5000L)
            .eventName(ExecutionStatus.COMPLETE.name()).flowExecutionId(0).message("Test message 1").processedCount(100)
            .jobExecutionId(1).lowWatermark("watermark:1").highWatermark("watermark:2")
            .issues(Suppliers.ofInstance(Collections.emptyList()))
            .build();
    org.apache.gobblin.service.monitoring.JobStatus js2 =
        org.apache.gobblin.service.monitoring.JobStatus.builder().flowGroup("fgroup1").flowName("flow1")
            .jobGroup("jgroup1").jobName("job2").startTime(2000L).endTime(6000L)
            .eventName(ExecutionStatus.COMPLETE.name()).flowExecutionId(0).message("Test message 2").processedCount(200)
            .jobExecutionId(2).lowWatermark("watermark:2").highWatermark("watermark:3")
            .issues(Suppliers.ofInstance(Collections.emptyList()))
            .build();
    org.apache.gobblin.service.monitoring.JobStatus fs1 =
        org.apache.gobblin.service.monitoring.JobStatus.builder().flowGroup("fgroup1").flowName("flow1")
            .jobGroup(JobStatusRetriever.NA_KEY).jobName(JobStatusRetriever.NA_KEY).endTime(7000L)
            .eventName(ExecutionStatus.COMPLETE.name()).flowExecutionId(0).message("Flow message")
            .issues(Suppliers.ofInstance(Collections.emptyList())).build();
    List<org.apache.gobblin.service.monitoring.JobStatus> jobStatusList = Lists.newArrayList(js1, js2, fs1);
    _listOfJobStatusLists = Lists.newArrayList();
    _listOfJobStatusLists.add(jobStatusList);

    FlowStatusId flowId = new FlowStatusId().setFlowGroup("fgroup1").setFlowName("flow1").setFlowExecutionId(0);
    FlowStatus flowStatus = _client.getFlowStatus(flowId);

    Assert.assertEquals(flowStatus.getId().getFlowGroup(), "fgroup1");
    Assert.assertEquals(flowStatus.getId().getFlowName(), "flow1");
    Assert.assertEquals(flowStatus.getExecutionStatistics().getExecutionStartTime().longValue(), 0L);
    Assert.assertEquals(flowStatus.getExecutionStatistics().getExecutionEndTime().longValue(), 7000L);
    Assert.assertEquals(flowStatus.getMessage(), fs1.getMessage());
    Assert.assertEquals(flowStatus.getExecutionStatus(), ExecutionStatus.COMPLETE);

    JobStatusArray jobStatuses = flowStatus.getJobStatuses();

    Assert.assertEquals(jobStatusList.size(), jobStatuses.size() + 1);

    for (int i = 0; i < jobStatuses.size(); i++) {
      org.apache.gobblin.service.monitoring.JobStatus mjs = jobStatusList.get(i);
      JobStatus js = jobStatuses.get(i);

      compareJobStatus(js, mjs);
    }
  }

  /**
   * Test a flow that has some jobs still running
   * @throws Exception
   */
  @Test
  public void testGetRunning() throws Exception {
    org.apache.gobblin.service.monitoring.JobStatus js1 =
        org.apache.gobblin.service.monitoring.JobStatus.builder().flowGroup("fgroup1").flowName("flow1")
            .jobGroup("jgroup1").jobName("job1").startTime(1000L).endTime(5000L)
            .eventName(ExecutionStatus.RUNNING.name()).flowExecutionId(0).message("Test message 1").processedCount(100)
            .jobExecutionId(1).lowWatermark("watermark:1").highWatermark("watermark:2")
            .issues(Suppliers.ofInstance(Collections.emptyList()))
            .build();
    org.apache.gobblin.service.monitoring.JobStatus js2 =
        org.apache.gobblin.service.monitoring.JobStatus.builder().flowGroup("fgroup1").flowName("flow1")
            .jobGroup("jgroup1").jobName("job2").startTime(2000L).endTime(6000L)
            .eventName(ExecutionStatus.COMPLETE.name()).flowExecutionId(0).message("Test message 2").processedCount(200)
            .jobExecutionId(2).lowWatermark("watermark:2").highWatermark("watermark:3")
            .issues(Suppliers.ofInstance(Collections.emptyList()))
            .build();
    org.apache.gobblin.service.monitoring.JobStatus fs1 =
        org.apache.gobblin.service.monitoring.JobStatus.builder().flowGroup("fgroup1").flowName("flow1")
            .jobGroup(JobStatusRetriever.NA_KEY).jobName(JobStatusRetriever.NA_KEY)
            .eventName(ExecutionStatus.RUNNING.name()).flowExecutionId(0).message("Flow message")
            .issues(Suppliers.ofInstance(Collections.emptyList())).build();
    List<org.apache.gobblin.service.monitoring.JobStatus> jobStatusList = Lists.newArrayList(js1, js2, fs1);
    _listOfJobStatusLists = Lists.newArrayList();
    _listOfJobStatusLists.add(jobStatusList);

    FlowStatusId flowId = new FlowStatusId().setFlowGroup("fgroup1").setFlowName("flow1").setFlowExecutionId(0);
    FlowStatus flowStatus = _client.getFlowStatus(flowId);

    Assert.assertEquals(flowStatus.getId().getFlowGroup(), "fgroup1");
    Assert.assertEquals(flowStatus.getId().getFlowName(), "flow1");
    Assert.assertEquals(flowStatus.getExecutionStatistics().getExecutionStartTime().longValue(), 0L);
    Assert.assertEquals(flowStatus.getExecutionStatistics().getExecutionEndTime().longValue(), 6000L);
    Assert.assertEquals(flowStatus.getMessage(), fs1.getMessage());
    Assert.assertEquals(flowStatus.getExecutionStatus(), ExecutionStatus.RUNNING);

    JobStatusArray jobStatuses = flowStatus.getJobStatuses();

    Assert.assertEquals(jobStatusList.size(), jobStatuses.size() + 1);

    for (int i = 0; i < jobStatuses.size(); i++) {
      org.apache.gobblin.service.monitoring.JobStatus mjs = jobStatusList.get(i);
      JobStatus js = jobStatuses.get(i);

      compareJobStatus(js, mjs);
    }
  }

  /**
   * Test a flow that has some failed jobs
   * @throws Exception
   */
  @Test
  public void testGetFailed() throws Exception {
    org.apache.gobblin.service.monitoring.JobStatus js1 =
        org.apache.gobblin.service.monitoring.JobStatus.builder().flowGroup("fgroup1").flowName("flow1")
            .jobGroup("jgroup1").jobName("job1").startTime(1000L).endTime(5000L)
            .eventName(ExecutionStatus.COMPLETE.name()).flowExecutionId(0).message("Test message 1").processedCount(100)
            .jobExecutionId(1).lowWatermark("watermark:1").highWatermark("watermark:2")
            .issues(Suppliers.ofInstance(Collections.emptyList()))
            .build();
    org.apache.gobblin.service.monitoring.JobStatus js2 =
        org.apache.gobblin.service.monitoring.JobStatus.builder().flowGroup("fgroup1").flowName("flow1")
            .jobGroup("jgroup1").jobName("job2").startTime(2000L).endTime(6000L)
            .eventName(ExecutionStatus.FAILED.name()).flowExecutionId(0).message("Test message 2").processedCount(200)
            .jobExecutionId(2).lowWatermark("watermark:2").highWatermark("watermark:3")
            .issues(Suppliers.ofInstance(Collections.emptyList()))
            .build();
    org.apache.gobblin.service.monitoring.JobStatus fs1 =
        org.apache.gobblin.service.monitoring.JobStatus.builder().flowGroup("fgroup1").flowName("flow1")
            .jobGroup(JobStatusRetriever.NA_KEY).jobName(JobStatusRetriever.NA_KEY).endTime(7000L)
            .eventName(ExecutionStatus.FAILED.name()).flowExecutionId(0).message("Flow message")
            .issues(Suppliers.ofInstance(Collections.emptyList())).build();
    List<org.apache.gobblin.service.monitoring.JobStatus> jobStatusList = Lists.newArrayList(js1, js2, fs1);
    _listOfJobStatusLists = Lists.newArrayList();
    _listOfJobStatusLists.add(jobStatusList);

    FlowStatusId flowId = new FlowStatusId().setFlowGroup("fgroup1").setFlowName("flow1").setFlowExecutionId(0);
    FlowStatus flowStatus = _client.getFlowStatus(flowId);

    Assert.assertEquals(flowStatus.getId().getFlowGroup(), "fgroup1");
    Assert.assertEquals(flowStatus.getId().getFlowName(), "flow1");
    Assert.assertEquals(flowStatus.getExecutionStatistics().getExecutionStartTime().longValue(), 0L);
    Assert.assertEquals(flowStatus.getExecutionStatistics().getExecutionEndTime().longValue(), 7000L);
    Assert.assertEquals(flowStatus.getMessage(), fs1.getMessage());
    Assert.assertEquals(flowStatus.getExecutionStatus(), ExecutionStatus.FAILED);

    JobStatusArray jobStatuses = flowStatus.getJobStatuses();

    Assert.assertEquals(jobStatusList.size(), jobStatuses.size() + 1);

    for (int i = 0; i < jobStatuses.size(); i++) {
      org.apache.gobblin.service.monitoring.JobStatus mjs = jobStatusList.get(i);
      JobStatus js = jobStatuses.get(i);

      compareJobStatus(js, mjs);
    }
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() throws Exception {
    if (_client != null) {
      _client.close();
    }
    if (_server != null) {
      _server.stopAsync();
      _server.awaitTerminated();
    }
  }

  /**
   * compare the job status objects from the REST call and monitoring service
   * @param js JobStatus from REST
   * @param mjs JobStatus from monitoring
   */
  private void compareJobStatus(JobStatus js, org.apache.gobblin.service.monitoring.JobStatus mjs) {
    Assert.assertEquals(mjs.getFlowGroup(), js.getFlowId().getFlowGroup());
    Assert.assertEquals(mjs.getFlowName(), js.getFlowId().getFlowName());
    Assert.assertEquals(mjs.getJobGroup(), js.getJobId().getJobGroup());
    Assert.assertEquals(mjs.getJobName(), js.getJobId().getJobName());
    Assert.assertEquals(mjs.getMessage(), js.getMessage());
    Assert.assertEquals(mjs.getStartTime(), js.getExecutionStatistics().getExecutionStartTime().longValue());
    Assert.assertEquals(mjs.getEndTime(), js.getExecutionStatistics().getExecutionEndTime().longValue());
    Assert.assertEquals(mjs.getProcessedCount(), js.getExecutionStatistics().getProcessedCount().longValue());
    Assert.assertEquals(mjs.getLowWatermark(), js.getJobState().getLowWatermark());
    Assert.assertEquals(mjs.getHighWatermark(), js.getJobState().getHighWatermark());
  }
}
