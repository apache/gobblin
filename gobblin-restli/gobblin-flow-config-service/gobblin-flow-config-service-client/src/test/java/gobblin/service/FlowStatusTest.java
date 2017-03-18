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

package gobblin.service;

import java.util.Iterator;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.linkedin.restli.server.resources.BaseResource;

import gobblin.config.ConfigBuilder;
import gobblin.metrics.event.TimingEvent;
import gobblin.restli.EmbeddedRestliServer;
import gobblin.service.monitoring.FlowStatusGenerator;
import gobblin.service.monitoring.JobStatusRetriever;

@Test(groups = { "gobblin.service" }, singleThreaded=true)
public class FlowStatusTest {
  private FlowStatusClient _client;
  private EmbeddedRestliServer _server;
  private List<gobblin.service.monitoring.JobStatus> _jobStatusList;
  private Joiner messageJoiner;

  class TestJobStatusRetriever extends JobStatusRetriever {
    @Override
    public Iterator<gobblin.service.monitoring.JobStatus> getJobStatusesForFlowExecution(String flowName,
        String flowGroup, long flowExecutionId) {
      return _jobStatusList.iterator();
    }

    @Override
    public long getLatestExecutionIdForFlow(String flowName, String flowGroup) {
      return 0;
    }
  }

  @BeforeClass
  public void setUp() throws Exception {
    ConfigBuilder configBuilder = ConfigBuilder.create();

    JobStatusRetriever jobStatusRetriever = new TestJobStatusRetriever();
    final FlowStatusGenerator flowStatusGenerator =
        FlowStatusGenerator.builder().jobStatusRetriever(jobStatusRetriever).build();

    Injector injector = Guice.createInjector(new Module() {
       @Override
       public void configure(Binder binder) {
         binder.bind(FlowStatusGenerator.class).annotatedWith(Names.named(FlowStatusResource.FLOW_STATUS_GENERATOR_INJECT_NAME))
             .toInstance(flowStatusGenerator);
       }
    });

    _server = EmbeddedRestliServer.builder().resources(
        Lists.<Class<? extends BaseResource>>newArrayList(FlowStatusResource.class)).injector(injector).build();

    _server.startAsync();
    _server.awaitRunning();

    _client =
        new FlowStatusClient(String.format("http://localhost:%s/", _server.getPort()));

    messageJoiner = Joiner.on(FlowStatusResource.MESSAGE_SEPARATOR);
  }

  /**
   * Test a flow that has all jobs completed
   * @throws Exception
   */
  @Test
  public void testGetCompleted() throws Exception {
    gobblin.service.monitoring.JobStatus js1 = gobblin.service.monitoring.JobStatus.builder().flowGroup("fgroup1")
        .flowName("flow1").jobGroup("jgroup1").jobName("job1").startTime(1000L).endTime(5000L)
        .eventName(TimingEvent.LauncherTimings.JOB_COMPLETE).flowExecutionId(0).message("Test message 1")
        .processedCount(100).jobExecutionId(1).lowWatermark("watermark:1").highWatermark("watermark:2").build();
    gobblin.service.monitoring.JobStatus js2 = gobblin.service.monitoring.JobStatus.builder().flowGroup("fgroup1")
        .flowName("flow1").jobGroup("jgroup1").jobName("job2").startTime(2000L).endTime(6000L)
        .eventName(TimingEvent.LauncherTimings.JOB_COMPLETE).flowExecutionId(0).message("Test message 2")
        .processedCount(200).jobExecutionId(2).lowWatermark("watermark:2").highWatermark("watermark:3").build();
    _jobStatusList = Lists.newArrayList(js1, js2);

    FlowStatusId flowId = new FlowStatusId().setFlowGroup("fgroup1").setFlowName("flow1");
    FlowStatus flowStatus = _client.getFlowStatus(flowId);

    Assert.assertEquals(flowStatus.getFlowGroup(), "fgroup1");
    Assert.assertEquals(flowStatus.getFlowName(), "flow1");
    Assert.assertEquals(flowStatus.getExecutionStartTime().longValue(), 1000L);
    Assert.assertEquals(flowStatus.getExecutionEndTime().longValue(), 6000L);
    Assert.assertEquals(flowStatus.getMessage(), messageJoiner.join(js1.getMessage(), js2.getMessage()));
    Assert.assertEquals(flowStatus.getExecutionStatus(), FlowStatusResource.STATUS_COMPLETE);

    JobStatusArray jobStatuses = flowStatus.getJobStatuses();

    Assert.assertEquals(_jobStatusList.size(), jobStatuses.size());

    for (int i = 0; i < jobStatuses.size(); i++) {
      gobblin.service.monitoring.JobStatus mjs = _jobStatusList.get(i);
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
    gobblin.service.monitoring.JobStatus js1 = gobblin.service.monitoring.JobStatus.builder().flowGroup("fgroup1")
        .flowName("flow1").jobGroup("jgroup1").jobName("job1").startTime(1000L).endTime(5000L)
        .eventName(TimingEvent.LauncherTimings.JOB_RUN).flowExecutionId(0).message("Test message 1").processedCount(100)
        .jobExecutionId(1).lowWatermark("watermark:1").highWatermark("watermark:2").build();
    gobblin.service.monitoring.JobStatus js2 = gobblin.service.monitoring.JobStatus.builder().flowGroup("fgroup1")
        .flowName("flow1").jobGroup("jgroup1").jobName("job2").startTime(2000L).endTime(6000L)
        .eventName(TimingEvent.LauncherTimings.JOB_COMPLETE).flowExecutionId(0).message("Test message 2")
        .processedCount(200).jobExecutionId(2).lowWatermark("watermark:2").highWatermark("watermark:3").build();
    _jobStatusList = Lists.newArrayList(js1, js2);

    FlowStatusId flowId = new FlowStatusId().setFlowGroup("fgroup1").setFlowName("flow1");
    FlowStatus flowStatus = _client.getFlowStatus(flowId);

    Assert.assertEquals(flowStatus.getFlowGroup(), "fgroup1");
    Assert.assertEquals(flowStatus.getFlowName(), "flow1");
    Assert.assertEquals(flowStatus.getExecutionStartTime().longValue(), 1000L);
    Assert.assertEquals(flowStatus.getExecutionEndTime().longValue(), 6000L);
    Assert.assertEquals(flowStatus.getMessage(), messageJoiner.join(js1.getMessage(), js2.getMessage()));
    Assert.assertEquals(flowStatus.getExecutionStatus(), FlowStatusResource.STATUS_RUNNING);

    JobStatusArray jobStatuses = flowStatus.getJobStatuses();

    Assert.assertEquals(_jobStatusList.size(), jobStatuses.size());

    for (int i = 0; i < jobStatuses.size(); i++) {
      gobblin.service.monitoring.JobStatus mjs = _jobStatusList.get(i);
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
    gobblin.service.monitoring.JobStatus js1 = gobblin.service.monitoring.JobStatus.builder().flowGroup("fgroup1")
        .flowName("flow1").jobGroup("jgroup1").jobName("job1").startTime(1000L).endTime(5000L)
        .eventName(TimingEvent.LauncherTimings.JOB_COMPLETE).flowExecutionId(0).message("Test message 1")
        .processedCount(100).jobExecutionId(1).lowWatermark("watermark:1").highWatermark("watermark:2").build();
    gobblin.service.monitoring.JobStatus js2 = gobblin.service.monitoring.JobStatus.builder().flowGroup("fgroup1")
        .flowName("flow1").jobGroup("jgroup1").jobName("job2").startTime(2000L).endTime(6000L)
        .eventName(TimingEvent.LauncherTimings.JOB_FAILED).flowExecutionId(0).message("Test message 2")
        .processedCount(200).jobExecutionId(2).lowWatermark("watermark:2").highWatermark("watermark:3").build();
    _jobStatusList = Lists.newArrayList(js1, js2);

    FlowStatusId flowId = new FlowStatusId().setFlowGroup("fgroup1").setFlowName("flow1");
    FlowStatus flowStatus = _client.getFlowStatus(flowId);

    Assert.assertEquals(flowStatus.getFlowGroup(), "fgroup1");
    Assert.assertEquals(flowStatus.getFlowName(), "flow1");
    Assert.assertEquals(flowStatus.getExecutionStartTime().longValue(), 1000L);
    Assert.assertEquals(flowStatus.getExecutionEndTime().longValue(), 6000L);
    Assert.assertEquals(flowStatus.getMessage(), messageJoiner.join(js1.getMessage(), js2.getMessage()));
    Assert.assertEquals(flowStatus.getExecutionStatus(), FlowStatusResource.STATUS_FAILED);

    JobStatusArray jobStatuses = flowStatus.getJobStatuses();

    Assert.assertEquals(_jobStatusList.size(), jobStatuses.size());

    for (int i = 0; i < jobStatuses.size(); i++) {
      gobblin.service.monitoring.JobStatus mjs = _jobStatusList.get(i);
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
  private void compareJobStatus(JobStatus js, gobblin.service.monitoring.JobStatus mjs) {
    Assert.assertEquals(mjs.getFlowGroup(), js.getFlowGroup());
    Assert.assertEquals(mjs.getFlowName(), js.getFlowName());
    Assert.assertEquals(mjs.getJobGroup(), js.getJobGroup());
    Assert.assertEquals(mjs.getJobName(), js.getJobName());
    Assert.assertEquals(mjs.getMessage(), js.getMessage());
    Assert.assertEquals(mjs.getStartTime(), js.getExecutionStartTime().longValue());
    Assert.assertEquals(mjs.getEndTime(), js.getExecutionEndTime().longValue());
    Assert.assertEquals(mjs.getProcessedCount(), js.getProcessedCount().longValue());
    Assert.assertEquals(mjs.getLowWatermark(), js.getLowWatermark());
    Assert.assertEquals(mjs.getHighWatermark(), js.getHighWatermark());
  }
}
