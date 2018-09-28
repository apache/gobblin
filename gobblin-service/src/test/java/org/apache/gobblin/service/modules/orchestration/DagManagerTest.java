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
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.FileUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Iterators;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.spec_executorInstance.InMemorySpecExecutor;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.spec.JobExecutionPlanDagFactory;
import org.apache.gobblin.service.monitoring.JobStatus;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;


public class DagManagerTest {
  private final String dagStateStoreDir = "/tmp/dagManagerTest/dagStateStore";
  private DagStateStore _dagStateStore;
  private JobStatusRetriever _jobStatusRetriever;
  private DagManager.DagManagerThread _dagManagerThread;
  private LinkedBlockingQueue<Dag<JobExecutionPlan>> queue;
  private Map<Dag.DagNode<JobExecutionPlan>, Dag<JobExecutionPlan>> jobToDag;
  private Map<String, LinkedList<Dag.DagNode<JobExecutionPlan>>> dagToJobs;
  private Map<String, Dag<JobExecutionPlan>> dags;

  @BeforeClass
  public void setUp() throws Exception {
    FileUtils.deleteDirectory(new File(this.dagStateStoreDir));
    Config config = ConfigFactory.empty()
        .withValue(DagManager.DAG_STATESTORE_DIR, ConfigValueFactory.fromAnyRef(this.dagStateStoreDir));
    this._dagStateStore = new FSDagStateStore(config);
    this._jobStatusRetriever = Mockito.mock(JobStatusRetriever.class);
    this.queue = new LinkedBlockingQueue<>();
    this._dagManagerThread = new DagManager.DagManagerThread(_jobStatusRetriever, _dagStateStore, queue, true);

    Field jobToDagField = DagManager.DagManagerThread.class.getDeclaredField("jobToDag");
    jobToDagField.setAccessible(true);
    this.jobToDag = (Map<Dag.DagNode<JobExecutionPlan>, Dag<JobExecutionPlan>>) jobToDagField.get(this._dagManagerThread);

    Field dagToJobsField = DagManager.DagManagerThread.class.getDeclaredField("dagToJobs");
    dagToJobsField.setAccessible(true);
    this.dagToJobs = (Map<String, LinkedList<Dag.DagNode<JobExecutionPlan>>>) dagToJobsField.get(this._dagManagerThread);

    Field dagsField = DagManager.DagManagerThread.class.getDeclaredField("dags");
    dagsField.setAccessible(true);
    this.dags = (Map<String, Dag<JobExecutionPlan>>) dagsField.get(this._dagManagerThread);

  }

  /**
   * Create a {@link Dag < JobExecutionPlan >} with one parent and remaining nodes as its children.
   * @return a Dag.
   */
  public Dag<JobExecutionPlan> buildDag(String id, Long flowExecutionId, int numNodes)
      throws URISyntaxException {
    List<JobExecutionPlan> jobExecutionPlans = new ArrayList<>();
    for (int i = 0; i < numNodes; i++) {
      String suffix = Integer.toString(i);
      Config jobConfig = ConfigBuilder.create().
          addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "group" + id).
          addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "flow" + id).
          addPrimitive(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, flowExecutionId).
          addPrimitive(ConfigurationKeys.JOB_GROUP_KEY, "group" + id).
          addPrimitive(ConfigurationKeys.JOB_NAME_KEY, "job" + suffix).build();
      if (i > 0) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job0"));
      }
      JobSpec js = JobSpec.builder("test_job" + suffix).withVersion(suffix).withConfig(jobConfig).
          withTemplate(new URI("job" + suffix)).build();
      SpecExecutor specExecutor = InMemorySpecExecutor.createDummySpecExecutor(new URI("job" + i));
      JobExecutionPlan jobExecutionPlan = new JobExecutionPlan(js, specExecutor);
      jobExecutionPlans.add(jobExecutionPlan);
    }
    return new JobExecutionPlanDagFactory().createDag(jobExecutionPlans);
  }

  private Iterator<JobStatus> getMockJobStatus(String flowName, String flowGroup,  Long flowExecutionId, String jobGroup, String jobName, String eventName) {
    return Iterators.singletonIterator(JobStatus.builder().flowName(flowName).flowGroup(flowGroup).jobGroup(jobGroup).jobName(jobName).flowExecutionId(flowExecutionId).
        message("Test message").eventName(eventName).startTime(5000L).build());
  }

  @Test
  public void testSuccessfulDag() throws URISyntaxException {
    long flowExecutionId = System.currentTimeMillis();
    String flowGroupId = "0";
    String flowGroup = "group" + flowGroupId;
    String flowName = "flow" + flowGroupId;
    String jobGroup = flowGroup;
    String jobName0 = "job0";
    String jobName1 = "job1";
    String jobName2 = "job2";

    Dag<JobExecutionPlan> dag = buildDag(flowGroupId, flowExecutionId, 3);
    String dagId = DagManagerUtils.generateDagId(dag);

    //Add a dag to the queue of dags
    this.queue.offer(dag);
    Iterator<JobStatus> jobStatusIterator1 = getMockJobStatus(flowName, flowGroup, flowExecutionId, jobGroup, jobName0, TimingEvent.LauncherTimings.JOB_RUN);
    Iterator<JobStatus> jobStatusIterator2 = getMockJobStatus(flowName, flowGroup, flowExecutionId, jobGroup, jobName0, TimingEvent.LauncherTimings.JOB_COMPLETE);
    Iterator<JobStatus> jobStatusIterator3 = getMockJobStatus(flowName, flowGroup, flowExecutionId, jobGroup, jobName1, TimingEvent.LauncherTimings.JOB_RUN);
    Iterator<JobStatus> jobStatusIterator4 = getMockJobStatus(flowName, flowGroup, flowExecutionId, jobGroup, jobName2, TimingEvent.LauncherTimings.JOB_RUN);
    Iterator<JobStatus> jobStatusIterator5 = getMockJobStatus(flowName, flowGroup, flowExecutionId, jobGroup, jobName1, TimingEvent.LauncherTimings.JOB_RUN);
    Iterator<JobStatus> jobStatusIterator6 = getMockJobStatus(flowName, flowGroup, flowExecutionId, jobGroup, jobName2, TimingEvent.LauncherTimings.JOB_COMPLETE);
    Iterator<JobStatus> jobStatusIterator7 = getMockJobStatus(flowName, flowGroup, flowExecutionId, jobGroup, jobName2, TimingEvent.LauncherTimings.JOB_COMPLETE);

    Mockito.when(_jobStatusRetriever.getJobStatusesForFlowExecution(Mockito.anyString(), Mockito.anyString(),
        Mockito.anyLong(), Mockito.anyString(), Mockito.anyString())).
        thenReturn(jobStatusIterator1).
        thenReturn(jobStatusIterator2).
        thenReturn(jobStatusIterator3).
        thenReturn(jobStatusIterator4).
        thenReturn(jobStatusIterator5).
        thenReturn(jobStatusIterator6).
        thenReturn(jobStatusIterator7);

    //Run the thread once. Ensure the first job is running
    this._dagManagerThread.run();
    Assert.assertEquals(this.dags.size(), 1);
    Assert.assertTrue(this.dags.containsKey(dagId));
    Assert.assertEquals(this.jobToDag.size(), 1);
    Assert.assertTrue(this.jobToDag.containsKey(dag.getStartNodes().get(0)));
    Assert.assertEquals(this.dagToJobs.get(dagId).size(), 1);
    Assert.assertTrue(this.dagToJobs.get(dagId).contains(dag.getStartNodes().get(0)));

    //Run the thread 2nd time. Ensure the job0 is complete and job1 and job2 are submitted.
    this._dagManagerThread.run();
    Assert.assertEquals(this.dags.size(), 1);
    Assert.assertTrue(this.dags.containsKey(dagId));
    Assert.assertEquals(this.jobToDag.size(), 2);
    Assert.assertTrue(this.jobToDag.containsKey(dag.getEndNodes().get(0)));
    Assert.assertTrue(this.jobToDag.containsKey(dag.getEndNodes().get(1)));
    Assert.assertEquals(this.dagToJobs.get(dagId).size(), 2);
    Assert.assertTrue(this.dagToJobs.get(dagId).contains(dag.getEndNodes().get(0)));
    Assert.assertTrue(this.dagToJobs.get(dagId).contains(dag.getEndNodes().get(1)));

    //Run the thread 3rd time. Ensure job1 and job2 are running.
    this._dagManagerThread.run();
    Assert.assertEquals(this.dags.size(), 1);
    Assert.assertTrue(this.dags.containsKey(dagId));
    Assert.assertEquals(this.jobToDag.size(), 2);
    Assert.assertTrue(this.jobToDag.containsKey(dag.getEndNodes().get(0)));
    Assert.assertTrue(this.jobToDag.containsKey(dag.getEndNodes().get(1)));
    Assert.assertEquals(this.dagToJobs.get(dagId).size(), 2);
    Assert.assertTrue(this.dagToJobs.get(dagId).contains(dag.getEndNodes().get(0)));
    Assert.assertTrue(this.dagToJobs.get(dagId).contains(dag.getEndNodes().get(1)));

    //Run the thread 4th time. One of the jobs is completed.
    this._dagManagerThread.run();
    Assert.assertEquals(this.dags.size(), 1);
    Assert.assertTrue(this.dags.containsKey(dagId));
    Assert.assertEquals(this.jobToDag.size(), 1);
    Assert.assertEquals(this.dagToJobs.get(dagId).size(), 1);

    //Run the thread again. Ensure all jobs completed and dag is cleaned up.
    this._dagManagerThread.run();
    Assert.assertEquals(this.dags.size(), 0);
    Assert.assertEquals(this.jobToDag.size(), 0);
    Assert.assertEquals(this.dagToJobs.size(), 0);
  }

  @Test (dependsOnMethods = "testSuccessfulDag")
  public void testFailedDag() throws URISyntaxException {
    long flowExecutionId = System.currentTimeMillis();
    String flowGroupId = "0";
    String flowGroup = "group" + flowGroupId;
    String flowName = "flow" + flowGroupId;
    String jobGroup = flowGroup;
    String jobName0 = "job0";
    String jobName1 = "job1";
    String jobName2 = "job2";

    Dag<JobExecutionPlan> dag = buildDag(flowGroupId, flowExecutionId, 3);
    String dagId = DagManagerUtils.generateDagId(dag);

    //Add a dag to the queue of dags
    this.queue.offer(dag);
    Iterator<JobStatus> jobStatusIterator1 = getMockJobStatus(flowName, flowGroup, flowExecutionId, jobGroup, jobName0, TimingEvent.LauncherTimings.JOB_START);
    Iterator<JobStatus> jobStatusIterator2 = getMockJobStatus(flowName, flowGroup, flowExecutionId, jobGroup, jobName0, TimingEvent.LauncherTimings.JOB_COMPLETE);

    Iterator<JobStatus> jobStatusIterator3 = getMockJobStatus(flowName, flowGroup, flowExecutionId, jobGroup, jobName1, TimingEvent.LauncherTimings.JOB_RUN);
    Iterator<JobStatus> jobStatusIterator4 = getMockJobStatus(flowName, flowGroup, flowExecutionId, jobGroup, jobName2, TimingEvent.LauncherTimings.JOB_RUN);

    Iterator<JobStatus> jobStatusIterator5 = getMockJobStatus(flowName, flowGroup, flowExecutionId, jobGroup, jobName1, TimingEvent.LauncherTimings.JOB_RUN);
    Iterator<JobStatus> jobStatusIterator6 = getMockJobStatus(flowName, flowGroup, flowExecutionId, jobGroup, jobName2, TimingEvent.LauncherTimings.JOB_FAILED);

    Mockito.when(_jobStatusRetriever.getJobStatusesForFlowExecution(Mockito.anyString(), Mockito.anyString(),
        Mockito.anyLong(), Mockito.anyString(), Mockito.anyString())).
        thenReturn(jobStatusIterator1).
        thenReturn(jobStatusIterator2).
        thenReturn(jobStatusIterator3).
        thenReturn(jobStatusIterator4).
        thenReturn(jobStatusIterator5).
        thenReturn(jobStatusIterator6);

    //Run the thread once. Ensure the first job is running
    this._dagManagerThread.run();
    Assert.assertEquals(this.dags.size(), 1);
    Assert.assertTrue(this.dags.containsKey(dagId));
    Assert.assertEquals(this.jobToDag.size(), 1);
    Assert.assertTrue(this.jobToDag.containsKey(dag.getStartNodes().get(0)));
    Assert.assertEquals(this.dagToJobs.get(dagId).size(), 1);
    Assert.assertTrue(this.dagToJobs.get(dagId).contains(dag.getStartNodes().get(0)));

    //Run the thread 2nd time. Ensure the job0 is complete and job1 and job2 are submitted.
    this._dagManagerThread.run();
    Assert.assertEquals(this.dags.size(), 1);
    Assert.assertTrue(this.dags.containsKey(dagId));
    Assert.assertEquals(this.jobToDag.size(), 2);
    Assert.assertTrue(this.jobToDag.containsKey(dag.getEndNodes().get(0)));
    Assert.assertTrue(this.jobToDag.containsKey(dag.getEndNodes().get(1)));
    Assert.assertEquals(this.dagToJobs.get(dagId).size(), 2);
    Assert.assertTrue(this.dagToJobs.get(dagId).contains(dag.getEndNodes().get(0)));
    Assert.assertTrue(this.dagToJobs.get(dagId).contains(dag.getEndNodes().get(1)));

    //Run the thread 3rd time. Ensure the job0 is complete and job1 and job2 are running.
    this._dagManagerThread.run();
    Assert.assertEquals(this.dags.size(), 1);
    Assert.assertTrue(this.dags.containsKey(dagId));
    Assert.assertEquals(this.jobToDag.size(), 2);
    Assert.assertTrue(this.jobToDag.containsKey(dag.getEndNodes().get(0)));
    Assert.assertTrue(this.jobToDag.containsKey(dag.getEndNodes().get(1)));
    Assert.assertEquals(this.dagToJobs.get(dagId).size(), 2);
    Assert.assertTrue(this.dagToJobs.get(dagId).contains(dag.getEndNodes().get(0)));
    Assert.assertTrue(this.dagToJobs.get(dagId).contains(dag.getEndNodes().get(1)));

    //Run the thread 4th time. One of the jobs is failed and so the dag is failed and all state is cleaned up.
    this._dagManagerThread.run();
    Assert.assertEquals(this.dags.size(), 0);
    Assert.assertEquals(this.jobToDag.size(), 0);
    Assert.assertEquals(this.dagToJobs.size(), 0);
  }

  @AfterClass
  public void cleanUp() throws Exception {
    FileUtils.deleteDirectory(new File(this.dagStateStoreDir));
  }
}