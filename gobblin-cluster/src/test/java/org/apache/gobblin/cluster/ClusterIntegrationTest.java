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

package org.apache.gobblin.cluster;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordStreamingSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.task.TargetState;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.zookeeper.datamodel.serializer.ChainedPathZkSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.PathBasedZkSerializer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.cluster.suite.IntegrationBasicSuite;
import org.apache.gobblin.cluster.suite.IntegrationDedicatedManagerClusterSuite;
import org.apache.gobblin.cluster.suite.IntegrationDedicatedTaskDriverClusterSuite;
import org.apache.gobblin.cluster.suite.IntegrationJobCancelSuite;
import org.apache.gobblin.cluster.suite.IntegrationJobFactorySuite;
import org.apache.gobblin.cluster.suite.IntegrationJobRestartViaSpecSuite;
import org.apache.gobblin.cluster.suite.IntegrationJobTagSuite;
import org.apache.gobblin.cluster.suite.IntegrationSeparateProcessSuite;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.testing.AssertWithBackoff;
import org.apache.gobblin.util.ConfigUtils;


@Slf4j
@Test
public class ClusterIntegrationTest {

  private IntegrationBasicSuite suite;
  private String zkConnectString;

  @Test
  public void testJobShouldComplete()
      throws Exception {
    this.suite = new IntegrationBasicSuite();
    runAndVerify();
  }

  private HelixManager getHelixManager() {
    Config helixConfig = this.suite.getManagerConfig();
    String clusterName = helixConfig.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY);
    String instanceName = ConfigUtils.getString(helixConfig, GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_KEY,
        GobblinClusterManager.class.getSimpleName());
    this.zkConnectString = helixConfig.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    HelixManager helixManager = HelixManagerFactory.getZKHelixManager(clusterName, instanceName, InstanceType.CONTROLLER, zkConnectString);
    return helixManager;
  }

  @Test
  void testJobShouldGetCancelled() throws Exception {
    // Cancellation usually needs long time to successfully be executed, therefore setting the sleeping time to 100.
    Config jobConfigOverrides = ClusterIntegrationTestUtils.buildSleepingJob(IntegrationJobCancelSuite.JOB_ID,
        IntegrationJobCancelSuite.TASK_STATE_FILE)
        .withValue(SleepingTask.SLEEP_TIME_IN_SECONDS, ConfigValueFactory.fromAnyRef(100));
    this.suite = new IntegrationJobCancelSuite(jobConfigOverrides);
    suite.startCluster();
    HelixManager helixManager = getHelixManager();
    helixManager.connect();

    ExecutorService executor = Executors.newSingleThreadExecutor();
    Runnable cancelAfterTaskInit = () -> {
      try {
        TaskDriver taskDriver = new TaskDriver(helixManager);
        // The actual cancellation needs to be executed in separated thread to make the cancel of helix is not blocked by
        // SleepingTask's thread in its own thread.
        // Issue the cancel after ensuring the workflow is created and the SleepingTask is running
        AssertWithBackoff.create().maxSleepMs(1000).backoffFactor(1).
            assertTrue(isTaskStarted(helixManager, IntegrationJobCancelSuite.JOB_ID), "Waiting for the job to start...");

        AssertWithBackoff.create().maxSleepMs(100).timeoutMs(2000).backoffFactor(1).
            assertTrue(isTaskRunning(IntegrationJobCancelSuite.TASK_STATE_FILE),
                "Waiting for the task to enter running state");

        log.info("Stopping the job");
        taskDriver.stop(IntegrationJobCancelSuite.JOB_ID);
        suite.shutdownCluster();
      } catch (Exception e) {
        throw new RuntimeException("Failure in canceling tasks");
      }
    };

    FutureTask<String> futureTask = new FutureTask<String>( cancelAfterTaskInit, "cancelled");
    executor.submit(futureTask);

    AssertWithBackoff assertWithBackoff = AssertWithBackoff.create().backoffFactor(1).maxSleepMs(1000).timeoutMs(500000);
    assertWithBackoff.assertTrue(new Predicate<Void>() {
      @Override
      public boolean apply(Void input) {
        return futureTask.isDone();
      }
    }, "waiting for future to complete");

    Assert.assertEquals(futureTask.get(), "cancelled");
    suite.waitForAndVerifyOutputFiles();
  }

  /**
   * An integration test for restarting a Helix workflow via a JobSpec. This test case starts a Helix cluster with
   * a {@link FsJobConfigurationManager}. The test case does the following:
   * <ul>
   *   <li> add a {@link org.apache.gobblin.runtime.api.JobSpec} that uses a {@link org.apache.gobblin.cluster.SleepingCustomTaskSource})
   *   to {@link IntegrationJobRestartViaSpecSuite#FS_SPEC_CONSUMER_DIR}.  which is picked by the JobConfigurationManager. </li>
   *   <li> the JobConfigurationManager sends a notification to the GobblinHelixJobScheduler which schedules the job for execution. The JobSpec is
   *   also added to the JobCatalog for persistence. Helix starts a Workflow for this JobSpec. </li>
   *   <li> We then add a {@link org.apache.gobblin.runtime.api.JobSpec} with UPDATE Verb to {@link IntegrationJobRestartViaSpecSuite#FS_SPEC_CONSUMER_DIR}.
   *   This signals GobblinHelixJobScheduler (and, Helix) to first cancel the running job (i.e., Helix Workflow) started in the previous step.
   *   <li> We inspect the state of the zNode corresponding to the Workflow resource in Zookeeper to ensure that its {@link org.apache.helix.task.TargetState}
   *   is STOP. </li>
   *   <li> Once the cancelled job from the previous steps is completed, the job will be re-launched for execution by the GobblinHelixJobScheduler.
   *   We confirm the execution by again inspecting the zNode and ensuring its TargetState is START. </li>
   * </ul>
   */
  @Test (enabled = false, dependsOnMethods = { "testJobShouldGetCancelled" }, groups = {"disabledOnCI"})
  public void testJobRestartViaSpec() throws Exception {
    Config jobConfigOverrides = ClusterIntegrationTestUtils.buildSleepingJob(IntegrationJobCancelSuite.JOB_ID,
        IntegrationJobCancelSuite.TASK_STATE_FILE);
    this.suite = new IntegrationJobRestartViaSpecSuite(jobConfigOverrides);
    HelixManager helixManager = getHelixManager();

    IntegrationJobRestartViaSpecSuite restartViaSpecSuite = (IntegrationJobRestartViaSpecSuite) this.suite;

    //Start the cluster
    restartViaSpecSuite.startCluster();

    helixManager.connect();

    AssertWithBackoff.create().timeoutMs(30000).maxSleepMs(1000).backoffFactor(1).
        assertTrue(isTaskStarted(helixManager, IntegrationJobRestartViaSpecSuite.JOB_ID), "Waiting for the job to start...");

    AssertWithBackoff.create().maxSleepMs(100).timeoutMs(2000).backoffFactor(1).
        assertTrue(isTaskRunning(IntegrationJobRestartViaSpecSuite.TASK_STATE_FILE), "Waiting for the task to enter running state");

    ZkClient zkClient = new ZkClient(this.zkConnectString);
    PathBasedZkSerializer zkSerializer = ChainedPathZkSerializer.builder(new ZNRecordStreamingSerializer()).build();
    zkClient.setZkSerializer(zkSerializer);

    String clusterName = getHelixManager().getClusterName();
    String zNodePath = Paths.get("/", clusterName, "CONFIGS", "RESOURCE", IntegrationJobCancelSuite.JOB_ID).toString();

    //Ensure that the Workflow is started
    ZNRecord record = zkClient.readData(zNodePath);
    String targetState = record.getSimpleField("TargetState");
    Assert.assertEquals(targetState, TargetState.START.name());

    //Add a JobSpec with UPDATE verb signalling the Helix cluster to restart the workflow
    restartViaSpecSuite.addJobSpec(IntegrationJobRestartViaSpecSuite.JOB_NAME, SpecExecutor.Verb.UPDATE.name());

    AssertWithBackoff.create().maxSleepMs(1000).timeoutMs(12000).backoffFactor(1).assertTrue(input -> {
      //Inspect the zNode at the path corresponding to the Workflow resource. Ensure the target state of the resource is in
      // the STOP state or that the zNode has been deleted.
      ZNRecord recordNew = zkClient.readData(zNodePath, true);
      String targetStateNew = null;
      if (recordNew != null) {
        targetStateNew = recordNew.getSimpleField("TargetState");
      }
      return recordNew == null || targetStateNew.equals(TargetState.STOP.name());
    }, "Waiting for Workflow TargetState to be STOP");

    //Ensure that the SleepingTask did not terminate normally i.e. it was interrupted. We check this by ensuring
    // that the line "Hello World!" is not present in the logged output.
    suite.waitForAndVerifyOutputFiles();

    AssertWithBackoff.create().maxSleepMs(1000).timeoutMs(120000).backoffFactor(1).assertTrue(input -> {
      //Inspect the zNode at the path corresponding to the Workflow resource. Ensure the target state of the resource is in
      // the START state.
      ZNRecord recordNew = zkClient.readData(zNodePath, true);
      String targetStateNew = null;
      if (recordNew != null) {
        targetStateNew = recordNew.getSimpleField("TargetState");
        return targetStateNew.equals(TargetState.START.name());
      }
      return false;
    }, "Waiting for Workflow TargetState to be START");
  }

  public static Predicate<Void> isTaskStarted(HelixManager helixManager, String jobId) {
    return input -> TaskDriver.getWorkflowContext(helixManager, jobId) != null;
  }

  public static Predicate<Void> isTaskRunning(String taskStateFileName) {
    return input -> {
      File taskStateFile = new File(taskStateFileName);
      return taskStateFile.exists();
    };
  }

  @Test
  public void testSeparateProcessMode()
      throws Exception {
    this.suite = new IntegrationSeparateProcessSuite();
    runAndVerify();
  }

  @Test
  public void testDedicatedManagerCluster()
      throws Exception {
    this.suite = new IntegrationDedicatedManagerClusterSuite();
    runAndVerify();
  }

  @Test(enabled = false)
  public void testDedicatedTaskDriverCluster()
      throws Exception {
    this.suite = new IntegrationDedicatedTaskDriverClusterSuite();
    runAndVerify();
  }

  @Test(enabled = false)
  public void testJobWithTag()
      throws Exception {
    this.suite = new IntegrationJobTagSuite();
    runAndVerify();
  }

  @Test
  public void testPlanningJobFactory()
      throws Exception {
    this.suite = new IntegrationJobFactorySuite();
    runAndVerify();
  }

  private void runAndVerify()
      throws Exception {
    suite.startCluster();
    suite.waitForAndVerifyOutputFiles();
    ensureJobLauncherFinished();
    suite.verifyMetricsCleaned();
    suite.shutdownCluster();
  }

  private void ensureJobLauncherFinished() throws Exception {
    AssertWithBackoff asserter = AssertWithBackoff.create().logger(log).timeoutMs(120_000)
       .maxSleepMs(100).backoffFactor(1.5);

    asserter.assertTrue(this::isJobLauncherFinished, "Waiting for job launcher completion");
  }

  protected boolean isJobLauncherFinished(Void input) {
    Map<Thread, StackTraceElement[]> map = Thread.getAllStackTraces();
    for (Map.Entry<Thread, StackTraceElement[]> entry: map.entrySet()) {
      for (StackTraceElement ste: entry.getValue()) {
        if (ste.toString().contains(HelixRetriggeringJobCallable.class.getSimpleName())) {
          return false;
        }
      }
    }

    return true;
  }


  @AfterMethod
  public void tearDown() throws IOException {
    this.suite.deleteWorkDir();
  }
}
