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

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ChainedPathZkSerializer;
import org.apache.helix.manager.zk.PathBasedZkSerializer;
import org.apache.helix.manager.zk.ZNRecordStreamingSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.task.TargetState;
import org.apache.helix.task.TaskDriver;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.cluster.suite.IntegrationBasicSuite;
import org.apache.gobblin.cluster.suite.IntegrationDedicatedManagerClusterSuite;
import org.apache.gobblin.cluster.suite.IntegrationDedicatedTaskDriverClusterSuite;
import org.apache.gobblin.cluster.suite.IntegrationJobCancelSuite;
import org.apache.gobblin.cluster.suite.IntegrationJobCancelViaSpecSuite;
import org.apache.gobblin.cluster.suite.IntegrationJobFactorySuite;
import org.apache.gobblin.cluster.suite.IntegrationJobTagSuite;
import org.apache.gobblin.cluster.suite.IntegrationSeparateProcessSuite;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.util.ConfigUtils;


@Slf4j
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

  @Test void testJobShouldGetCancelled() throws Exception {
    this.suite =new IntegrationJobCancelSuite();
    HelixManager helixManager = getHelixManager();
    suite.startCluster();
    helixManager.connect();

    TaskDriver taskDriver = new TaskDriver(helixManager);

    while (TaskDriver.getWorkflowContext(helixManager, IntegrationJobCancelSuite.JOB_ID) == null) {
      log.warn("Waiting for the job to start...");
      Thread.sleep(1000L);
    }

    Assert.assertTrue(isTaskRunning(IntegrationJobCancelSuite.TASK_STATE_FILE));

    log.info("Stopping the job");
    taskDriver.stop(IntegrationJobCancelSuite.JOB_ID);

    suite.shutdownCluster();

    suite.waitForAndVerifyOutputFiles();
  }

  /**
   * An integration test for cancelling a Helix workflow via a JobSpec. This test case starts a Helix cluster with
   * a {@link FsScheduledJobConfigurationManager}. The test case does the following:
   * <ul>
   *   <li> add a {@link org.apache.gobblin.runtime.api.JobSpec} that uses a {@link org.apache.gobblin.cluster.SleepingCustomTaskSource})
   *   to {@link IntegrationJobCancelViaSpecSuite#FS_SPEC_CONSUMER_DIR}.  which is picked by the JobConfigurationManager. </li>
   *   <li> the JobConfigurationManager sends a notification to the GobblinHelixJobScheduler which schedules the job for execution. The JobSpec is
   *   also added to the JobCatalog for persistence. Helix starts a Workflow for this JobSpec. </li>
   *   <li> We then add a {@link org.apache.gobblin.runtime.api.JobSpec} with DELETE Verb to {@link IntegrationJobCancelViaSpecSuite#FS_SPEC_CONSUMER_DIR}.
   *   This signals GobblinHelixJobScheduler (and, Helix) to delete the running job (i.e., Helix Workflow) started in the previous step. </li>
   *   <li> Finally, we inspect the state of the zNode corresponding to the Workflow resource in Zookeeper to ensure that its {@link org.apache.helix.task.TargetState}
   *   is STOP. </li>
   * </ul>
   */
  @Test (dependsOnMethods = { "testJobShouldGetCancelled" })
  public void testJobCancellationViaSpec() throws Exception {
    this.suite = new IntegrationJobCancelViaSpecSuite();
    HelixManager helixManager = getHelixManager();

    IntegrationJobCancelViaSpecSuite cancelViaSpecSuite = (IntegrationJobCancelViaSpecSuite) this.suite;

    //Add a new JobSpec to the path monitored by the SpecConsumer
    cancelViaSpecSuite.addJobSpec(IntegrationJobCancelViaSpecSuite.JOB_ID, SpecExecutor.Verb.ADD.name());

    //Start the cluster
    cancelViaSpecSuite.startCluster();

    helixManager.connect();

    while (TaskDriver.getWorkflowContext(helixManager, IntegrationJobCancelViaSpecSuite.JOB_ID) == null) {
      log.warn("Waiting for the job to start...");
      Thread.sleep(1000L);
    }

    Assert.assertTrue(isTaskRunning(IntegrationJobCancelViaSpecSuite.TASK_STATE_FILE));

    ZkClient zkClient = new ZkClient(this.zkConnectString);
    PathBasedZkSerializer zkSerializer = ChainedPathZkSerializer.builder(new ZNRecordStreamingSerializer()).build();
    zkClient.setZkSerializer(zkSerializer);

    String clusterName = getHelixManager().getClusterName();
    String zNodePath = Paths.get("/", clusterName, "CONFIGS", "RESOURCE", IntegrationJobCancelViaSpecSuite.JOB_ID).toString();

    //Ensure that the Workflow is started
    ZNRecord record = zkClient.readData(zNodePath);
    String targetState = record.getSimpleField("TargetState");
    Assert.assertEquals(targetState, TargetState.START.name());

    //Add a JobSpec with DELETE verb signalling the Helix cluster to cancel the workflow
    cancelViaSpecSuite.addJobSpec(IntegrationJobCancelViaSpecSuite.JOB_ID, SpecExecutor.Verb.DELETE.name());

    int j = 0;
    boolean successFlag = false;
    while (true) {
      //Inspect the zNode at the path corresponding to the Workflow resource. Ensure the target state of the resource is in
      // the STOP state or that the zNode has been deleted.
      ZNRecord recordNew = zkClient.readData(zNodePath, true);
      String targetStateNew = null;
      if (recordNew != null) {
        targetStateNew = recordNew.getSimpleField("TargetState");
      }
      if (recordNew == null || targetStateNew.equals(TargetState.STOP.name())) {
        successFlag = true;
        break;
      } else {
        j++;
        if (j > 5) {
          break;
        }
        log.warn("Sleeping for 1 second...");
        Thread.sleep(1000L);
      }
    }
    Assert.assertTrue(successFlag);
    suite.waitForAndVerifyOutputFiles();
  }

  private boolean isTaskRunning(String taskStateFileName)
      throws IOException, InterruptedException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    int i = 0;
    Path taskStateFile = new Path(taskStateFileName);
    while (!fs.exists(taskStateFile) && (i++ < 20)) {
      log.warn("Waiting for the task to enter running state...");
      Thread.sleep(100L);
    }
    return fs.exists(taskStateFile);
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
    suite.shutdownCluster();
  }

  @AfterMethod
  public void tearDown() throws IOException {
    this.suite.deleteWorkDir();
  }
}
