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

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.helix.HelixManager;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.apache.helix.tools.ClusterSetup;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.JobException;
import org.apache.gobblin.runtime.listeners.JobListener;

import static org.apache.helix.task.TaskState.STOPPED;


/**
 * A utility class for working with Gobblin on Helix.
 *
 * @author Yinan Li
 */
@Alpha
@Slf4j
public class HelixUtils {

  /**
   * Create a Helix cluster for the Gobblin Cluster application.
   *
   * @param zkConnectionString the ZooKeeper connection string
   * @param clusterName the Helix cluster name
   */
  public static void createGobblinHelixCluster(
      String zkConnectionString,
      String clusterName) {
    createGobblinHelixCluster(zkConnectionString, clusterName, true);
  }

  /**
   * Create a Helix cluster for the Gobblin Cluster application.
   *
   * @param zkConnectionString the ZooKeeper connection string
   * @param clusterName the Helix cluster name
   * @param overwrite true to overwrite exiting cluster, false to reuse existing cluster
   */
  public static void createGobblinHelixCluster(
      String zkConnectionString,
      String clusterName,
      boolean overwrite) {
    ClusterSetup clusterSetup = new ClusterSetup(zkConnectionString);
    // Create the cluster and overwrite if it already exists
    clusterSetup.addCluster(clusterName, overwrite);
    // Helix 0.6.x requires a configuration property to have the form key=value.
    String autoJoinConfig = ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN + "=true";
    clusterSetup.setConfig(HelixConfigScope.ConfigScopeProperty.CLUSTER, clusterName, autoJoinConfig);
  }

  /**
   * Get a Helix instance name.
   *
   * @param namePrefix a prefix of Helix instance names
   * @param instanceId an integer instance ID
   * @return a Helix instance name that is a concatenation of the given prefix and instance ID
   */
  public static String getHelixInstanceName(
      String namePrefix,
      int instanceId) {
    return namePrefix + "_" + instanceId;
  }

  // We have switched from Helix JobQueue to WorkFlow based job execution.
  @Deprecated
  public static void submitJobToQueue(
      JobConfig.Builder jobConfigBuilder,
      String queueName,
      String jobName,
      TaskDriver helixTaskDriver,
      HelixManager helixManager,
      long jobQueueDeleteTimeoutSeconds) throws Exception {
    submitJobToWorkFlow(jobConfigBuilder, queueName, jobName, helixTaskDriver, helixManager, jobQueueDeleteTimeoutSeconds);
  }

  public static void submitJobToWorkFlow(JobConfig.Builder jobConfigBuilder,
      String workFlowName,
      String jobName,
      TaskDriver helixTaskDriver,
      HelixManager helixManager,
      long workFlowExpiryTime) throws Exception {

    WorkflowConfig workFlowConfig = new WorkflowConfig.Builder().setExpiry(workFlowExpiryTime, TimeUnit.SECONDS).build();
    // Create a work flow for each job with the name being the queue name
    Workflow workFlow = new Workflow.Builder(workFlowName).setWorkflowConfig(workFlowConfig).addJob(jobName, jobConfigBuilder).build();
    // start the workflow
    helixTaskDriver.start(workFlow);
    log.info("Created a work flow {}", workFlowName);
    WorkflowContext workflowContext = TaskDriver.getWorkflowContext(helixManager, workFlowName);

    // If the helix job is deleted from some other thread or a completely external process,
    // method waitJobCompletion() needs to differentiate between the cases where
    // 1) workflowContext did not get initialized ever, in which case we need to keep waiting, or
    // 2) it did get initialized but deleted soon after, in which case we should stop waiting
    // To overcome this issue, we wait here till workflowContext gets initialized

    while (workflowContext == null || workflowContext.getJobState(TaskUtil.getNamespacedJobName(workFlowName, jobName)) == null) {
      workflowContext = TaskDriver.getWorkflowContext(helixManager, workFlowName);
      Thread.sleep(1000);
      log.info("Waiting for work flow initialization.");
    }
    log.info("Work flow {} initialized", workFlowName);
  }

  static void waitJobCompletion(HelixManager helixManager, String workFlowName, String jobName,
      Optional<Long> timeoutInSeconds) throws InterruptedException, TimeoutException {

    log.info("Waiting for job {} to complete...", jobName);
    long endTime = 0;
    if (timeoutInSeconds.isPresent()) {
      endTime = System.currentTimeMillis() + timeoutInSeconds.get() * 1000;
    }

    while (!timeoutInSeconds.isPresent() || System.currentTimeMillis() <= endTime) {
      WorkflowContext workflowContext = TaskDriver.getWorkflowContext(helixManager, workFlowName);
      if (workflowContext != null) {
        TaskState jobState = workflowContext.getJobState(TaskUtil.getNamespacedJobName(workFlowName, jobName));
        switch (jobState) {
          case STOPPED:
            // user requested cancellation, which is executed by executeCancellation()
            log.info("Job {} is cancelled, it will be deleted now.", jobName);
            HelixUtils.deleteStoppedHelixJob(helixManager, workFlowName, jobName);
            return;
          case FAILED:
          case COMPLETED:
          return;
          default:
            log.info("Waiting for job {} to complete...", jobName);
            Thread.sleep(1000);
        }
      } else {
        // We have waited for WorkflowContext to get initialized,
        // so it is found null here, it must have been deleted in job cancellation process.
        log.info("WorkflowContext not found. Job is probably cancelled.");
        return;
      }
    }

    throw new TimeoutException("task driver wait time [" + timeoutInSeconds + " sec] is expired.");
  }

  static void handleJobTimeout(String workFlowName, String jobName, HelixManager helixManager, Object jobLauncher,
      JobListener jobListener) throws InterruptedException {
    try {
      if (jobLauncher instanceof GobblinHelixJobLauncher) {
        ((GobblinHelixJobLauncher) jobLauncher).cancelJob(jobListener);
      } else if (jobLauncher instanceof GobblinHelixDistributeJobExecutionLauncher) {
        ((GobblinHelixDistributeJobExecutionLauncher) jobLauncher).cancel();
      } else {
        log.warn("Timeout occured for unknown job launcher {}", jobLauncher.getClass());
      }
    } catch (JobException e) {
      throw new RuntimeException("Unable to cancel job " + jobName + ": ", e);
    }
    // TODO : fix this when HELIX-1180 is completed
    // We should not be deleting a workflow explicitly.
    // Workflow state should be set to a final state, which will remove it automatically because expiry time is set.
    // After that, all delete calls can be replaced by something like HelixUtils.setStateToFinal();
    HelixUtils.deleteStoppedHelixJob(helixManager, workFlowName, jobName);
    log.info("Stopped and deleted the workflow {}", workFlowName);
  }

  /**
   * Deletes the stopped Helix Workflow.
   * Caller should stop the Workflow before calling this method.
   * @param helixManager helix manager
   * @param workFlowName workflow needed to be deleted
   * @param jobName helix job name
   * @throws InterruptedException
   */
  private static void deleteStoppedHelixJob(HelixManager helixManager, String workFlowName, String jobName)
      throws InterruptedException {
    WorkflowContext workflowContext = TaskDriver.getWorkflowContext(helixManager, workFlowName);
    while (workflowContext.getJobState(TaskUtil.getNamespacedJobName(workFlowName, jobName)) != STOPPED) {
      log.info("Waiting for job {} to stop...", jobName);
      workflowContext = TaskDriver.getWorkflowContext(helixManager, workFlowName);
      Thread.sleep(1000);
    }
    // deleting the entire workflow, as one workflow contains only one job
    new TaskDriver(helixManager).deleteAndWaitForCompletion(workFlowName, 10000L);
    log.info("Workflow deleted.");
  }
}