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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.helix.HelixManager;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.apache.helix.tools.ClusterSetup;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.JobException;
import org.apache.gobblin.runtime.listeners.JobListener;

import static org.apache.helix.task.TaskState.STOPPED;


/**
 * A utility class for working with Gobblin on Helix.
 *
 * @author Yinan Li
 */
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

  static void waitJobInitialization(
      HelixManager helixManager,
      String workFlowName,
      String jobName,
      long timeoutMillis) throws Exception {
    WorkflowContext workflowContext = TaskDriver.getWorkflowContext(helixManager, workFlowName);

    // If the helix job is deleted from some other thread or a completely external process,
    // method waitJobCompletion() needs to differentiate between the cases where
    // 1) workflowContext did not get initialized ever, in which case we need to keep waiting, or
    // 2) it did get initialized but deleted soon after, in which case we should stop waiting
    // To overcome this issue, we wait here till workflowContext gets initialized
    long start = System.currentTimeMillis();
    while (workflowContext == null || workflowContext.getJobState(TaskUtil.getNamespacedJobName(workFlowName, jobName)) == null) {
      if (System.currentTimeMillis() - start > timeoutMillis) {
        log.error("Job cannot be initialized within {} milliseconds, considered as an error", timeoutMillis);
        throw new JobException("Job cannot be initialized within {} milliseconds, considered as an error");
      }
      workflowContext = TaskDriver.getWorkflowContext(helixManager, workFlowName);
      Thread.sleep(1000);
      log.info("Waiting for work flow initialization.");
    }

    log.info("Work flow {} initialized", workFlowName);
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

    waitJobInitialization(helixManager, workFlowName, jobName, Long.MAX_VALUE);
  }

  static void waitJobCompletion(HelixManager helixManager, String workFlowName, String jobName,
      Optional<Long> timeoutInSeconds, Long stoppingStateTimeoutInSeconds) throws InterruptedException, TimeoutException {
    log.info("Waiting for job {} to complete...", jobName);
    long endTime = 0;
    long currentTimeMillis = System.currentTimeMillis();

    if (timeoutInSeconds.isPresent()) {
      endTime = currentTimeMillis + timeoutInSeconds.get() * 1000;
    }

    long stoppingStateEndTime = currentTimeMillis + stoppingStateTimeoutInSeconds * 1000;

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
          case STOPPING:
            log.info("Waiting for job {} to complete... State - {}", jobName, jobState);
            Thread.sleep(1000);
            // Workaround for a Helix bug where a job may be stuck in the STOPPING state due to an unresponsive task.
            if (System.currentTimeMillis() > stoppingStateEndTime) {
              log.info("Deleting workflow {}", workFlowName);
              new TaskDriver(helixManager).delete(workFlowName);
              log.info("Deleted workflow {}", workFlowName);
            }
            return;
          default:
            log.info("Waiting for job {} to complete... State - {}", jobName, jobState);
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

  static boolean isJobFinished(String workflowName, String jobName, HelixManager helixManager) {
    WorkflowContext workflowContext = TaskDriver.getWorkflowContext(helixManager, workflowName);
    if (workflowContext == null) {
      // this workflow context doesn't exist, considered as finished.
      return true;
    }

    TaskState jobState = workflowContext.getJobState(TaskUtil.getNamespacedJobName(workflowName, jobName));
    switch (jobState) {
      case STOPPED:
      case FAILED:
      case COMPLETED:
      case ABORTED:
      case TIMED_OUT:
        return true;
      default:
        return false;
    }
  }

  static void deleteWorkflow (String workflowName, HelixManager helixManager, long timeOut) throws InterruptedException {
    TaskDriver taskDriver = new TaskDriver(helixManager);
    taskDriver.deleteAndWaitForCompletion(workflowName, timeOut);
  }

  static void handleJobTimeout(String workFlowName, String jobName, HelixManager helixManager, Object jobLauncher,
      JobListener jobListener) throws InterruptedException {
    try {
      log.warn("Timeout occurred for job launcher {} with job {}", jobLauncher.getClass(), jobName);
      if (jobLauncher instanceof GobblinHelixJobLauncher) {
        ((GobblinHelixJobLauncher) jobLauncher).cancelJob(jobListener);
      } else if (jobLauncher instanceof GobblinHelixDistributeJobExecutionLauncher) {
        ((GobblinHelixDistributeJobExecutionLauncher) jobLauncher).cancel();
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

  /**
   * Returns the Helix Workflow Ids given {@link Iterable} of Gobblin job names. The method returns a
   * {@link java.util.Map} from Gobblin job name to the corresponding Helix Workflow Id. This method iterates
   * over all Helix workflows, and obtains the jobs of each workflow from its jobDag.
   *
   * NOTE: This call is expensive as it results in listing of znodes and subsequently, multiple ZK calls to get the job
   * configuration for each HelixJob. Ideally, this method should be called infrequently e.g. when a job is deleted/cancelled.
   *
   * @param jobNames a list of Gobblin job names.
   * @return a map from jobNames to their Helix Workflow Ids.
   */
  public static Map<String, String> getWorkflowIdsFromJobNames(HelixManager helixManager, Collection<String> jobNames) {
    Map<String, String> jobNameToWorkflowId = new HashMap<>();
    TaskDriver taskDriver = new TaskDriver(helixManager);
    Map<String, WorkflowConfig> workflowConfigMap = taskDriver.getWorkflows();
    for (String workflow : workflowConfigMap.keySet()) {
      WorkflowConfig workflowConfig = taskDriver.getWorkflowConfig(workflow);
      Set<String> helixJobs = workflowConfig.getJobDag().getAllNodes();
      for (String helixJob : helixJobs) {
        Iterator<TaskConfig> taskConfigIterator = taskDriver.getJobConfig(helixJob).getTaskConfigMap().values().iterator();
        if (taskConfigIterator.hasNext()) {
          TaskConfig taskConfig = taskConfigIterator.next();
          String jobName = taskConfig.getConfigMap().get(ConfigurationKeys.JOB_NAME_KEY);
          if (jobNames.contains(jobName)) {
            if (!jobNameToWorkflowId.containsKey(jobName)) {
              jobNameToWorkflowId.put(jobName, workflow);
            } else {
              log.warn("JobName {} previously found to have WorkflowId {}; found " + " a different WorkflowId {} for the job; "
                  + "Skipping this entry", jobName, jobNameToWorkflowId.get(jobName), workflow);
            }
            break;
          }
        }
      }
    }
    return jobNameToWorkflowId;
  }
}