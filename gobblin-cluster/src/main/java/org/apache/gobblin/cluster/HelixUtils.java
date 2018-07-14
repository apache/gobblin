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
import java.util.concurrent.TimeoutException;

import org.apache.helix.HelixManager;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TargetState;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.apache.helix.tools.ClusterSetup;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;

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

  public static void submitJobToQueue(
      JobConfig.Builder jobConfigBuilder,
      String queueName,
      String jobName,
      TaskDriver helixTaskDriver,
      HelixManager helixManager,
      long jobQueueDeleteTimeoutSeconds) throws Exception {

    WorkflowConfig workflowConfig = helixTaskDriver.getWorkflowConfig(helixManager, queueName);

    log.info("[DELETE] workflow {} in the beginning", queueName);
    // If the queue is present, but in delete state then wait for cleanup before recreating the queue
    if (workflowConfig != null && workflowConfig.getTargetState() == TargetState.DELETE) {
      new TaskDriver(helixManager).deleteAndWaitForCompletion(queueName, jobQueueDeleteTimeoutSeconds);
      // if we get here then the workflow was successfully deleted
      workflowConfig = null;
    }

    // Create one queue for each job with the job name being the queue name
    if (workflowConfig == null) {
      JobQueue jobQueue = new JobQueue.Builder(queueName).build();
      helixTaskDriver.createQueue(jobQueue);
      log.info("Created job queue {}", queueName);
    } else {
      log.info("Job queue {} already exists", queueName);
    }

    // Put the job into the queue
    helixTaskDriver.enqueueJob(queueName, jobName, jobConfigBuilder);
  }

  public static void waitJobCompletion(
      HelixManager helixManager,
      String queueName,
      String jobName,
      Optional<Long> timeoutInSeconds) throws InterruptedException, TimeoutException {

    log.info("Waiting for job to complete...");
    long endTime = 0;
    if (timeoutInSeconds.isPresent()) {
      endTime = System.currentTimeMillis() + timeoutInSeconds.get() * 1000;
    }

    while (!timeoutInSeconds.isPresent() || System.currentTimeMillis() <= endTime) {
      WorkflowContext workflowContext = TaskDriver.getWorkflowContext(helixManager, queueName);
      if (workflowContext != null) {
        org.apache.helix.task.TaskState helixJobState = workflowContext.getJobState(TaskUtil.getNamespacedJobName(queueName, jobName));
        if (helixJobState == org.apache.helix.task.TaskState.COMPLETED ||
            helixJobState == org.apache.helix.task.TaskState.FAILED ||
            helixJobState == org.apache.helix.task.TaskState.STOPPED) {
          return;
        }
      }
      Thread.sleep(1000);
    }

    throw new TimeoutException("task driver wait time [" + timeoutInSeconds + " sec] is expired.");
  }
}
