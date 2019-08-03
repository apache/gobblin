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

package org.apache.gobblin.yarn;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.helix.HelixManager;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.JobDag;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.ExecutorsUtils;


/**
 * The autoscaling manager is responsible for figuring out how many containers are required for the workload and
 * requesting the {@link YarnService} to request that many containers.
 */
@Slf4j
public class YarnAutoScalingManager extends AbstractIdleService {
  private final String AUTO_SCALING_PREFIX = GobblinYarnConfigurationKeys.GOBBLIN_YARN_PREFIX + "autoScaling.";
  private final String AUTO_SCALING_POLLING_INTERVAL_SECS =
      AUTO_SCALING_PREFIX + "pollingIntervalSeconds";
  private final int DEFAULT_AUTO_SCALING_POLLING_INTERVAL_SECS = 60;
  // Only one container will be requested for each N partitions of work
  private final String AUTO_SCALING_PARTITIONS_PER_CONTAINER = AUTO_SCALING_PREFIX + "partitionsPerContainer";
  private final int DEFAULT_AUTO_SCALING_PARTITIONS_PER_CONTAINER = 1;
  private final String AUTO_SCALING_MIN_CONTAINERS = AUTO_SCALING_PREFIX + "minContainers";
  private final int DEFAULT_AUTO_SCALING_MIN_CONTAINERS = 1;
  private final String AUTO_SCALING_MAX_CONTAINERS = AUTO_SCALING_PREFIX + "maxContainers";
  private final int DEFAULT_AUTO_SCALING_MAX_CONTAINERS = Integer.MAX_VALUE;
  private final String AUTO_SCALING_INITIAL_DELAY = AUTO_SCALING_PREFIX + "initialDelay";
  private final int DEFAULT_AUTO_SCALING_INITIAL_DELAY_SECS = 60;

  private final Config config;
  private final HelixManager helixManager;
  private final ScheduledExecutorService autoScalingExecutor;
  private final YarnService yarnService;
  private final int partitionsPerContainer;
  private final int minContainers;
  private final int maxContainers;

  public YarnAutoScalingManager(GobblinApplicationMaster appMaster) {
    this.config = appMaster.getConfig();
    this.helixManager = appMaster.getMultiManager().getJobClusterHelixManager();
    this.yarnService = appMaster.getYarnService();
    this.partitionsPerContainer = ConfigUtils.getInt(this.config, AUTO_SCALING_PARTITIONS_PER_CONTAINER,
        DEFAULT_AUTO_SCALING_PARTITIONS_PER_CONTAINER);

    Preconditions.checkArgument(this.partitionsPerContainer > 0,
        AUTO_SCALING_PARTITIONS_PER_CONTAINER + " needs to be greater than 0");

    this.minContainers = ConfigUtils.getInt(this.config, AUTO_SCALING_MIN_CONTAINERS,
        DEFAULT_AUTO_SCALING_MIN_CONTAINERS);

    Preconditions.checkArgument(this.minContainers > 0,
        DEFAULT_AUTO_SCALING_MIN_CONTAINERS + " needs to be greater than 0");

    this.maxContainers = ConfigUtils.getInt(this.config, AUTO_SCALING_MAX_CONTAINERS,
        DEFAULT_AUTO_SCALING_MAX_CONTAINERS);

    Preconditions.checkArgument(this.maxContainers > 0,
        DEFAULT_AUTO_SCALING_MAX_CONTAINERS + " needs to be greater than 0");

    Preconditions.checkArgument(this.maxContainers >= this.minContainers,
        DEFAULT_AUTO_SCALING_MAX_CONTAINERS + " needs to be greater than or equal to "
            + DEFAULT_AUTO_SCALING_MIN_CONTAINERS);

    this.autoScalingExecutor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(log), Optional.of("AutoScalingExecutor")));
  }

  @Override
  protected void startUp() throws Exception {
    int scheduleInterval = ConfigUtils.getInt(this.config, AUTO_SCALING_POLLING_INTERVAL_SECS,
        DEFAULT_AUTO_SCALING_POLLING_INTERVAL_SECS);
    int initialDelay = ConfigUtils.getInt(this.config, AUTO_SCALING_INITIAL_DELAY,
        DEFAULT_AUTO_SCALING_INITIAL_DELAY_SECS);
    log.info("Starting the " + YarnAutoScalingManager.class.getSimpleName());
    log.info("Scheduling the auto scaling task with an interval of {} seconds", scheduleInterval);

    this.autoScalingExecutor.scheduleAtFixedRate(new YarnAutoScalingRunnable(new TaskDriver(this.helixManager),
            this.yarnService, this.partitionsPerContainer, this.minContainers, this.maxContainers), initialDelay,
        scheduleInterval, TimeUnit.SECONDS);
  }

  @Override
  protected void shutDown() throws Exception {
    log.info("Stopping the " + YarnAutoScalingManager.class.getSimpleName());

    ExecutorsUtils.shutdownExecutorService(this.autoScalingExecutor, Optional.of(log));
  }

  /**
   * A {@link Runnable} that figures out the number of containers required for the workload
   * and requests those containers.
   */
  @VisibleForTesting
  @AllArgsConstructor
  static class YarnAutoScalingRunnable implements Runnable {
    private final TaskDriver taskDriver;
    private final YarnService yarnService;
    private final int partitionsPerContainer;
    private final int minContainers;
    private final int maxContainers;


    @Override
    public void run() {
      // Suppress errors to avoid interrupting any scheduled executions of this Runnable
      try {
        runInternal();
      } catch (Throwable t) {
        log.warn("Suppressing error from YarnAutoScalingRunnable.run()", t);
      }
    }

    /**
     * Iterate through the workflows configured in Helix to figure out the number of required partitions
     * and request the {@link YarnService} to scale to the desired number of containers.
     */
    @VisibleForTesting
    void runInternal() {
      Set<String> inUseInstances = new HashSet<>();

      int numPartitions = 0;
      for (Map.Entry<String, WorkflowConfig> workFlowEntry : taskDriver.getWorkflows().entrySet()) {
        WorkflowContext workflowContext = taskDriver.getWorkflowContext(workFlowEntry.getKey());

        // Only allocate for active workflows
        if (workflowContext == null || !workflowContext.getWorkflowState().equals(TaskState.IN_PROGRESS)) {
          continue;
        }

        log.debug("Workflow name {} config {} context {}", workFlowEntry.getKey(), workFlowEntry.getValue(),
            workflowContext);

        WorkflowConfig workflowConfig = workFlowEntry.getValue();
        JobDag jobDag = workflowConfig.getJobDag();

        Set<String> jobs = jobDag.getAllNodes();

        // sum up the number of partitions
        for (String jobName : jobs) {
          JobContext jobContext = taskDriver.getJobContext(jobName);

          if (jobContext != null) {
            log.debug("JobContext {} num partitions {}", jobContext, jobContext.getPartitionSet().size());

            inUseInstances.addAll(jobContext.getPartitionSet().stream().map(jobContext::getAssignedParticipant)
                .filter(e -> e != null).collect(Collectors.toSet()));

            numPartitions += jobContext.getPartitionSet().size();
          }
        }
      }

      // compute the target containers as a ceiling of number of partitions divided by the number of containers
      // per partition.
      int numTargetContainers = (int) Math.ceil((double)numPartitions / this.partitionsPerContainer);

      // adjust the number of target containers based on the configured min and max container values.
      numTargetContainers = Math.max(this.minContainers, Math.min(this.maxContainers, numTargetContainers));

      this.yarnService.requestTargetNumberOfContainers(numTargetContainers, inUseInstances);
    }
  }
}
