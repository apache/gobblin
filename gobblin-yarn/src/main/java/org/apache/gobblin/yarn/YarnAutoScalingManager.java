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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.task.*;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.gobblin.yarn.GobblinYarnTaskRunner.HELIX_YARN_INSTANCE_NAME_PREFIX;


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
  private final String AUTO_SCALING_CONTAINER_OVERPROVISION_FACTOR = AUTO_SCALING_PREFIX + "overProvisionFactor";
  private final double DEFAULT_AUTO_SCALING_CONTAINER_OVERPROVISION_FACTOR = 1.0;

  // A rough value of how much containers should be an intolerable number.
  private final int DEFAULT_AUTO_SCALING_MAX_CONTAINERS = Integer.MAX_VALUE;
  private final String AUTO_SCALING_INITIAL_DELAY = AUTO_SCALING_PREFIX + "initialDelay";
  private final int DEFAULT_AUTO_SCALING_INITIAL_DELAY_SECS = 60;

  private final String AUTO_SCALING_WINDOW_SIZE = AUTO_SCALING_PREFIX + "windowSize";

  private final static int DEFAULT_MAX_IDLE_TIME_BEFORE_SCALING_DOWN_MINUTES = 10;

  private final Config config;
  private final HelixManager helixManager;
  private final ScheduledExecutorService autoScalingExecutor;
  private final YarnService yarnService;
  private final int partitionsPerContainer;
  private final int minContainers;
  private final int maxContainers;
  private final double overProvisionFactor;
  private final SlidingWindowReservoir slidingFixedSizeWindow;
  private static int maxIdleTimeInMinutesBeforeScalingDown = DEFAULT_MAX_IDLE_TIME_BEFORE_SCALING_DOWN_MINUTES;

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

    this.overProvisionFactor = ConfigUtils.getDouble(this.config, AUTO_SCALING_CONTAINER_OVERPROVISION_FACTOR,
        DEFAULT_AUTO_SCALING_CONTAINER_OVERPROVISION_FACTOR);

    Preconditions.checkArgument(this.maxContainers > 0,
        DEFAULT_AUTO_SCALING_MAX_CONTAINERS + " needs to be greater than 0");

    Preconditions.checkArgument(this.maxContainers >= this.minContainers,
        DEFAULT_AUTO_SCALING_MAX_CONTAINERS + " needs to be greater than or equal to "
            + DEFAULT_AUTO_SCALING_MIN_CONTAINERS);

    this.slidingFixedSizeWindow = config.hasPath(AUTO_SCALING_WINDOW_SIZE)
        ? new SlidingWindowReservoir(maxContainers, config.getInt(AUTO_SCALING_WINDOW_SIZE))
        : new SlidingWindowReservoir(maxContainers);

    this.autoScalingExecutor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(log), Optional.of("AutoScalingExecutor")));
  }

  @Override
  protected void startUp() {
    int scheduleInterval = ConfigUtils.getInt(this.config, AUTO_SCALING_POLLING_INTERVAL_SECS,
        DEFAULT_AUTO_SCALING_POLLING_INTERVAL_SECS);
    int initialDelay = ConfigUtils.getInt(this.config, AUTO_SCALING_INITIAL_DELAY,
        DEFAULT_AUTO_SCALING_INITIAL_DELAY_SECS);
    log.info("Starting the " + YarnAutoScalingManager.class.getSimpleName());
    log.info("Scheduling the auto scaling task with an interval of {} seconds", scheduleInterval);

    this.autoScalingExecutor.scheduleAtFixedRate(new YarnAutoScalingRunnable(new TaskDriver(this.helixManager),
            this.yarnService, this.partitionsPerContainer, this.minContainers, this.maxContainers, this.overProvisionFactor,
            this.slidingFixedSizeWindow, this.helixManager.getHelixDataAccessor()), initialDelay, scheduleInterval,
        TimeUnit.SECONDS);
  }

  @Override
  protected void shutDown() {
    log.info("Stopping the " + YarnAutoScalingManager.class.getSimpleName());

    ExecutorsUtils.shutdownExecutorService(this.autoScalingExecutor);
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
    private final double overProvisionFactor;
    private final SlidingWindowReservoir slidingWindowReservoir;
    private final HelixDataAccessor helixDataAccessor;
    /**
     * A static map that keep track of an idle instance and its latest beginning idle time.
     * If an instance is no longer idle when inspected, it will be dropped from this map.
     */
    private static final Map<String, Long> instanceIdleSince = new HashMap<>();


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
     * Getting all instances (Helix Participants) in cluster at this moment.
     * Note that the raw result could contains AppMaster node and replanner node.
     * @param filterString Helix instances whose name containing fitlerString will pass filtering.
     */
    private Set<String> getParticipants(String filterString) {
      PropertyKey.Builder keyBuilder = helixDataAccessor.keyBuilder();
      return helixDataAccessor.getChildValuesMap(keyBuilder.liveInstances())
          .keySet().stream().filter(x -> filterString.isEmpty() || x.contains(filterString)).collect(Collectors.toSet());
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

      // Find all participants appearing in this cluster. Note that Helix instances can contain cluster-manager
      // and potentially replanner-instance.
      Set<String> allParticipants = getParticipants(HELIX_YARN_INSTANCE_NAME_PREFIX);

      // Find all joined participants not in-use for this round of inspection.
      // If idle time is beyond tolerance, mark the instance as unused by assigning timestamp as -1.
      for (String participant : allParticipants) {
        if (!inUseInstances.contains(participant)) {
          instanceIdleSince.putIfAbsent(participant, System.currentTimeMillis());
          if (!isInstanceUnused(participant)) {
            inUseInstances.add(participant);
          }
        } else {
          // A previously idle instance is now detected to be in use.
          // Remove this instance if existed in the tracking map.
          instanceIdleSince.remove(participant);
        }
      }

      // compute the target containers as a ceiling of number of partitions divided by the number of containers
      // per partition. Scale the result by a constant overprovision factor.
      int numTargetContainers = (int) Math.ceil(((double)numPartitions / this.partitionsPerContainer) * this.overProvisionFactor);

      // adjust the number of target containers based on the configured min and max container values.
      numTargetContainers = Math.max(this.minContainers, Math.min(this.maxContainers, numTargetContainers));

      slidingWindowReservoir.add(numTargetContainers);

      log.info("There are {} containers being requested", numTargetContainers);

      this.yarnService.requestTargetNumberOfContainers(slidingWindowReservoir.getMax(), inUseInstances);
    }

    @VisibleForTesting
    /**
     * Return true is the condition for tagging an instance as "unused" holds.
     * The condition, by default is that if an instance went back to
     * active (having partition running on it) within {@link #maxIdleTimeInMinutesBeforeScalingDown} minutes, we will
     * not tag that instance as "unused" and have that as the candidate for scaling down.
     */
    boolean isInstanceUnused(String participant){
      return System.currentTimeMillis() - instanceIdleSince.get(participant) >
          TimeUnit.MINUTES.toMillis(maxIdleTimeInMinutesBeforeScalingDown);
    }
  }

  /**
   * A FIFO queue with fixed size and returns maxValue among all elements within the queue in constant time.
   * This data structure prevents temporary fluctuation in the number of active helix partitions as the size of queue
   * grows and will be less sensitive when scaling down is actually required.
   *
   * The interface for this is implemented in a minimal-necessity manner to serve only as a sliding-sized-window
   * which captures max value. It is NOT built for general purpose.
   */
  static class SlidingWindowReservoir {
    private ArrayDeque<Integer> fifoQueue;
    private PriorityQueue<Integer> priorityQueue;

    // Queue Size
    private int maxSize;
    private static final int DEFAULT_MAX_SIZE = 10;

    // Upper-bound of value within the queue.
    private int upperBound;

    public SlidingWindowReservoir(int maxSize, int upperBound) {
      Preconditions.checkArgument(maxSize > 0, "maxSize has to be a value larger than 0");

      this.maxSize = maxSize;
      this.upperBound = upperBound;
      this.fifoQueue = new ArrayDeque<>(maxSize);
      this.priorityQueue = new PriorityQueue<>(maxSize, new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
          return o2.compareTo(o1);
        }
      });
    }

    public SlidingWindowReservoir(int upperBound) {
      this(DEFAULT_MAX_SIZE, upperBound);
    }

    /**
     * Add element into data structure.
     * When a new element is larger than upperbound, reject the value since we may request too many Yarn containers.
     * When queue is full, evict head of FIFO-queue (In FIFO queue, elements are inserted from tail).
     */
    public void add(int e) {
      if (e > upperBound) {
        log.error(String.format("Request of getting %s containers seems to be excessive, rejected", e));
        return;
      }

      if (fifoQueue.size() == maxSize) {
        Integer removedElement = fifoQueue.remove();
        priorityQueue.remove(removedElement);
      }

      if (fifoQueue.size() == priorityQueue.size()) {
        fifoQueue.add(e);
        priorityQueue.add(e);
      } else {
        throw new IllegalStateException("Queue has its internal data structure being inconsistent.");
      }
    }

    /**
     * If queue is empty, throw {@link IllegalStateException}.
     */
    public int getMax() {
      if (priorityQueue.size() > 0) {
        return this.priorityQueue.peek();
      } else {
        throw new IllegalStateException("Queried before elements added into the queue.");
      }
    }
  }
}
