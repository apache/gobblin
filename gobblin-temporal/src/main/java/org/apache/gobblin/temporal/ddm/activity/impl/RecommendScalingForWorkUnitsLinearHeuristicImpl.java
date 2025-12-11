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

package org.apache.gobblin.temporal.ddm.activity.impl;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.temporal.ddm.work.TimeBudget;
import org.apache.gobblin.temporal.ddm.work.WorkUnitsSizeSummary;
import org.apache.gobblin.yarn.GobblinYarnConfigurationKeys;


/**
 * Simple config-driven linear recommendation for how many containers to use to complete the "remaining work" within a given {@link TimeBudget}, per:
 *
 *   a. from {@link WorkUnitsSizeSummary}, find how many (remaining) {@link org.apache.gobblin.source.workunit.WorkUnit}s
 *   b. from the configured GobblinTemporalConfigurationKeys.TEMPORAL_WORKER_THREAD_AMORTIZED_THROUGHPUT_PER_MINUTE, find the expected "processing rate" in bytes / minute
 * 1. estimate the total container-minutes required to process all MWUs
 *   c. from the {@link TimeBudget}, find the target number of minutes in which to complete processing of all MWUs
 * 2. estimate container count based on target minutes
 * 2. estimate container count based on the maximum number of work units allowed per container
 * 4. recommend the number of containers as max of above two container counts
 */
@Slf4j
public class RecommendScalingForWorkUnitsLinearHeuristicImpl extends AbstractRecommendScalingForWorkUnitsImpl {

  /**
   * Calculates the recommended number of containers for processing the remaining work units.
   * <p>
   * This method first checks whether dynamic scaling is enabled via the job state configuration.
   * If dynamic scaling is disabled, it returns the initial container count as specified in the job state.
   * When dynamic scaling is enabled, it computes the throughput based on the count of constituent work units (WUs)
   * and the processing rate (bytes per minute per thread). The calculation involves:
   * <ol>
   *   <li>Computing the total bytes to be processed based on the count and mean size of top-level work units.</li>
   *   <li>Calculating the processing rate per container using the amortized bytes per minute rate and the container's work unit capacity.</li>
   *   <li>Estimating the total container-minutes required to process all MWUs and determining the number of containers needed
   *       to meet the job time budget.</li>
   *   <li>Computing an alternative container count based on the maximum number of work units allowed per container.</li>
   *   <li>Returning the maximum of the two computed container counts as the recommended scaling.</li>
   * </ol>
   * </p>
   *
   * @param remainingWork the summary of work unit sizes and counts remaining for processing
   * @param sourceClass the name of the class invoking this method
   * @param jobTimeBudget the time budget allocated for the job execution
   * @param jobState the current job state that holds configuration properties and runtime parameters
   * @return the recommended number of containers to allocate for processing the work units
   */
  @Override
  protected int calcDerivationSetPoint(WorkUnitsSizeSummary remainingWork, String sourceClass, TimeBudget jobTimeBudget, JobState jobState) {
    if (!jobState.getPropAsBoolean(GobblinTemporalConfigurationKeys.DYNAMIC_SCALING_ENABLED, false)) {
      int initialContainerCount = Integer.parseInt(jobState.getProp(GobblinYarnConfigurationKeys.INITIAL_CONTAINERS_KEY, "1"));
      log.info("Dynamic scaling is disabled, returning initial container count: " + initialContainerCount);
      return initialContainerCount;
    }

    long numWUs = remainingWork.getConstituentWorkUnitsCount();
    long numMWUs = remainingWork.getTopLevelWorkUnitsCount();
    double meanBytesPerMWU = remainingWork.getTopLevelWorkUnitsMeanSize();
    double totalBytes = numMWUs * meanBytesPerMWU;
    long bytesPerMinuteProcRatePerThread = calcAmortizedBytesPerMinute(jobState);
    log.info("Calculating auto-scaling (for {} remaining work units within {}) using: bytesPerMinuteProcRatePerThread = {}; meanBytesPerMWU = {}",
        numMWUs, jobTimeBudget, bytesPerMinuteProcRatePerThread, meanBytesPerMWU);

    // calc how many container*minutes to process all MWUs, based on mean MWU size
    int numSimultaneousMWUsPerContainer = calcPerContainerWUCapacity(jobState); // (a worker-thread is a slot for top-level (MWUs) - not constituent sub-WUs)
    double containerProcRate = bytesPerMinuteProcRatePerThread * numSimultaneousMWUsPerContainer;
    double estContainerMinutesForAllMWUs = totalBytes/containerProcRate;
    log.info("Container byte processing throughput: {}, totalBytes: {}, est. containerMinutes to complete all MWUs: {}",
        containerProcRate, totalBytes, estContainerMinutesForAllMWUs);

    // Determine the required number of containers based on the job's time budget
    long targetNumMinutesForAllMWUs = jobTimeBudget.getMaxTargetDurationMinutes();
    int numContainerForThroughout = (int) Math.ceil(estContainerMinutesForAllMWUs / targetNumMinutesForAllMWUs);

    // Determine the required number of containers based on work unit count limits
    int maxWUsPerContainer = calcMaxWUPerContainer(jobState);
    int numContainerForWUs = (int) Math.ceil((numWUs * 1.0) / maxWUsPerContainer);

    int recommendedNumContainers = Math.max(numContainerForWUs, numContainerForThroughout);
    log.info("Recommended auto-scaling: {} containers, no. of containers considering throughput: {}, no. of containers considering WUs: {}",
        recommendedNumContainers, numContainerForThroughout, numContainerForWUs);
/**
 *
 */
    return recommendedNumContainers;
  }

  /**
   * Calculates the work unit processing capacity for a single container.
   * <p>
   * This is determined by multiplying the number of workers assigned to a container by the number of threads available per worker.
   * </p>
   *
   * @param jobState the current job state containing configuration properties
   * @return the number of top-level work units (MWUs) that can be processed concurrently by one container
   */
  protected int calcPerContainerWUCapacity(JobState jobState) {
    int numWorkersPerContainer = jobState.getPropAsInt(GobblinTemporalConfigurationKeys.TEMPORAL_NUM_WORKERS_PER_CONTAINER,
        GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_NUM_WORKERS_PER_CONTAINERS);
    int numThreadsPerWorker = jobState.getPropAsInt(GobblinTemporalConfigurationKeys.TEMPORAL_NUM_THREADS_PER_WORKER,
        GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_NUM_THREADS_PER_WORKER);
    return numWorkersPerContainer * numThreadsPerWorker;
  }


  /**
   * Retrieves the amortized throughput rate in bytes per minute per thread from the job configuration.
   * <p>
   * This value represents the expected processing rate for a worker thread and is used to compute container's processing rate.
   * </p>
   *
   * @param jobState the current job state containing configuration properties
   * @return the amortized processing rate in bytes per minute per thread
   */
  protected long calcAmortizedBytesPerMinute(JobState jobState) {
    return jobState.getPropAsLong(GobblinTemporalConfigurationKeys.TEMPORAL_WORKER_THREAD_AMORTIZED_THROUGHPUT_PER_MINUTE,
        GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_WORKER_THREAD_AMORTIZED_THROUGHPUT_PER_MINUTE);
  }

  /**
   * Determines the maximum number of work units that can be assigned to a single container.
   * <p>
   * This limit is obtained from the job configuration and is used as an alternative constraint when scaling containers.
   * </p>
   *
   * @param jobState the current job state containing configuration properties
   * @return the maximum allowed number of work units per container
   */
  protected int calcMaxWUPerContainer(JobState jobState) {
    return jobState.getPropAsInt(GobblinTemporalConfigurationKeys.TEMPORAL_WORKUNITS_PER_CONTAINER,
        GobblinTemporalConfigurationKeys.DEFAULT_MAX_WORKUNITS_PER_CONTAINER);
  }

}
