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
import org.apache.gobblin.temporal.ddm.worker.WorkFulfillmentWorker;


/**
 * Simple config-driven linear relationship between `remainingWork` and the resulting `setPoint`
 *
 *
 * TODO: describe algo!!!!!
 */
@Slf4j
public class RecommendScalingForWorkUnitsLinearHeuristicImpl extends AbstractRecommendScalingForWorkUnitsImpl {

  public static final String AMORTIZED_NUM_BYTES_PER_MINUTE = GobblinTemporalConfigurationKeys.DYNAMIC_SCALING_PREFIX + "heuristic.params.numBytesPerMinute";
  public static final long DEFAULT_AMORTIZED_NUM_BYTES_PER_MINUTE = 10 * 1000L * 1000L * 60L; // 10MB/sec

  @Override
  protected int calcDerivationSetPoint(WorkUnitsSizeSummary remainingWork, String sourceClass, TimeBudget jobTimeBudget, JobState jobState) {
    // for simplicity, for now, consider only top-level work units (aka. `MultiWorkUnit`s - MWUs)
    long numMWUs = remainingWork.getTopLevelWorkUnitsCount();
    double meanBytesPerMWU = remainingWork.getTopLevelWorkUnitsMeanSize();
    int numSimultaneousMWUsPerContainer = calcPerContainerWUCapacity(jobState); // (a worker-thread is a slot for top-level (MWUs) - not constituent sub-WUs)
    long bytesPerMinuteProcRate = calcAmortizedBytesPerMinute(jobState);
    log.info("Calculating auto-scaling (for {} remaining work units within {}) using: bytesPerMinuteProcRate = {}; meanBytesPerMWU = {}",
        numMWUs, jobTimeBudget, bytesPerMinuteProcRate, meanBytesPerMWU);

    // calc how many container*minutes to process all MWUs, based on mean MWU size
    double minutesProcTimeForMeanMWU = meanBytesPerMWU / bytesPerMinuteProcRate;
    double meanMWUsThroughputPerContainerMinute = numSimultaneousMWUsPerContainer / minutesProcTimeForMeanMWU;
    double estContainerMinutesForAllMWUs = numMWUs / meanMWUsThroughputPerContainerMinute;

    long targetNumMinutesForAllMWUs = jobTimeBudget.getMaxDurationDesiredMinutes();
    // TODO: take into account `jobTimeBudget.getPermittedOverageMinutes()` - e.g. to decide whether to use `Math.ceil` vs. `Math.floor`

    // TODO: decide how to account for container startup; working est. for GoT-on-YARN ~ 3 mins (req to alloc ~ 30s; alloc to workers ready ~ 2.5m)
    //   e.g. can we amortize away / ignore when `targetNumMinutesForAllMWUs >> workerRequestToReadyNumMinutes`?
    // TODO take into account that MWUs are quantized into discrete chunks; this est. uses avg and presumes to divide partial MWUs amongst workers
    //   can we we mostly ignore if we keep MWU "chunk size" "small-ish", like maybe even just `duration(max(MWU)) <= targetNumMinutesForAllMWUs/2)`?

    int recommendedNumContainers = (int) Math.floor(estContainerMinutesForAllMWUs / targetNumMinutesForAllMWUs);
    log.info("Recommended auto-scaling: {} containers, given: minutesToProc(mean(MWUs)) = {}; throughput = {} (MWUs / container*minute); "
        + "est. container*minutes to complete ALL ({}) MWUs = {}",
        recommendedNumContainers, minutesProcTimeForMeanMWU, meanMWUsThroughputPerContainerMinute, numMWUs, estContainerMinutesForAllMWUs);
    return recommendedNumContainers;
  }

  protected int calcPerContainerWUCapacity(JobState jobState) {
    int numWorkersPerContainer = jobState.getPropAsInt(GobblinTemporalConfigurationKeys.TEMPORAL_NUM_WORKERS_PER_CONTAINER,
        GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_NUM_WORKERS_PER_CONTAINERS);
    int numThreadsPerWorker = WorkFulfillmentWorker.MAX_EXECUTION_CONCURRENCY; // TODO: get from config, once that's implemented
    return numWorkersPerContainer * numThreadsPerWorker;
  }

  protected long calcAmortizedBytesPerMinute(JobState jobState) {
    return jobState.getPropAsLong(AMORTIZED_NUM_BYTES_PER_MINUTE, DEFAULT_AMORTIZED_NUM_BYTES_PER_MINUTE);
  }
}
