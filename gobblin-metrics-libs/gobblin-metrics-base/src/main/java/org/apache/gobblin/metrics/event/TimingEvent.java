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

package org.apache.gobblin.metrics.event;

import java.util.Map;

import com.google.common.collect.Maps;

/**
 * Event to time actions in the program. Automatically reports start time, end time, and duration from the time
 * the {@link org.apache.gobblin.metrics.event.TimingEvent} was created to the time {@link #stop} is called.
 */
public class TimingEvent {

  public static class LauncherTimings {
    public static final String FULL_JOB_EXECUTION = "FullJobExecutionTimer";
    public static final String WORK_UNITS_CREATION = "WorkUnitsCreationTimer";
    public static final String WORK_UNITS_PREPARATION = "WorkUnitsPreparationTimer";
    public static final String JOB_PREPARE = "JobPrepareTimer";
    public static final String JOB_START = "JobStartTimer";
    public static final String JOB_RUN = "JobRunTimer";
    public static final String JOB_COMMIT = "JobCommitTimer";
    public static final String JOB_CLEANUP = "JobCleanupTimer";
    public static final String JOB_CANCEL = "JobCancelTimer";
    public static final String JOB_COMPLETE = "JobCompleteTimer";
    public static final String JOB_FAILED = "JobFailedTimer";
  }

  public static class RunJobTimings {
    public static final String JOB_LOCAL_SETUP = "JobLocalSetupTimer";
    public static final String WORK_UNITS_RUN = "WorkUnitsRunTimer";
    public static final String WORK_UNITS_PREPARATION = "WorkUnitsPreparationTimer";
    public static final String MR_STAGING_DATA_CLEAN = "JobMrStagingDataCleanTimer";
    public static final String MR_DISTRIBUTED_CACHE_SETUP = "JobMrDistributedCacheSetupTimer";
    public static final String MR_JOB_SETUP = "JobMrSetupTimer";
    public static final String MR_JOB_RUN = "JobMrRunTimer";
    public static final String HELIX_JOB_SUBMISSION= "JobHelixSubmissionTimer";
    public static final String HELIX_JOB_RUN = "JobHelixRunTimer";
  }

  public static final String METADATA_START_TIME = "startTime";
  public static final String METADATA_END_TIME = "endTime";
  public static final String METADATA_DURATION = "durationMillis";
  public static final String METADATA_TIMING_EVENT = "timingEvent";

  private final String name;
  private final Long startTime;
  private final EventSubmitter submitter;
  private boolean stopped;

  public TimingEvent(EventSubmitter submitter, String name) {
    this.stopped = false;
    this.name = name;
    this.submitter = submitter;
    this.startTime = System.currentTimeMillis();
  }

  /**
   * Stop the timer and submit the event. If the timer was already stopped before, this is a no-op.
   */
  public void stop() {
    stop(Maps.<String, String> newHashMap());
  }

  /**
   * Stop the timer and submit the event, along with the additional metadata specified. If the timer was already stopped
   * before, this is a no-op.
   *
   * @param additionalMetadata a {@link Map} of additional metadata that should be submitted along with this event
   */
  public void stop(Map<String, String> additionalMetadata) {
    if (this.stopped) {
      return;
    }
    this.stopped = true;
    long endTime = System.currentTimeMillis();
    long duration = endTime - this.startTime;

    Map<String, String> finalMetadata = Maps.newHashMap();
    finalMetadata.putAll(additionalMetadata);
    finalMetadata.put(EventSubmitter.EVENT_TYPE, METADATA_TIMING_EVENT);
    finalMetadata.put(METADATA_START_TIME, Long.toString(this.startTime));
    finalMetadata.put(METADATA_END_TIME, Long.toString(endTime));
    finalMetadata.put(METADATA_DURATION, Long.toString(duration));

    this.submitter.submit(this.name, finalMetadata);
  }
}
