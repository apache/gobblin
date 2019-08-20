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

import java.io.Closeable;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Maps;

import lombok.Getter;

import org.apache.gobblin.metrics.GobblinTrackingEvent;


/**
 * Event to time actions in the program. Automatically reports start time, end time, and duration from the time
 * the {@link org.apache.gobblin.metrics.event.TimingEvent} was created to the time {@link #stop} is called.
 */
public class TimingEvent extends GobblinEventBuilder implements Closeable {

  public static class LauncherTimings {
    public static final String FULL_JOB_EXECUTION = "FullJobExecutionTimer";
    public static final String WORK_UNITS_CREATION = "WorkUnitsCreationTimer";
    public static final String WORK_UNITS_PREPARATION = "WorkUnitsPreparationTimer";
    public static final String JOB_PENDING = "JobPending";
    public static final String JOB_ORCHESTRATED = "JobOrchestrated";
    public static final String JOB_PREPARE = "JobPrepareTimer";
    public static final String JOB_START = "JobStartTimer";
    public static final String JOB_RUN = "JobRunTimer";
    public static final String JOB_COMMIT = "JobCommitTimer";
    public static final String JOB_CLEANUP = "JobCleanupTimer";
    public static final String JOB_CANCEL = "JobCancelTimer";
    public static final String JOB_COMPLETE = "JobCompleteTimer";
    public static final String JOB_FAILED = "JobFailedTimer";
    public static final String JOB_SUCCEEDED = "JobSucceededTimer";
  }

  public static class RunJobTimings {
    public static final String JOB_LOCAL_SETUP = "JobLocalSetupTimer";
    public static final String WORK_UNITS_RUN = "WorkUnitsRunTimer";
    public static final String WORK_UNITS_PREPARATION = "WorkUnitsPreparationTimer";
    public static final String MR_STAGING_DATA_CLEAN = "JobMrStagingDataCleanTimer";
    public static final String MR_DISTRIBUTED_CACHE_SETUP = "JobMrDistributedCacheSetupTimer";
    public static final String MR_JOB_SETUP = "JobMrSetupTimer";
    public static final String MR_JOB_RUN = "JobMrRunTimer";
    public static final String HELIX_JOB_SUBMISSION = "JobHelixSubmissionTimer";
    public static final String HELIX_JOB_RUN = "JobHelixRunTimer";
  }

  public static class FlowTimings {
    public static final String FLOW_COMPILED = "FlowCompiled";
    public static final String FLOW_COMPILE_FAILED = "FlowCompileFailed";
    public static final String FLOW_SUCCEEDED = "FlowSucceeded";
    public static final String FLOW_FAILED = "FlowFailed";
    public static final String FLOW_RUNNING = "FlowRunning";
  }

  public static class FlowEventConstants {
    public static final String FLOW_NAME_FIELD = "flowName";
    public static final String FLOW_GROUP_FIELD = "flowGroup";
    public static final String FLOW_EXECUTION_ID_FIELD = "flowExecutionId";
    public static final String JOB_NAME_FIELD = "jobName";
    public static final String JOB_GROUP_FIELD = "jobGroup";
    public static final String JOB_EXECUTION_ID_FIELD = "jobExecutionId";
    public static final String SPEC_EXECUTOR_FIELD = "specExecutor";
    public static final String LOW_WATERMARK_FIELD = "lowWatermark";
    public static final String HIGH_WATERMARK_FIELD = "highWatermark";
    public static final String PROCESSED_COUNT_FIELD = "processedCount";
    public static final String MAX_ATTEMPTS_FIELD = "maxAttempts";
    public static final String CURRENT_ATTEMPTS_FIELD = "currentAttempts";
    public static final String SHOULD_RETRY_FIELD = "shouldRetry";
  }

  public static final String METADATA_START_TIME = "startTime";
  public static final String METADATA_END_TIME = "endTime";
  public static final String METADATA_DURATION = "durationMillis";
  public static final String METADATA_TIMING_EVENT = "timingEvent";
  public static final String METADATA_MESSAGE = "message";
  public static final String JOB_START_TIME = "jobStartTime";
  public static final String JOB_END_TIME = "jobEndTime";

  @Getter
  private Long startTime;
  @Getter
  private Long endTime;
  @Getter
  private Long duration;
  private final EventSubmitter submitter;
  private boolean stopped;

  public TimingEvent(EventSubmitter submitter, String name) {
    super(name);
    this.stopped = false;
    this.submitter = submitter;
    this.startTime = System.currentTimeMillis();
  }

  /**
   * Stop the timer and submit the event. If the timer was already stopped before, this is a no-op.
   */
  public void stop() {
    stop(Maps.<String, String>newHashMap());
  }

  /**
   * Stop the timer and submit the event, along with the additional metadata specified. If the timer was already stopped
   * before, this is a no-op.
   *
   * @param additionalMetadata a {@link Map} of additional metadata that should be submitted along with this event
   * @deprecated Use {@link #close()}
   */
  @Deprecated
  public void stop(Map<String, String> additionalMetadata) {
    if (this.stopped) {
      return;
    }

    this.metadata.putAll(additionalMetadata);
    doStop();
    this.submitter.submit(name, this.metadata);
  }

  public void doStop() {
    if (this.stopped) {
      return;
    }

    this.stopped = true;
    this.endTime = System.currentTimeMillis();
    this.duration = this.endTime - this.startTime;

    this.metadata.put(EventSubmitter.EVENT_TYPE, METADATA_TIMING_EVENT);
    this.metadata.put(METADATA_START_TIME, Long.toString(this.startTime));
    this.metadata.put(METADATA_END_TIME, Long.toString(this.endTime));
    this.metadata.put(METADATA_DURATION, Long.toString(this.duration));
  }

  @Override
  public void close() {
    doStop();
    submitter.submit(this);
  }

  /**
   * Check if the given {@link GobblinTrackingEvent} is a {@link TimingEvent}
   */
  public static boolean isTimingEvent(GobblinTrackingEvent event) {
    String eventType = (event.getMetadata() == null) ? "" : event.getMetadata().get(EVENT_TYPE);
    return StringUtils.isNotEmpty(eventType) && eventType.equals(METADATA_TIMING_EVENT);
  }

  /**
   * Create a {@link TimingEvent} from a {@link GobblinTrackingEvent}. An inverse function
   * to {@link TimingEvent#build()}
   */
  public static TimingEvent fromEvent(GobblinTrackingEvent event) {
    if(!isTimingEvent(event)) {
      return null;
    }

    Map<String, String> metadata = event.getMetadata();
    TimingEvent timingEvent = new TimingEvent(null, event.getName());

    metadata.forEach((key, value) -> {
      switch (key) {
        case METADATA_START_TIME:
          timingEvent.startTime = Long.parseLong(value);
          break;
        case METADATA_END_TIME:
          timingEvent.endTime = Long.parseLong(value);
          break;
        case METADATA_DURATION:
          timingEvent.duration = Long.parseLong(value);
          break;
        default:
          timingEvent.addMetadata(key, value);
          break;
      }
    });

    return timingEvent;
  }
}
