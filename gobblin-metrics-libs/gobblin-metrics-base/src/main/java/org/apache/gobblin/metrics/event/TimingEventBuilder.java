package org.apache.gobblin.metrics.event;

import org.apache.gobblin.metrics.GobblinTrackingEvent;


/**
 * A Builder that builds and can submit a {@link GobblinTrackingEvent }
 *
 */
public class TimingEventBuilder extends GobblinEventBuilder {

  public static class LauncherTimings {
    public static final String FULL_JOB_EXECUTION = "FullJobExecutionTimer";
    public static final String WORK_UNITS_CREATION = "WorkUnitsCreationTimer";
    public static final String WORK_UNITS_PREPARATION = "WorkUnitsPreparationTimer";
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

  private final long startTime;
  private boolean stopped;

  public TimingEventBuilder(String name) {
    super(name);
    this.stopped = false;
    this.startTime = System.currentTimeMillis();
  }

  /**
   * Stops the timer if it hasn't already been stopped
   * and records the duration of the event in metadata
   */
  public void stop() {
    if (this.stopped) {
      return;
    }
    this.stopped = true;
    long endTime = System.currentTimeMillis();

    metadata.put(EVENT_TYPE, METADATA_TIMING_EVENT);
    metadata.put(METADATA_START_TIME, Long.toString(this.startTime));
    metadata.put(METADATA_END_TIME, Long.toString(endTime));
    metadata.put(METADATA_DURATION, Long.toString((endTime - this.startTime)));
  }

  /**
   * stops the timer and returns a {@link GobblinTrackingEvent}
   * @return {@link GobblinTrackingEvent}
   */
  @Override
  public GobblinTrackingEvent build() {
    stop();
    return super.build();
  }
}
