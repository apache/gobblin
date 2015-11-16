package gobblin.runtime;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import gobblin.metrics.event.EventSubmitter;

import lombok.AllArgsConstructor;


@AllArgsConstructor

/**
 * Submits metadata about a completed {@link JobState} using the provided {@link EventSubmitter}.
 */
public class JobExecutionEventSubmitter {

  private final EventSubmitter eventSubmitter;

  // Event names
  private static final String JOB_STATE_EVENT = "JobStateEvent";
  private static final String TASK_STATE_EVENT = "TaskStateEvent";

  // Job Event metadata keys
  private static final String JOB_ID = "jobId";
  private static final String JOB_NAME = "jobName";
  private static final String JOB_START_TIME = "jobBeginTime";
  private static final String JOB_END_TIME = "jobEndTime";
  private static final String JOB_STATE = "jobState";
  private static final String JOB_LAUNCHED_TASKS = "jobLaunchedTasks";
  private static final String JOB_COMPLETED_TASKS = "jobCompletedTasks";
  private static final String JOB_LAUNCHER_TYPE = "jobLauncherType";
  private static final String JOB_TRACKING_URL = "jobTrackingURL";

  // Task Event metadata keys
  private static final String TASK_ID = "taskId";
  private static final String TASK_START_TIME = "taskStartTime";
  private static final String TASK_END_TIME = "taskEndTime";
  private static final String TASK_WORKING_STATE = "taskWorkingState";
  private static final String TASK_FAILURE_CONTEXT = "taskFailureContext";

  // The value of any metadata key that cannot be determined
  private static final String UNKNOWN_VALUE = "UNKNOWN";

  /**
   * Submits metadata about a given {@link JobState} and each of its {@link TaskState}s. This method will submit a
   * single event for the {@link JobState} called {@link #JOB_STATE_EVENT}. It will submit an event for each
   * {@link TaskState} called {@link #TASK_STATE_EVENT}.
   *
   * @param jobState is the {@link JobState} to emit events for
   */
  public void submitJobExecutionEvents(JobState jobState) {
    submitJobStateEvent(jobState);
    submitTaskStateEvents(jobState);
  }

  /**
   * Submits an event for the given {@link JobState}.
   */
  private void submitJobStateEvent(JobState jobState) {
    ImmutableMap.Builder<String, String> jobMetadataBuilder = new ImmutableMap.Builder<String, String>();

    jobMetadataBuilder.put(JOB_ID, jobState.getJobId());
    jobMetadataBuilder.put(JOB_NAME, jobState.getJobName());
    jobMetadataBuilder.put(JOB_START_TIME, Long.toString(jobState.getStartTime()));
    jobMetadataBuilder.put(JOB_END_TIME, Long.toString(jobState.getEndTime()));
    jobMetadataBuilder.put(JOB_STATE, jobState.getState().toString());
    jobMetadataBuilder.put(JOB_LAUNCHED_TASKS, Integer.toString(jobState.getTaskCount()));
    jobMetadataBuilder.put(JOB_COMPLETED_TASKS, Integer.toString(jobState.getCompletedTasks()));
    jobMetadataBuilder.put(JOB_LAUNCHER_TYPE, jobState.getLauncherType().toString());
    jobMetadataBuilder.put(JOB_TRACKING_URL, jobState.getTrackingURL().or(UNKNOWN_VALUE));

    this.eventSubmitter.submit(JOB_STATE_EVENT, jobMetadataBuilder.build());
  }

  /**
   * Submits an event for each {@link TaskState} in the given {@link JobState}.
   */
  private void submitTaskStateEvents(JobState jobState) {
    // Build Job Metadata applicable for TaskStates
    ImmutableMap.Builder<String, String> jobMetadataBuilder = new ImmutableMap.Builder<String, String>();
    jobMetadataBuilder.put(JOB_ID, jobState.getJobId());
    jobMetadataBuilder.put(JOB_NAME, jobState.getJobName());
    Map<String, String> jobMetadata = jobMetadataBuilder.build();

    // Submit event for each TaskState
    for (TaskState taskState : jobState.getTaskStates()) {
      submitTaskStateEvent(taskState, jobMetadata);
    }
  }

  /**
   * Submits an event for a given {@link TaskState}. It will include all metadata specified in the jobMetadata parameter.
   */
  private void submitTaskStateEvent(TaskState taskState, Map<String, String> jobMetadata) {
    ImmutableMap.Builder<String, String> taskMetadataBuilder = new ImmutableMap.Builder<String, String>();

    taskMetadataBuilder.putAll(jobMetadata);
    taskMetadataBuilder.put(TASK_ID, taskState.getTaskId());
    taskMetadataBuilder.put(TASK_START_TIME, Long.toString(taskState.getStartTime()));
    taskMetadataBuilder.put(TASK_END_TIME, Long.toString(taskState.getEndTime()));
    taskMetadataBuilder.put(TASK_WORKING_STATE, taskState.getWorkingState().toString());
    taskMetadataBuilder.put(TASK_FAILURE_CONTEXT, taskState.getTaskFailureException().or(UNKNOWN_VALUE));

    this.eventSubmitter.submit(TASK_STATE_EVENT, taskMetadataBuilder.build());
  }
}
