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

package org.apache.gobblin.runtime;

import static org.apache.gobblin.metrics.event.JobEvent.JOB_STATE;
import static org.apache.gobblin.metrics.event.JobEvent.METADATA_JOB_ID;
import static org.apache.gobblin.metrics.event.JobEvent.METADATA_JOB_NAME;
import static org.apache.gobblin.metrics.event.JobEvent.METADATA_JOB_START_TIME;
import static org.apache.gobblin.metrics.event.JobEvent.METADATA_JOB_END_TIME;
import static org.apache.gobblin.metrics.event.JobEvent.METADATA_JOB_STATE;
import static org.apache.gobblin.metrics.event.JobEvent.METADATA_JOB_LAUNCHED_TASKS;
import static org.apache.gobblin.metrics.event.JobEvent.METADATA_JOB_COMPLETED_TASKS;
import static org.apache.gobblin.metrics.event.JobEvent.METADATA_JOB_LAUNCHER_TYPE;
import static org.apache.gobblin.metrics.event.JobEvent.METADATA_JOB_TRACKING_URL;
import static org.apache.gobblin.metrics.event.TaskEvent.*;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.gobblin.metrics.event.EventSubmitter;

import lombok.AllArgsConstructor;


@AllArgsConstructor

/**
 * Submits metadata about a completed {@link JobState} using the provided {@link EventSubmitter}.
 */
public class JobExecutionEventSubmitter {

  private final EventSubmitter eventSubmitter;

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
    ImmutableMap.Builder<String, String> jobMetadataBuilder = new ImmutableMap.Builder<>();

    jobMetadataBuilder.put(METADATA_JOB_ID, jobState.getJobId());
    jobMetadataBuilder.put(METADATA_JOB_NAME, jobState.getJobName());
    jobMetadataBuilder.put(METADATA_JOB_START_TIME, Long.toString(jobState.getStartTime()));
    jobMetadataBuilder.put(METADATA_JOB_END_TIME, Long.toString(jobState.getEndTime()));
    jobMetadataBuilder.put(METADATA_JOB_STATE, jobState.getState().toString());
    jobMetadataBuilder.put(METADATA_JOB_LAUNCHED_TASKS, Integer.toString(jobState.getTaskCount()));
    jobMetadataBuilder.put(METADATA_JOB_COMPLETED_TASKS, Integer.toString(jobState.getCompletedTasks()));
    jobMetadataBuilder.put(METADATA_JOB_LAUNCHER_TYPE, jobState.getLauncherType().toString());
    jobMetadataBuilder.put(METADATA_JOB_TRACKING_URL, jobState.getTrackingURL().or(UNKNOWN_VALUE));
    jobMetadataBuilder.put(EventSubmitter.EVENT_TYPE, JOB_STATE);

    this.eventSubmitter.submit(JOB_STATE, jobMetadataBuilder.build());
  }

  /**
   * Submits an event for each {@link TaskState} in the given {@link JobState}.
   */
  private void submitTaskStateEvents(JobState jobState) {
    // Build Job Metadata applicable for TaskStates
    ImmutableMap.Builder<String, String> jobMetadataBuilder = new ImmutableMap.Builder<>();
    jobMetadataBuilder.put(METADATA_JOB_ID, jobState.getJobId());
    jobMetadataBuilder.put(METADATA_JOB_NAME, jobState.getJobName());
    jobMetadataBuilder.put(METADATA_JOB_TRACKING_URL, jobState.getTrackingURL().or(UNKNOWN_VALUE));
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
    ImmutableMap.Builder<String, String> taskMetadataBuilder = new ImmutableMap.Builder<>();

    taskMetadataBuilder.putAll(jobMetadata);
    taskMetadataBuilder.put(METADATA_TASK_ID, taskState.getTaskId());
    taskMetadataBuilder.put(METADATA_TASK_START_TIME, Long.toString(taskState.getStartTime()));
    taskMetadataBuilder.put(METADATA_TASK_END_TIME, Long.toString(taskState.getEndTime()));
    taskMetadataBuilder.put(METADATA_TASK_WORKING_STATE, taskState.getWorkingState().toString());
    taskMetadataBuilder.put(METADATA_TASK_FAILURE_CONTEXT, taskState.getTaskFailureException().or(UNKNOWN_VALUE));
    taskMetadataBuilder.put(EventSubmitter.EVENT_TYPE, TASK_STATE);

    this.eventSubmitter.submit(TASK_STATE, taskMetadataBuilder.build());
  }
}
