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

package org.apache.gobblin.runtime.mapreduce;

import com.google.common.collect.Maps;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.task.BaseAbstractTask;
import java.io.IOException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;


/**
 * A task that runs an MR job.
 *
 * Usage:
 * TaskUtils.setTaskFactoryClass(workUnit, MRTaskFactory.class);
 * MRTask.serializeJobToState(workUnit, myJob);
 *
 * Subclasses can override {@link #createJob()} to customize the way the MR job is prepared.
 */
@Slf4j
public class MRTask extends BaseAbstractTask {

  private static final String JOB_CONFIGURATION_PREFIX = "MRTask.jobConfiguration.";

  public static class Events {
    public static final String MR_JOB_STARTED_EVENT = "MRJobStarted";
    public static final String MR_JOB_SUCCESSFUL = "MRJobSuccessful";
    public static final String MR_JOB_FAILED = "MRJobFailed";
    public static final String MR_JOB_SKIPPED = "MRJobSkipped";

    public static final String JOB_URL = "jobTrackingUrl";
    public static final String FAILURE_CONTEXT = "failureContext";
  }

  public static void serializeJobToState(State state, Job job) {
    for (Map.Entry<String, String> entry : job.getConfiguration()) {
      state.setProp(JOB_CONFIGURATION_PREFIX + entry.getKey(), entry.getValue());
    }
  }

  protected final TaskContext taskContext;
  private final EventSubmitter eventSubmitter;
  protected Job mrJob;

  public MRTask(TaskContext taskContext) {
    super(taskContext);
    this.taskContext = taskContext;
    this.eventSubmitter = new EventSubmitter.Builder(this.metricContext, "gobblin.MRTask")
        .addMetadata(additionalEventMetadata()).build();
  }

  public void onMRTaskComplete (boolean isSuccess, Throwable t) throws RuntimeException {
    if (isSuccess) {
      this.workingState = WorkUnitState.WorkingState.SUCCESSFUL;
    } else if (t == null) {
      this.workingState = WorkUnitState.WorkingState.FAILED;
    } else {
      log.error ("Failed to run MR job with exception {}", ExceptionUtils.getStackTrace(t));
      this.workingState = WorkUnitState.WorkingState.FAILED;
    }
  }

  @Override
  public void commit() {
    log.debug ("State is set to {} inside onMRTaskComplete.", this.workingState);
  }

  @Override
  public void run() {

    try {
      Job job = createJob();

      if (job == null) {
        log.info("No MR job created. Skipping.");
        this.workingState = WorkUnitState.WorkingState.SUCCESSFUL;
        this.eventSubmitter.submit(Events.MR_JOB_SKIPPED);
        onSkippedMRJob();
        return;
      }

      job.submit();

      log.info("MR tracking URL {} for job {}", job.getTrackingURL(), job.getJobName());

      this.eventSubmitter.submit(Events.MR_JOB_STARTED_EVENT, Events.JOB_URL, job.getTrackingURL());
      job.waitForCompletion(true);
      this.mrJob = job;

      if (job.isSuccessful()) {
        this.eventSubmitter.submit(Events.MR_JOB_SUCCESSFUL, Events.JOB_URL, job.getTrackingURL());
        this.onMRTaskComplete(true, null);
      } else {
        this.eventSubmitter.submit(Events.MR_JOB_FAILED, Events.JOB_URL, job.getTrackingURL());
        JobStatus jobStatus = job.getStatus();
        this.onMRTaskComplete (false,
            new IOException(String.format("MR Job:%s is not successful, failure info: %s",
                job.getTrackingURL(), jobStatus == null ? "Job status not available to inspect for this failing instance."
                    : job.getStatus().getFailureInfo())));
      }
    } catch (Throwable t) {
      log.error("Failed to run MR job.", t);
      this.eventSubmitter.submit(Events.MR_JOB_FAILED, Events.FAILURE_CONTEXT, t.getMessage());
      this.onMRTaskComplete (false, t);
    }
  }

  protected Map<String, String> additionalEventMetadata() {
    return Maps.newHashMap();
  }

  /**
   * Create the {@link Job} to run in this task.
   * @return the {@link Job} to run. If this method returns null, no job will be run and the task will be marked as successful.
   */
  protected Job createJob() throws IOException {
    Job job = Job.getInstance(new Configuration());
    for (Map.Entry<Object, Object> entry : this.taskContext.getTaskState().getProperties().entrySet()) {
      if (entry.getKey() instanceof String && ((String) entry.getKey()).startsWith(JOB_CONFIGURATION_PREFIX)) {
        String actualKey = ((String) entry.getKey()).substring(JOB_CONFIGURATION_PREFIX.length());
        job.getConfiguration().set(actualKey, (String) entry.getValue());
      }
    }
    return job;
  }

  /**
   * Called when a job is skipped (because {@link #createJob()} returned null).
   */
  protected void onSkippedMRJob() {
    // do nothing
  }

}
