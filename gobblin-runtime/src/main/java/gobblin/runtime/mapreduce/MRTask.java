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

package gobblin.runtime.mapreduce;

import com.google.common.collect.Maps;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.metrics.event.EventSubmitter;
import gobblin.runtime.TaskContext;
import gobblin.runtime.task.BaseAbstractTask;
import java.io.IOException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;


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

    public static final String JOB_URL = "jobTrackingUrl";
    public static final String FAILURE_CONTEXT = "failureContext";
  }

  public static void serializeJobToState(State state, Job job) {
    for (Map.Entry<String, String> entry : job.getConfiguration()) {
      state.setProp(JOB_CONFIGURATION_PREFIX + entry.getKey(), entry.getValue());
    }
  }

  private final TaskContext taskContext;
  private final EventSubmitter eventSubmitter;

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

      job.submit();
      this.eventSubmitter.submit(Events.MR_JOB_STARTED_EVENT, Events.JOB_URL, job.getTrackingURL());
      job.waitForCompletion(false);

      if (job.isSuccessful()) {
        this.eventSubmitter.submit(Events.MR_JOB_SUCCESSFUL, Events.JOB_URL, job.getTrackingURL());
        this.onMRTaskComplete(true, null);
      } else {
        this.eventSubmitter.submit(Events.MR_JOB_FAILED, Events.JOB_URL, job.getTrackingURL());
        this.onMRTaskComplete (false, new IOException("MR Job is not successful"));
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

}
