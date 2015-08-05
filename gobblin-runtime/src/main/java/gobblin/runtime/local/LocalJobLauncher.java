/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime.local;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ServiceManager;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.metrics.event.TimingEvent;
import gobblin.runtime.AbstractJobLauncher;
import gobblin.runtime.FileBasedJobLock;
import gobblin.runtime.JobLock;
import gobblin.runtime.JobState;
import gobblin.runtime.Task;
import gobblin.runtime.TaskExecutor;
import gobblin.runtime.TaskStateTracker;
import gobblin.runtime.util.TimingEventNames;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.JobLauncherUtils;


/**
 * An implementation of {@link gobblin.runtime.JobLauncher} for launching and running jobs
 * locally on a single node.
 *
 * @author ynli
 */
public class LocalJobLauncher extends AbstractJobLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(LocalJobLauncher.class);

  private final TaskExecutor taskExecutor;
  private final TaskStateTracker taskStateTracker;
  // Service manager to manage dependent services
  private final ServiceManager serviceManager;

  private volatile CountDownLatch countDownLatch;

  public LocalJobLauncher(Properties jobProps) throws Exception {
    super(jobProps);

    TimingEvent jobLocalSetupTimer = this.eventSubmitter.getTimingEvent(TimingEventNames.RunJobTimings.JOB_LOCAL_SETUP);

    this.taskExecutor = new TaskExecutor(jobProps);
    this.taskStateTracker = new LocalTaskStateTracker2(jobProps, this.taskExecutor);

    this.serviceManager = new ServiceManager(Lists.newArrayList(
        // The order matters due to dependencies between services
        this.taskExecutor, this.taskStateTracker));
    // Start all dependent services
    this.serviceManager.startAsync().awaitHealthy(5, TimeUnit.SECONDS);

    startCancellationExecutor();

    jobLocalSetupTimer.stop();
  }

  @Override
  public void close() throws IOException {
    try {
      // Stop all dependent services
      this.serviceManager.stopAsync().awaitStopped(5, TimeUnit.SECONDS);
    } catch (TimeoutException te) {
      LOG.warn("Timed out while waiting for the service manager to be stopped", te);
    } finally {
      super.close();
    }
  }

  @Override
  protected void runWorkUnits(List<WorkUnit> workUnits) throws Exception {
    TimingEvent workUnitsPreparationTimer =
        this.eventSubmitter.getTimingEvent(TimingEventNames.RunJobTimings.WORK_UNITS_PREPARATION);
    List<WorkUnit> workUnitsToRun = JobLauncherUtils.flattenWorkUnits(workUnits);
    workUnitsPreparationTimer.stop();

    if (workUnitsToRun.isEmpty()) {
      LOG.warn("No work units to run");
      return;
    }

    String jobId = this.jobContext.getJobId();
    JobState jobState = this.jobContext.getJobState();

    for (WorkUnit workUnit : workUnitsToRun) {
      workUnit.addAllIfNotExist(jobState);
    }

    TimingEvent workUnitsRunTimer = this.eventSubmitter.getTimingEvent(TimingEventNames.RunJobTimings.WORK_UNITS_RUN);

    this.countDownLatch = new CountDownLatch(workUnitsToRun.size());
    List<Task> tasks = AbstractJobLauncher.submitWorkUnits(this.jobContext.getJobId(), workUnitsToRun,
        this.taskStateTracker, this.taskExecutor, this.countDownLatch);

    LOG.info(String.format("Waiting for submitted tasks of job %s to complete...", jobId));
    while (this.countDownLatch.getCount() > 0) {
      LOG.info(String.format("%d out of %d tasks of job %s are running", this.countDownLatch.getCount(),
          workUnitsToRun.size(), jobId));
      this.countDownLatch.await(1, TimeUnit.MINUTES);
    }

    workUnitsRunTimer.stop();

    if (this.cancellationRequested) {
      // Wait for the cancellation execution if it has been requested
      synchronized (this.cancellationExecution) {
        if (this.cancellationExecuted) {
          return;
        }
      }
    }

    LOG.info(String.format("All tasks of job %s have completed", jobId));

    if (jobState.getState() == JobState.RunningState.RUNNING) {
      jobState.setState(JobState.RunningState.SUCCESSFUL);
    }

    // Collect task states and set job state to FAILED if any task failed
    for (Task task : tasks) {
      jobState.addTaskState(task.getTaskState());
      if (task.getTaskState().getWorkingState() == WorkUnitState.WorkingState.FAILED) {
        this.eventSubmitter.submit(gobblin.metrics.event.EventNames.TASK_FAILED, "taskId", task.getTaskId());
        jobState.setState(JobState.RunningState.FAILED);
      }
    }
  }

  @Override
  protected JobLock getJobLock() throws IOException {
    URI fsUri = URI.create(this.jobProps.getProperty(ConfigurationKeys.FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI));
    return new FileBasedJobLock(FileSystem.get(fsUri, new Configuration()),
        this.jobProps.getProperty(ConfigurationKeys.JOB_LOCK_DIR_KEY), this.jobContext.getJobName());
  }

  @Override
  protected void executeCancellation() {
    if (this.countDownLatch != null) {
      while (this.countDownLatch.getCount() > 0) {
        this.countDownLatch.countDown();
      }
    }
  }
}
