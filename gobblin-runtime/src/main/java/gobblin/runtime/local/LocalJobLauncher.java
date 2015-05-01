/* (c) 2014 LinkedIn Corp. All rights reserved.
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

import gobblin.runtime.AbstractJobLauncher;
import gobblin.runtime.FileBasedJobLock;
import gobblin.runtime.JobState;
import gobblin.runtime.Task;
import gobblin.runtime.TaskExecutor;
import gobblin.runtime.TaskStateTracker;
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
import gobblin.runtime.JobException;
import gobblin.runtime.JobLock;
import gobblin.source.workunit.MultiWorkUnit;
import gobblin.source.workunit.WorkUnit;


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
  private volatile boolean isCancelled = false;

  public LocalJobLauncher(Properties properties)
      throws Exception {
    super(properties);

    this.taskExecutor = new TaskExecutor(properties);
    this.taskStateTracker = new LocalTaskStateTracker2(properties, this.taskExecutor);
    this.serviceManager = new ServiceManager(Lists.newArrayList(
        // The order matters due to dependencies between services
        this.taskExecutor, this.taskStateTracker));
  }

  @Override
  public void cancelJob(Properties jobProps)
      throws JobException {
    if (!this.isCancelled && this.countDownLatch != null) {
      // Unblock the thread that calls runJob below
      while (this.countDownLatch.getCount() > 0) {
        this.countDownLatch.countDown();
      }
      this.isCancelled = true;
      LOG.info("Cancelled job " + jobProps.getProperty(ConfigurationKeys.JOB_ID_KEY));
    }
  }

  @Override
  public void close()
      throws IOException {
    try {
      // Stop all dependent services
      this.serviceManager.stopAsync().awaitStopped(5, TimeUnit.SECONDS);
    } catch (TimeoutException te) {
      LOG.warn("Timed out while waiting for the service manager to be stopped", te);
    }
  }

  @Override
  protected void runJob(String jobName, Properties jobProps, JobState jobState, List<WorkUnit> workUnits)
      throws Exception {

    // Start all dependent services
    this.serviceManager.startAsync().awaitHealthy(5, TimeUnit.SECONDS);

    // Figure out the actual work units to run by flattening MultiWorkUnits
    List<WorkUnit> workUnitsToRun = Lists.newArrayList();
    for (WorkUnit workUnit : workUnits) {
      if (workUnit instanceof MultiWorkUnit) {
        workUnitsToRun.addAll(((MultiWorkUnit) workUnit).getWorkUnits());
      } else {
        workUnitsToRun.add(workUnit);
      }
    }

    if (workUnitsToRun.isEmpty()) {
      LOG.warn("No work units to run");
      return;
    }

    String jobId = jobProps.getProperty(ConfigurationKeys.JOB_ID_KEY);
    this.countDownLatch = new CountDownLatch(workUnitsToRun.size());
    List<Task> tasks = AbstractJobLauncher
        .runWorkUnits(jobId, workUnitsToRun, this.taskStateTracker, this.taskExecutor, this.countDownLatch);

    // Set job state appropriately
    if (isCancelled) {
      jobState.setState(JobState.RunningState.CANCELLED);
    } else if (jobState.getState() == JobState.RunningState.RUNNING) {
      jobState.setState(JobState.RunningState.SUCCESSFUL);
    }

    // Collect task states and set job state to FAILED if any task failed
    for (Task task : tasks) {
      jobState.addTaskState(task.getTaskState());
      if (task.getTaskState().getWorkingState() == WorkUnitState.WorkingState.FAILED) {
        jobState.setState(JobState.RunningState.FAILED);
      }
    }
  }

  @Override
  protected JobLock getJobLock(String jobName, Properties jobProps)
      throws IOException {
    URI fsUri = URI.create(jobProps.getProperty(ConfigurationKeys.FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI));
    return new FileBasedJobLock(FileSystem.get(fsUri, new Configuration()),
        jobProps.getProperty(ConfigurationKeys.JOB_LOCK_DIR_KEY), jobName);
  }
}
