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

package gobblin.runtime.local;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ServiceManager;

import gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.metrics.Tag;
import gobblin.metrics.event.TimingEvent;
import gobblin.runtime.AbstractJobLauncher;
import gobblin.runtime.GobblinMultiTaskAttempt;
import gobblin.runtime.JobState;
import gobblin.runtime.TaskExecutor;
import gobblin.runtime.TaskStateTracker;
import gobblin.runtime.api.Configurable;
import gobblin.runtime.api.JobSpec;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.JobConfigurationUtils;
import gobblin.util.JobLauncherUtils;


/**
 * An implementation of {@link gobblin.runtime.JobLauncher} for launching and running jobs
 * locally on a single node.
 *
 * @author Yinan Li
 */
public class LocalJobLauncher extends AbstractJobLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(LocalJobLauncher.class);

  private final TaskExecutor taskExecutor;

  private final TaskStateTracker taskStateTracker;

  // Service manager to manage dependent services
  private final ServiceManager serviceManager;

  private volatile CountDownLatch countDownLatch;

  public LocalJobLauncher(Properties jobProps) throws Exception {
    this(jobProps, null);
  }

  public LocalJobLauncher(Properties jobProps, SharedResourcesBroker<GobblinScopeTypes> instanceBroker) throws Exception {
    super(jobProps, ImmutableList.<Tag<?>> of(), instanceBroker);

    TimingEvent jobLocalSetupTimer = this.eventSubmitter.getTimingEvent(TimingEvent.RunJobTimings.JOB_LOCAL_SETUP);

    this.taskExecutor = new TaskExecutor(jobProps);
    this.taskStateTracker =
        new LocalTaskStateTracker(jobProps, this.jobContext.getJobState(), this.taskExecutor, this.eventBus);

    this.serviceManager = new ServiceManager(Lists.newArrayList(
        // The order matters due to dependencies between services
        this.taskExecutor, this.taskStateTracker));
    // Start all dependent services
    this.serviceManager.startAsync().awaitHealthy(5, TimeUnit.SECONDS);

    startCancellationExecutor();

    jobLocalSetupTimer.stop();
  }

  public LocalJobLauncher(Configurable instanceConf, JobSpec jobSpec) throws Exception {
    this(JobConfigurationUtils.combineSysAndJobProperties(instanceConf.getConfigAsProperties(),
                                                          jobSpec.getConfigAsProperties()));
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
        this.eventSubmitter.getTimingEvent(TimingEvent.RunJobTimings.WORK_UNITS_PREPARATION);
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

    TimingEvent workUnitsRunTimer = this.eventSubmitter.getTimingEvent(TimingEvent.RunJobTimings.WORK_UNITS_RUN);

    GobblinMultiTaskAttempt.runWorkUnits(this.jobContext, workUnitsToRun,
        this.taskStateTracker, this.taskExecutor, GobblinMultiTaskAttempt.CommitPolicy.IMMEDIATE);

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
