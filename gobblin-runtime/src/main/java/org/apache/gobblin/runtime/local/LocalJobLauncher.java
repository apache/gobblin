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

package org.apache.gobblin.runtime.local;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ServiceManager;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.AbstractJobLauncher;
import org.apache.gobblin.runtime.GobblinMultiTaskAttempt;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskExecutor;
import org.apache.gobblin.runtime.TaskStateTracker;
import org.apache.gobblin.runtime.api.Configurable;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.JobConfigurationUtils;
import org.apache.gobblin.runtime.util.MultiWorkUnitUnpackingIterator;
import org.apache.gobblin.source.workunit.WorkUnitStream;

/**
 * An implementation of {@link org.apache.gobblin.runtime.JobLauncher} for launching and running jobs
 * locally on a single node.
 *
 * @author Yinan Li
 */
@Slf4j
public class LocalJobLauncher extends AbstractJobLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(LocalJobLauncher.class);

  private final TaskExecutor taskExecutor;

  private final TaskStateTracker taskStateTracker;

  // Service manager to manage dependent services
  private final ServiceManager serviceManager;

  public LocalJobLauncher(Properties jobProps) throws Exception {
    this(jobProps, null);
  }

  public LocalJobLauncher(Properties jobProps, SharedResourcesBroker<GobblinScopeTypes> instanceBroker) throws Exception {
    super(jobProps, ImmutableList.<Tag<?>> of(), instanceBroker);
    log.debug("Local job launched with properties: {}", jobProps);

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
    // This should never happen
    throw new UnsupportedOperationException();
  }

  @Override
  protected void runWorkUnitStream(WorkUnitStream workUnitStream) throws Exception {
    String jobId = this.jobContext.getJobId();
    final JobState jobState = this.jobContext.getJobState();

    Iterator<WorkUnit> workUnitIterator = workUnitStream.getWorkUnits();
    if (!workUnitIterator.hasNext()) {
      LOG.warn("No work units to run");
      return;
    }


    TimingEvent workUnitsRunTimer = this.eventSubmitter.getTimingEvent(TimingEvent.RunJobTimings.WORK_UNITS_RUN);
    Iterator<WorkUnit> flattenedWorkUnits = new MultiWorkUnitUnpackingIterator(workUnitStream.getWorkUnits());
    Iterator<WorkUnit> workUnitsWithJobState = Iterators.transform(flattenedWorkUnits, new Function<WorkUnit, WorkUnit>() {
      @Override
      public WorkUnit apply(WorkUnit workUnit) {
        workUnit.addAllIfNotExist(jobState);
        return workUnit;
      }
    });

    GobblinMultiTaskAttempt.runWorkUnits(this.jobContext, workUnitsWithJobState, this.taskStateTracker,
        this.taskExecutor, GobblinMultiTaskAttempt.CommitPolicy.IMMEDIATE);

    if (this.cancellationRequested) {
      // Wait for the cancellation execution if it has been requested
      synchronized (this.cancellationExecution) {
        if (this.cancellationExecuted) {
          return;
        }
      }
    }
    workUnitsRunTimer.stop();

    LOG.info(String.format("All tasks of job %s have completed", jobId));

    if (jobState.getState() == JobState.RunningState.RUNNING) {
      jobState.setState(JobState.RunningState.SUCCESSFUL);
    }
  }

  @Override
  protected void executeCancellation() {

  }
}
