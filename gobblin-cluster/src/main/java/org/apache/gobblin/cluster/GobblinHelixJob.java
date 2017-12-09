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

package org.apache.gobblin.cluster;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixManager;

import org.quartz.InterruptableJob;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.runtime.JobLauncher;
import org.apache.gobblin.runtime.listeners.JobListener;
import org.apache.gobblin.scheduler.BaseGobblinJob;
import org.apache.gobblin.scheduler.JobScheduler;
import org.quartz.UnableToInterruptJobException;


/**
 * An implementation of a Quartz's {@link Job} that uses a {@link GobblinHelixJobLauncher}
 * to launch a Gobblin job.
 *
 * @author Yinan Li
 */
@Alpha
@Slf4j
public class GobblinHelixJob extends BaseGobblinJob implements InterruptableJob {
  private Future cancellable = null;

  @Override
  public void executeImpl(JobExecutionContext context) throws JobExecutionException {
    JobDataMap dataMap = context.getJobDetail().getJobDataMap();
    ConcurrentHashMap runningMap = (ConcurrentHashMap)dataMap.get(GobblinHelixJobScheduler.JOB_RUNNING_MAP);
    final JobScheduler jobScheduler = (JobScheduler) dataMap.get(JobScheduler.JOB_SCHEDULER_KEY);
    // the properties may get mutated during job execution and the scheduler reuses it for the next round of scheduling,
    // so clone it
    final Properties jobProps = (Properties)((Properties) dataMap.get(JobScheduler.PROPERTIES_KEY)).clone();
    final JobListener jobListener = (JobListener) dataMap.get(JobScheduler.JOB_LISTENER_KEY);
    HelixManager helixManager = (HelixManager) dataMap.get(GobblinHelixJobScheduler.HELIX_MANAGER_KEY);
    Path appWorkDir = (Path) dataMap.get(GobblinHelixJobScheduler.APPLICATION_WORK_DIR_KEY);
    @SuppressWarnings("unchecked")
    List<? extends Tag<?>> eventMetadata = (List<? extends Tag<?>>) dataMap.get(GobblinHelixJobScheduler.METADATA_TAGS);

    try {
      final JobLauncher jobLauncher = new GobblinHelixJobLauncher(jobProps, helixManager, appWorkDir, eventMetadata, runningMap);
      if (Boolean.valueOf(jobProps.getProperty(GobblinClusterConfigurationKeys.JOB_EXECUTE_IN_SCHEDULING_THREAD,
              Boolean.toString(GobblinClusterConfigurationKeys.JOB_EXECUTE_IN_SCHEDULING_THREAD_DEFAULT)))) {
        jobScheduler.runJob(jobProps, jobListener, jobLauncher);
      } else {
        cancellable = jobScheduler.scheduleJobImmediately(jobProps, jobListener, jobLauncher);
      }
    } catch (Throwable t) {
      throw new JobExecutionException(t);
    }
  }

  @Override
  public void interrupt() throws UnableToInterruptJobException {
    if (cancellable != null) {
      try {
        if (cancellable.cancel(false)) {
          return;
        }
      } catch (Exception e) {
        log.error("Failed to gracefully cancel job. Attempting to force cancellation.", e);
      }
      try {
        cancellable.cancel(true);
      } catch (Exception e) {
        throw new UnableToInterruptJobException(e);
      }
    }
  }
}
