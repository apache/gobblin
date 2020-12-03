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

import java.util.Properties;
import java.util.concurrent.Future;

import org.quartz.InterruptableJob;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.listeners.JobListener;
import org.apache.gobblin.scheduler.BaseGobblinJob;
import org.apache.gobblin.scheduler.JobScheduler;


/**
 * An implementation of a Quartz's {@link Job} that uses a {@link GobblinHelixJobLauncher}
 * to launch a Gobblin job.
 *
 * @author Yinan Li
 */
@Alpha
@Slf4j
public class GobblinHelixJob extends BaseGobblinJob implements InterruptableJob {
  private Future<?> cancellable = null;

  @Override
  public void executeImpl(JobExecutionContext context) throws JobExecutionException {
    JobDataMap dataMap = context.getJobDetail().getJobDataMap();
    final JobScheduler jobScheduler = (JobScheduler) dataMap.get(JobScheduler.JOB_SCHEDULER_KEY);
    // the properties may get mutated during job execution and the scheduler reuses it for the next round of scheduling,
    // so clone it
    final Properties jobProps = (Properties)((Properties) dataMap.get(JobScheduler.PROPERTIES_KEY)).clone();
    final JobListener jobListener = (JobListener) dataMap.get(JobScheduler.JOB_LISTENER_KEY);

    try {
      if (Boolean.parseBoolean(jobProps.getProperty(GobblinClusterConfigurationKeys.JOB_EXECUTE_IN_SCHEDULING_THREAD,
              Boolean.toString(GobblinClusterConfigurationKeys.JOB_EXECUTE_IN_SCHEDULING_THREAD_DEFAULT)))) {
        jobScheduler.runJob(jobProps, jobListener);
      } else {
        cancellable = jobScheduler.scheduleJobImmediately(jobProps, jobListener);
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
