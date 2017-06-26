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

package gobblin.cluster;

import java.util.List;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixManager;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import gobblin.annotation.Alpha;
import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.Tag;
import gobblin.runtime.JobException;
import gobblin.runtime.JobLauncher;
import gobblin.runtime.listeners.JobListener;
import gobblin.scheduler.BaseGobblinJob;
import gobblin.scheduler.JobScheduler;


/**
 * An implementation of a Quartz's {@link Job} that uses a {@link GobblinHelixJobLauncher}
 * to launch a Gobblin job.
 *
 * @author Yinan Li
 */
@Alpha
@Slf4j
public class GobblinHelixJob extends BaseGobblinJob {
  @Override
  public void executeImpl(JobExecutionContext context) throws JobExecutionException {
    JobDataMap dataMap = context.getJobDetail().getJobDataMap();

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
      final JobLauncher jobLauncher = new GobblinHelixJobLauncher(jobProps, helixManager, appWorkDir, eventMetadata);

      if (Boolean.valueOf(jobProps.getProperty(GobblinClusterConfigurationKeys.JOB_EXECUTE_IN_SCHEDULING_THREAD,
          Boolean.toString(GobblinClusterConfigurationKeys.JOB_EXECUTE_IN_SCHEDULING_THREAD_DEFAULT)))) {
        jobScheduler.runJob(jobProps, jobListener, jobLauncher);
      } else {
        // if not executing in the scheduling thread then submit a runnable to the job scheduler's ExecutorService
        // for asynchronous execution.
        Runnable runnable = new Runnable() {
          @Override
          public void run() {
            try {
              jobScheduler.runJob(jobProps, jobListener, jobLauncher);
            } catch (JobException je) {
              log.error("Failed to run job " + jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), je);
            }
          }
        };

        jobScheduler.submitRunnableToExecutor(runnable);
      }
    } catch (Throwable t) {
      throw new JobExecutionException(t);
    }
  }
}
