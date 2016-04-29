/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.yarn;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixManager;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import gobblin.metrics.Tag;
import gobblin.runtime.JobLauncher;
import gobblin.runtime.listeners.JobListener;
import gobblin.scheduler.JobScheduler;


/**
 * An implementation of a Quartz's {@link Job} that uses a {@link GobblinHelixJobLauncher}
 * to launch a Gobblin job.
 *
 * @author Yinan Li
 */
public class GobblinHelixJob implements Job {

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    JobDataMap dataMap = context.getJobDetail().getJobDataMap();

    JobScheduler jobScheduler = (JobScheduler) dataMap.get(JobScheduler.JOB_SCHEDULER_KEY);
    Properties jobProps = (Properties) dataMap.get(JobScheduler.PROPERTIES_KEY);
    JobListener jobListener = (JobListener) dataMap.get(JobScheduler.JOB_LISTENER_KEY);
    HelixManager helixManager = (HelixManager) dataMap.get(GobblinHelixJobScheduler.HELIX_MANAGER_KEY);
    Path appWorkDir = (Path) dataMap.get(GobblinHelixJobScheduler.APPLICATION_WORK_DIR_KEY);
    @SuppressWarnings("unchecked")
    List<? extends Tag<?>> eventMetadata = (List<? extends Tag<?>>) dataMap.get(GobblinHelixJobScheduler.METADATA_TAGS);

    try {
      JobLauncher jobLauncher = new GobblinHelixJobLauncher(jobProps, helixManager, appWorkDir, eventMetadata);
      jobScheduler.runJob(jobProps, jobListener, jobLauncher);
    } catch (Throwable t) {
      throw new JobExecutionException(t);
    }
  }
}
