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

package org.apache.gobblin.runtime.listeners;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.JobContext;
import org.apache.gobblin.runtime.JobState;


/**
 * An implementation of {@link JobListener} for run-once jobs.
 *
 * @author Yinan Li
 */
public class RunOnceJobListener extends AbstractJobListener {

  private static final Logger LOG = LoggerFactory.getLogger(RunOnceJobListener.class);

  @Override
  public void onJobCompletion(JobContext jobContext) {
    JobState jobState = jobContext.getJobState();
    if (!jobState.contains(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY)) {
      LOG.error("Job configuration file path not found in job state of job " + jobState.getJobId());
      return;
    }

    String jobConfigFile = jobState.getProp(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY);
    // Rename the config file so we won't run this job when the worker is bounced
    try {
      Files.move(new File(jobConfigFile), new File(jobConfigFile + ".done"));
    } catch (IOException ioe) {
      LOG.error("Failed to rename job configuration file for job " + jobState.getJobName(), ioe);
    }
  }
}
