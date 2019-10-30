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

package org.apache.gobblin.scheduler;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.gobblin.runtime.job_spec.JobSpecResolver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.JobException;
import org.apache.gobblin.runtime.listeners.EmailNotificationJobListener;
import org.apache.gobblin.runtime.listeners.RunOnceJobListener;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.SchedulerUtils;
import org.apache.gobblin.util.filesystem.PathAlterationListenerAdaptor;


/**
 * Inner subclass of PathAlterationListenerAdaptor for implementation of Listen's methods,
 * avoiding anonymous class
 */
public class PathAlterationListenerAdaptorForMonitor extends PathAlterationListenerAdaptor {

  private static final Logger LOG = LoggerFactory.getLogger(PathAlterationListenerAdaptorForMonitor.class);

  Path jobConfigFileDirPath;
  JobScheduler jobScheduler;
  /** Store path to job mappings. Required for correctly unscheduling. */
  private final Map<Path, String> jobNameMap;
  private final JobSpecResolver jobSpecResolver;

  PathAlterationListenerAdaptorForMonitor(Path jobConfigFileDirPath, JobScheduler jobScheduler) {
    this.jobConfigFileDirPath = jobConfigFileDirPath;
    this.jobScheduler = jobScheduler;
    this.jobNameMap = Maps.newConcurrentMap();
    this.jobSpecResolver = jobScheduler.getJobSpecResolver();
  }

  private Path getJobPath(Properties jobProps) {
    return PathUtils.getPathWithoutSchemeAndAuthority(new Path(jobProps.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY)));
  }

  public void addToJobNameMap(Properties jobProps) {
    this.jobNameMap.put(getJobPath(jobProps),
        jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY));
  }

  public void handleNewJob(Properties jobProps, JobScheduler.Action action) {
    try {
      String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
      Path jobPath = getJobPath(jobProps);

      if (action.equals(JobScheduler.Action.UNSCHEDULE)) {
        throw new RuntimeException("Should not call loadNewCommonConfigAndHandleNewJob to unschedule the jobs: " + jobName);
      }

      if (action.equals(JobScheduler.Action.RESCHEDULE)) {
        // prep for re-scheduling: unschedule and delete the old job first
        if (this.jobNameMap.containsKey(jobPath)) {
          jobScheduler.unscheduleJob(this.jobNameMap.get(jobPath));
          this.jobNameMap.remove(jobPath);
        }
      }

      // Schedule the job
      boolean runOnce = Boolean.parseBoolean(jobProps.getProperty(ConfigurationKeys.JOB_RUN_ONCE_KEY, "false"));
      if(runOnce){
        Runnable jobToRun = jobScheduler.new NonScheduledJobRunner(jobProps, new RunOnceJobListener());
        jobScheduler.submitRunnableToExecutor(jobToRun);
        LOG.debug("[JobScheduler] The new job " + jobName + " is submitted to run once.");
      }else{
        // schedule the job with the new job configuration
        jobScheduler.scheduleJob(jobProps, runOnce ? new RunOnceJobListener() : new EmailNotificationJobListener());
        addToJobNameMap(jobProps);
        LOG.debug("[JobScheduler] The new job " + jobName + " is scheduled.");
      }
    } catch (JobException je) {
      LOG.error("Failed to " + action.name().toLowerCase() + " new job loaded from job configuration file " + jobProps.getProperty(
              ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY), je);
    }
  }

  public void loadNewJobConfigAndHandleNewJob(Path path, JobScheduler.Action action) {
    try {
      Properties jobProps = SchedulerUtils.loadGenericJobConfig(this.jobScheduler.properties, path, jobConfigFileDirPath, this.jobSpecResolver);
      LOG.debug("Loaded job properties: {}", jobProps);
      handleNewJob(jobProps, action);
    } catch (ConfigurationException | IOException e) {
      LOG.error("Failed to load from job configuration file " + path.toString(), e);
    }
  }

  public void loadNewCommonConfigAndHandleNewJob(Path path, JobScheduler.Action action) {
    try {
      for (Properties jobProps : SchedulerUtils.loadGenericJobConfigs(jobScheduler.properties, path,
          jobConfigFileDirPath, this.jobSpecResolver)) {
          handleNewJob(jobProps, action);
      }
    } catch (ConfigurationException | IOException e) {
      LOG.error(
          "Failed to reload job configuration files  while " + action.toString() + "ing for job:" + path.toString(), e);
    }
  }

  @Override
  public void onFileCreate(Path path) {
    String fileExtension = path.getName().substring(path.getName().lastIndexOf('.') + 1);
    String noExtFileName = path.getName().substring(0, path.getName().lastIndexOf('.'));
    if (fileExtension.equalsIgnoreCase(SchedulerUtils.JOB_PROPS_FILE_EXTENSION)) {
      //check no other properties pre-existed
      try {
        if (checkCommonPropExistance(path.getParent(), noExtFileName)) {
          return;
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      LOG.info("Detected creation of common properties file" + path.toString());
      // New .properties file founded with some new attributes, reschedule jobs.
      loadNewCommonConfigAndHandleNewJob(path, JobScheduler.Action.SCHEDULE);
      return;
    }
    if (!jobScheduler.jobConfigFileExtensions.contains(fileExtension)) {
      return;
    }
    LOG.info("Detected creation of new job configuration file: " + path.toString());

    loadNewJobConfigAndHandleNewJob(path, JobScheduler.Action.SCHEDULE);
  }

  /**
   * Called when a job configuration file is changed.
   */
  @Override
  public void onFileChange(Path path) {
    String fileExtension = path.getName().substring(path.getName().lastIndexOf('.') + 1);
    if (fileExtension.equalsIgnoreCase(SchedulerUtils.JOB_PROPS_FILE_EXTENSION)) {
      LOG.info("Detected change to common properties file " + path.toString());
      loadNewCommonConfigAndHandleNewJob(path, JobScheduler.Action.RESCHEDULE);
      return;
    }

    if (!jobScheduler.jobConfigFileExtensions.contains(fileExtension)) {
      // Not a job configuration file, ignore.
      return;
    }

    LOG.info("Detected change to job configuration file " + path.toString());
    loadNewJobConfigAndHandleNewJob(path, JobScheduler.Action.RESCHEDULE);
  }

  /**
   * Called when a job configuration file is deleted.
   */
  @Override
  public void onFileDelete(Path path) {
    String fileExtension = path.getName().substring(path.getName().lastIndexOf('.') + 1);
    if (fileExtension.equalsIgnoreCase(SchedulerUtils.JOB_PROPS_FILE_EXTENSION)) {
      LOG.info("Detected deletion of common properties file " + path.toString());
      // For JobProps, deletion in local folder means inheritance from ancestor folder and reschedule.
      loadNewCommonConfigAndHandleNewJob(path, JobScheduler.Action.RESCHEDULE);
      return;
    }

    if (!jobScheduler.jobConfigFileExtensions.contains(fileExtension)) {
      // Not a job configuration file, ignore.
      return;
    }

    LOG.info("Detected deletion of job configuration file " + path.toString());
    // As for normal job file, deletion means unschedule
    unscheduleJobAtPath(path);
  }

  private void unscheduleJobAtPath(Path path) {
    try {
      Path pathWithoutSchemeOrAuthority = PathUtils.getPathWithoutSchemeAndAuthority(path);
      String jobName = this.jobNameMap.get(pathWithoutSchemeOrAuthority);
      if (jobName == null) {
        LOG.info("Could not find a scheduled job to unschedule with path " + pathWithoutSchemeOrAuthority);
        return;
      }
      LOG.info("Unscheduling job " + jobName);
      this.jobScheduler.unscheduleJob(jobName);
      this.jobNameMap.remove(pathWithoutSchemeOrAuthority);
    } catch (JobException je) {
      LOG.error("Could not unschedule job " + this.jobNameMap.get(path));
    }
  }

  /**
   * Given the target rootPath, check if there's common properties existed. Return false if so.
   * @param rootPath
   * @return
   */
  private boolean checkCommonPropExistance(Path rootPath, String noExtFileName)
      throws IOException {
    Configuration conf = new Configuration();
    FileStatus[] children = rootPath.getFileSystem(conf).listStatus(rootPath);

    for (FileStatus aChild : children) {
      if (aChild.getPath().getName().contains(noExtFileName)) {
        return false;
      }
    }
    return true;
  }
}
