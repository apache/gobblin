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

package gobblin.scheduler;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.google.common.collect.Maps;

import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.JobException;
import gobblin.runtime.listeners.EmailNotificationJobListener;
import gobblin.runtime.listeners.RunOnceJobListener;
import gobblin.util.PathUtils;
import gobblin.util.SchedulerUtils;
import gobblin.util.filesystem.PathAlterationListenerAdaptor;


/**
 * Inner subclass of PathAlterationListenerAdaptor for implementation of Listen's methods,
 * avoiding anonymous class
 */
public class PathAlterationListenerAdaptorForMonitor extends PathAlterationListenerAdaptor {

  private static final Logger LOG = LoggerFactory.getLogger(JobScheduler.class);

  Path jobConfigFileDirPath;
  JobScheduler jobScheduler;
  /** Store path to job mappings. Required for correctly unscheduling. */
  private final Map<Path, String> jobNameMap;

  PathAlterationListenerAdaptorForMonitor(Path jobConfigFileDirPath, JobScheduler jobScheduler) {
    this.jobConfigFileDirPath = jobConfigFileDirPath;
    this.jobScheduler = jobScheduler;
    this.jobNameMap = Maps.newConcurrentMap();
  }

  private Path getJobPath(Properties jobProps) {
    return PathUtils.getPathWithoutSchemeAndAuthority(new Path(jobProps.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY)));
  }

  public void addToJobNameMap(Properties jobProps) {
    this.jobNameMap.put(getJobPath(jobProps),
        jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY));
  }

  public void loadNewJobConfigAndHandleNewJob(Path path, JobScheduler.Action action) {
    // Load the new job configuration and schedule the new job

    String customizedInfo = "";
    try {
      Properties jobProps =
          SchedulerUtils.loadGenericJobConfig(this.jobScheduler.properties, path, jobConfigFileDirPath);
      switch (action) {
        case SCHEDULE:
          boolean runOnce = Boolean.valueOf(jobProps.getProperty(ConfigurationKeys.JOB_RUN_ONCE_KEY, "false"));
          customizedInfo = "schedule";
          addToJobNameMap(jobProps);
          jobScheduler.scheduleJob(jobProps, runOnce ? new RunOnceJobListener() : new EmailNotificationJobListener());
          break;
        case RESCHEDULE:
          customizedInfo = "reschedule";
          rescheduleJob(jobProps);
          break;
        case UNSCHEDULE:
          throw new RuntimeException("Should not call loadNewJobConfigAndHandleNewJob for unscheduling jobs.");
        default:
          break;
      }
    } catch (ConfigurationException | IOException e) {
      LOG.error("Failed to load from job configuration file " + path.toString(), e);
    } catch (JobException je) {
      LOG.error("Failed to " + customizedInfo + " new job loaded from job configuration file " + path.toString(), je);
    }
  }

  public void loadNewCommonConfigAndHandleNewJob(Path path, JobScheduler.Action action) {
    String customizedInfoAction = "";
    String customizedInfoResult = "";
    try {
      for (Properties jobProps : SchedulerUtils.loadGenericJobConfigs(jobScheduler.properties, path,
          jobConfigFileDirPath)) {
        try {
          switch (action) {
            case SCHEDULE:
              boolean runOnce = Boolean.valueOf(jobProps.getProperty(ConfigurationKeys.JOB_RUN_ONCE_KEY, "false"));
              customizedInfoAction = "schedule";
              customizedInfoResult = "creation or equivalent action";
              addToJobNameMap(jobProps);
              jobScheduler.scheduleJob(jobProps,
                  runOnce ? new RunOnceJobListener() : new EmailNotificationJobListener());
              break;
            case RESCHEDULE:
              customizedInfoAction = "reschedule";
              customizedInfoResult = "change";
              rescheduleJob(jobProps);
              break;
            case UNSCHEDULE:
              throw new RuntimeException("Should not call loadNewCommonConfigAndHandleNewJob for unscheduling jobs.");
            default:
              break;
          }
        } catch (JobException je) {
          LOG.error(
              "Failed to " + customizedInfoAction + " job reloaded from job configuration file " + jobProps.getProperty(
                  ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY), je);
        }
      }
    } catch (ConfigurationException | IOException e) {
      LOG.error(
          "Failed to reload job configuration files affected by " + customizedInfoResult + " to " + path.toString(), e);
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
      LOG.info("Detected cration to common properties file" + path.toString());
      // New .properties file founded with some new attributes, reschedule jobs.
      loadNewCommonConfigAndHandleNewJob(path, JobScheduler.Action.RESCHEDULE);
      return;
    }
    if (!jobScheduler.jobConfigFileExtensions.contains(fileExtension)) {
      return;
    }
    LOG.info("Detected new job configuration file " + path.toString());

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
      LOG.info("Detected deletion to common properties file " + path.toString());
      // For JobProps, deletion in local folder means inheritance from ancestor folder and reschedule.
      loadNewCommonConfigAndHandleNewJob(path, JobScheduler.Action.RESCHEDULE);
      return;
    }

    if (!jobScheduler.jobConfigFileExtensions.contains(fileExtension)) {
      // Not a job configuration file, ignore.
      return;
    }

    LOG.info("Detected deletion to job configuration file " + path.toString());
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

  private void rescheduleJob(Properties jobProps)
      throws JobException {
    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    Path jobPath = getJobPath(jobProps);
    // First unschedule and delete the old job
    if (this.jobNameMap.containsKey(jobPath)) {
      jobScheduler.unscheduleJob(this.jobNameMap.get(jobPath));
      this.jobNameMap.remove(jobPath);
    }
    boolean runOnce = Boolean.valueOf(jobProps.getProperty(ConfigurationKeys.JOB_RUN_ONCE_KEY, "false"));
    // Reschedule the job with the new job configuration
    jobScheduler.scheduleJob(jobProps, runOnce ? new RunOnceJobListener() : new EmailNotificationJobListener());
    addToJobNameMap(jobProps);
    LOG.debug("[JobScheduler] The new job " + jobName + " is rescheduled.");
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
