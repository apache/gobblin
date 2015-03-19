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

package gobblin.scheduler;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.quartz.CronScheduleBuilder;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractIdleService;

import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.EmailNotificationJobListener;
import gobblin.runtime.JobException;
import gobblin.runtime.JobLauncher;
import gobblin.runtime.JobLauncherFactory;
import gobblin.runtime.JobListener;
import gobblin.runtime.RunOnceJobListener;
import gobblin.util.ExecutorsUtils;
import gobblin.util.JobLauncherUtils;
import gobblin.util.SchedulerUtils;


/**
 * Gobblin job scheduler.
 *
 * <p>
 *     The scheduler is a pure scheduler in the sense that it is only responsible
 *     for scheduling Gobblin jobs. Job state tracking and monitoring are handled
 *     by the {@link JobLauncher}.
 * </p>
 *
 * <p>
 *     For job scheduling, This class uses a Quartz {@link org.quartz.Scheduler}.
 *     Each job is associated with a cron schedule that is used to create a
 *     {@link org.quartz.Trigger} for the job.
 * </p>
 *
 * @author ynli
 */
public class JobScheduler extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(JobScheduler.class);

  private static final String JOB_SCHEDULER_KEY = "jobScheduler";
  private static final String PROPERTIES_KEY = "jobProps";
  private static final String JOB_LISTENER_KEY = "jobListener";

  // System configuration properties
  private final Properties properties;

  // A Quartz scheduler
  private final Scheduler scheduler;

  // A thread pool executor for running jobs without schedules
  private final ExecutorService jobExecutor;

  // Mapping between jobs to job listeners associated with them
  private final Map<String, JobListener> jobListenerMap = Maps.newHashMap();

  // A map for all scheduled jobs
  private final Map<String, JobKey> scheduledJobs = Maps.newHashMap();

  // Set of supported job configuration file extensions
  private final Set<String> jobConfigFileExtensions;

  // A monitor for changes to job configuration files
  private final FileAlterationMonitor fileAlterationMonitor;

  public JobScheduler(Properties properties)
      throws Exception {
    this.properties = properties;
    this.scheduler = new StdSchedulerFactory().getScheduler();

    this.jobExecutor = Executors.newFixedThreadPool(
        Integer.parseInt(properties.getProperty(ConfigurationKeys.JOB_EXECUTOR_THREAD_POOL_SIZE_KEY,
            Integer.toString(ConfigurationKeys.DEFAULT_JOB_EXECUTOR_THREAD_POOL_SIZE))),
        ExecutorsUtils.newThreadFactory(Optional.of(LOG), Optional.of("JobScheduler-%d")));

    this.jobConfigFileExtensions = Sets.newHashSet(Splitter.on(",").omitEmptyStrings().split(this.properties
        .getProperty(ConfigurationKeys.JOB_CONFIG_FILE_EXTENSIONS_KEY,
            ConfigurationKeys.DEFAULT_JOB_CONFIG_FILE_EXTENSIONS)));

    long pollingInterval = Long.parseLong(this.properties.getProperty(
        ConfigurationKeys.JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL_KEY,
        Long.toString(ConfigurationKeys.DEFAULT_JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL)));
    this.fileAlterationMonitor = new FileAlterationMonitor(pollingInterval);
  }

  @Override
  protected void startUp()
      throws Exception {
    LOG.info("Starting the job scheduler");
    this.scheduler.start();
    if (this.properties.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY)) {
      scheduleLocallyConfiguredJobs();
      startJobConfigFileMonitor();
    }
  }

  @Override
  protected void shutDown()
      throws Exception {
    LOG.info("Stopping the job scheduler");
    this.scheduler.shutdown(true);
    if (this.properties.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY)) {
      // Stop the file alteration monitor in one second
      this.fileAlterationMonitor.stop(1000);
    }
  }

  /**
   * Schedule a job.
   *
   * <p>
   *     This method calls the Quartz scheduler to scheduler the job.
   * </p>
   *
   * @param jobProps Job configuration properties
   * @param jobListener {@link gobblin.runtime.JobListener} used for callback,
   *                    can be <em>null</em> if no callback is needed.
   * @throws JobException when there is anything wrong
   *                      with scheduling the job
   */
  public void scheduleJob(Properties jobProps, JobListener jobListener)
      throws JobException {
    Preconditions.checkNotNull(jobProps);

    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    if (Strings.isNullOrEmpty(jobName)) {
      throw new JobException("A job must have a job name specified by job.name");
    }

    // Check if the job has been disabled
    boolean disabled = Boolean.valueOf(jobProps.getProperty(ConfigurationKeys.JOB_DISABLED_KEY, "false"));
    if (disabled) {
      LOG.info("Skipping disabled job " + jobName);
      return;
    }

    if (!jobProps.containsKey(ConfigurationKeys.JOB_SCHEDULE_KEY)) {
      // A job without a cron schedule is considered a one-time job
      jobProps.setProperty(ConfigurationKeys.JOB_RUN_ONCE_KEY, "true");
      // Submit the job to run
      this.jobExecutor.submit(new NonScheduledJobRunner(jobProps, jobListener));
      return;
    }

    if (jobListener != null) {
      this.jobListenerMap.put(jobName, jobListener);
    }

    // Build a data map that gets passed to the job
    JobDataMap jobDataMap = new JobDataMap();
    jobDataMap.put(JOB_SCHEDULER_KEY, this);
    jobDataMap.put(PROPERTIES_KEY, jobProps);
    jobDataMap.put(JOB_LISTENER_KEY, jobListener);

    // Build a Quartz job
    JobDetail job = JobBuilder.newJob(GobblinJob.class)
        .withIdentity(jobName, Strings.nullToEmpty(jobProps.getProperty(ConfigurationKeys.JOB_GROUP_KEY)))
        .withDescription(Strings.nullToEmpty(jobProps.getProperty(ConfigurationKeys.JOB_DESCRIPTION_KEY)))
        .usingJobData(jobDataMap).build();

    try {
      // Schedule the Quartz job with a trigger built from the job configuration
      this.scheduler.scheduleJob(job, getTrigger(job.getKey(), jobProps));
    } catch (SchedulerException se) {
      LOG.error("Failed to schedule job " + jobName, se);
      throw new JobException("Failed to schedule job " + jobName, se);
    }

    this.scheduledJobs.put(jobName, job.getKey());
  }

  /**
   * Unschedule and delete a job.
   *
   * @param jobName Job name
   * @throws JobException when there is anything wrong unschedule the job
   */
  public void unscheduleJob(String jobName)
      throws JobException {
    if (this.scheduledJobs.containsKey(jobName)) {
      try {
        this.scheduler.deleteJob(this.scheduledJobs.remove(jobName));
      } catch (SchedulerException se) {
        LOG.error("Failed to unschedule and delete job " + jobName, se);
        throw new JobException("Failed to unschedule and delete job " + jobName, se);
      }
    }
  }

  /**
   * Run a job.
   *
   * <p>
   *     This method runs the job immediately without going through the Quartz scheduler.
   *     This is particularly useful for testing.
   * </p>
   *
   * @param jobProps Job configuration properties
   * @param jobListener {@link JobListener} used for callback,
   *                    can be <em>null</em> if no callback is needed.
   * @throws JobException when there is anything wrong
   *                      with running the job
   */
  public void runJob(Properties jobProps, JobListener jobListener)
      throws JobException {
    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    if (Strings.isNullOrEmpty(jobName)) {
      throw new JobException("A job must have a job name specified by job.name");
    }

    // Populate the assigned job ID
    jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, JobLauncherUtils.newJobId(jobName));

    // Launch the job
    try {
      JobLauncher jobLauncher = JobLauncherFactory.newJobLauncher(this.properties);
      jobLauncher.launchJob(jobProps, jobListener);
      boolean runOnce = Boolean.valueOf(jobProps.getProperty(ConfigurationKeys.JOB_RUN_ONCE_KEY, "false"));
      if (runOnce && this.scheduledJobs.containsKey(jobName)) {
        this.scheduler.deleteJob(this.scheduledJobs.remove(jobName));
      }
    } catch (Throwable t) {
      String errMsg = "Failed to launch and run job " + jobName;
      LOG.error(errMsg, t);
      throw new JobException(errMsg, t);
    }
  }

  /**
   * Get the names of the scheduled jobs.
   *
   * @return names of the scheduled jobs
   */
  public Collection<String> getScheduledJobs() {
    return this.scheduledJobs.keySet();
  }

  /**
   * Schedule locally configured Gobblin jobs.
   */
  private void scheduleLocallyConfiguredJobs()
      throws ConfigurationException, JobException {
    LOG.info("Scheduling locally configured jobs");
    for (Properties jobProps : loadLocalJobConfigs()) {
      boolean runOnce = Boolean.valueOf(jobProps.getProperty(ConfigurationKeys.JOB_RUN_ONCE_KEY, "false"));
      scheduleJob(jobProps, runOnce ? new RunOnceJobListener() : new EmailNotificationJobListener());
    }
  }

  /**
   * Load local job configurations.
   */
  private List<Properties> loadLocalJobConfigs()
      throws ConfigurationException {
    List<Properties> jobConfigs = SchedulerUtils.loadJobConfigs(this.properties);
    LOG.info(String.format(jobConfigs.size() <= 1 ? "Loaded %d job configuration" : "Loaded %d job configurations",
        jobConfigs.size()));

    return jobConfigs;
  }

  /**
   * Start the job configuration file monitor.
   *
   * <p>
   *   The job configuration file monitor currently only supports monitoring the following types of changes:
   *
   *   <ul>
   *     <li>New job configuration files.</li>
   *     <li>Changes to existing job configuration files.</li>
   *     <li>Changes to existing common properties file with a .properties extension.</li>
   *   </ul>
   * </p>
   *
   * <p>
   *   This monitor has one limitation: in case more than one file including at least one common properties
   *   file are changed between two adjacent checks, the reloading of affected job configuration files may
   *   be intermixed and applied in an order that is not desirable. This is because the order the listener
   *   is called on the changes is not controlled by Gobblin, but instead by the monitor itself.
   * </p>
   */
  private void startJobConfigFileMonitor()
      throws Exception {
    final File jobConfigFileDir = new File(this.properties.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY));
    FileAlterationListener listener = new FileAlterationListenerAdaptor() {
      /**
       * Called when a new job configuration file is dropped in.
       */
      @Override
      public void onFileCreate(File file) {
        String fileExtension = Files.getFileExtension(file.getName());
        if (!jobConfigFileExtensions.contains(fileExtension)) {
          // Not a job configuration file, ignore.
          return;
        }

        // Load the new job configuration and schedule the new job
        try {
          LOG.info("Detected new job configuration file " + file.getAbsolutePath());
          Properties jobProps = SchedulerUtils.loadJobConfig(properties, file, jobConfigFileDir);
          boolean runOnce = Boolean.valueOf(jobProps.getProperty(ConfigurationKeys.JOB_RUN_ONCE_KEY, "false"));
          scheduleJob(jobProps, runOnce ? new RunOnceJobListener() : new EmailNotificationJobListener());
        } catch (ConfigurationException ce) {
          LOG.error("Failed to load from job configuration file " + file.getAbsolutePath(), ce);
        } catch (IOException ioe) {
          LOG.error("Failed to load from job configuration file " + file.getAbsolutePath(), ioe);
        } catch (JobException je) {
          LOG.error("Failed to schedule new job loaded from job configuration file " + file.getAbsolutePath(), je);
        }
      }

      /**
       * Called when a job configuration file is changed.
       */
      @Override
      public void onFileChange(File file) {
        String fileExtension = Files.getFileExtension(file.getName());
        if (fileExtension.equalsIgnoreCase(SchedulerUtils.JOB_PROPS_FILE_EXTENSION)) {
          LOG.info("Detected change to common properties file " + file.getAbsolutePath());
          try {
            for (Properties jobProps : SchedulerUtils.loadJobConfigs(properties, file, jobConfigFileDir)) {
              try {
                rescheduleJob(jobProps);
              } catch (JobException je) {
                LOG.error("Failed to reschedule job reloaded from job configuration file " + jobProps
                    .getProperty(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY), je);
              }
            }
          } catch (ConfigurationException ce) {
            LOG.error("Failed to reload job configuration files affected by changes to " + file.getAbsolutePath(), ce);
          } catch (IOException ioe) {
            LOG.error("Failed to reload job configuration files affected by changes to " + file.getAbsolutePath(), ioe);
          }
          return;
        }

        if (!jobConfigFileExtensions.contains(fileExtension)) {
          // Not a job configuration file, ignore.
          return;
        }

        try {
          LOG.info("Detected change to job configuration file " + file.getAbsolutePath());
          Properties jobProps = SchedulerUtils.loadJobConfig(properties, file, jobConfigFileDir);
          rescheduleJob(jobProps);
        } catch (ConfigurationException ce) {
          LOG.error("Failed to reload from job configuration file " + file.getAbsolutePath(), ce);
        } catch (IOException ioe) {
          LOG.error("Failed to reload from job configuration file " + file.getAbsolutePath(), ioe);
        } catch (JobException je) {
          LOG.error("Failed to reschedule job reloaded from job configuration file " + file.getAbsolutePath(), je);
        }
      }

      private void rescheduleJob(Properties jobProps) throws JobException {
        String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
        // First unschedule and delete the old job
        unscheduleJob(jobName);
        boolean runOnce = Boolean.valueOf(jobProps.getProperty(ConfigurationKeys.JOB_RUN_ONCE_KEY, "false"));
        // Reschedule the job with the new job configuration
        scheduleJob(jobProps, runOnce ? new RunOnceJobListener() : new EmailNotificationJobListener());
      }
    };

    SchedulerUtils.addFileAlterationObserver(this.fileAlterationMonitor, listener, jobConfigFileDir);
    this.fileAlterationMonitor.start();
  }

  /**
   * Get a {@link org.quartz.Trigger} from the given job configuration properties.
   */
  private Trigger getTrigger(JobKey jobKey, Properties jobProps) {
    // Build a trigger for the job with the given cron-style schedule
    return TriggerBuilder.newTrigger().withIdentity(jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY),
        Strings.nullToEmpty(jobProps.getProperty(ConfigurationKeys.JOB_GROUP_KEY))).forJob(jobKey)
        .withSchedule(CronScheduleBuilder.cronSchedule(jobProps.getProperty(ConfigurationKeys.JOB_SCHEDULE_KEY)))
        .build();
  }

  /**
   * A Gobblin job to be scheduled.
   */
  @DisallowConcurrentExecution
  public static class GobblinJob implements Job {

    @Override
    public void execute(JobExecutionContext context)
        throws JobExecutionException {
      JobDataMap dataMap = context.getJobDetail().getJobDataMap();
      JobScheduler jobScheduler = (JobScheduler) dataMap.get(JOB_SCHEDULER_KEY);
      Properties jobProps = (Properties) dataMap.get(PROPERTIES_KEY);
      JobListener jobListener = (JobListener) dataMap.get(JOB_LISTENER_KEY);

      try {
        jobScheduler.runJob(jobProps, jobListener);
      } catch (Throwable t) {
        throw new JobExecutionException(t);
      }
    }
  }

  /**
   * A class for running non-scheduled Gobblin jobs.
   */
  class NonScheduledJobRunner implements Runnable {

    private final Properties jobProps;
    private final JobListener jobListener;

    public NonScheduledJobRunner(Properties jobProps, JobListener jobListener) {
      this.jobProps = jobProps;
      this.jobListener = jobListener;
    }

    @Override
    public void run() {
      try {
        JobScheduler.this.runJob(this.jobProps, this.jobListener);
      } catch (JobException je) {
        LOG.error("Failed to run job " + this.jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), je);
      }
    }
  }
}
