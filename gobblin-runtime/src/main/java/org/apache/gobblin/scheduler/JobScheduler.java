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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.fs.Path;

import org.quartz.CronScheduleBuilder;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.InterruptableJob;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.UnableToInterruptJobException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.AbstractIdleService;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.JobException;
import org.apache.gobblin.runtime.JobLauncher;
import org.apache.gobblin.runtime.JobLauncherFactory;
import org.apache.gobblin.runtime.listeners.EmailNotificationJobListener;
import org.apache.gobblin.runtime.listeners.JobListener;
import org.apache.gobblin.runtime.listeners.RunOnceJobListener;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.SchedulerUtils;
import org.apache.gobblin.util.filesystem.PathAlterationObserverScheduler;


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
 * @author Yinan Li
 */
public class JobScheduler extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(JobScheduler.class);

  public enum Action {
    SCHEDULE, RESCHEDULE, UNSCHEDULE
  }

  public static final String JOB_SCHEDULER_KEY = "jobScheduler";
  public static final String PROPERTIES_KEY = "jobProps";
  public static final String JOB_LISTENER_KEY = "jobListener";

  // System configuration properties
  public final Properties properties;

  // A Quartz scheduler
  private final SchedulerService scheduler;

  // A thread pool executor for running jobs without schedules
  protected final ExecutorService jobExecutor;

  // Mapping between jobs to job listeners associated with them
  private final Map<String, JobListener> jobListenerMap = Maps.newHashMap();

  // A map for all scheduled jobs
  private final Map<String, JobKey> scheduledJobs = Maps.newHashMap();

  // Set of supported job configuration file extensions
  public final Set<String> jobConfigFileExtensions;

  public final Path jobConfigFileDirPath;

  // A monitor for changes to job conf files for general FS
  public final PathAlterationObserverScheduler pathAlterationDetector;
  public final PathAlterationListenerAdaptorForMonitor listener;

  // A period of time for scheduler to wait until jobs are finished
  private final boolean waitForJobCompletion;

  private final Closer closer = Closer.create();

  private volatile boolean cancelRequested = false;

  public JobScheduler(Properties properties, SchedulerService scheduler)
      throws Exception {
    this.properties = properties;
    this.scheduler = scheduler;

    this.jobExecutor = Executors.newFixedThreadPool(Integer.parseInt(
        properties.getProperty(ConfigurationKeys.JOB_EXECUTOR_THREAD_POOL_SIZE_KEY,
            Integer.toString(ConfigurationKeys.DEFAULT_JOB_EXECUTOR_THREAD_POOL_SIZE))),
        ExecutorsUtils.newThreadFactory(Optional.of(LOG), Optional.of("JobScheduler-%d")));

    this.jobConfigFileExtensions = Sets.newHashSet(Splitter.on(",")
        .omitEmptyStrings()
        .split(this.properties.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_EXTENSIONS_KEY,
            ConfigurationKeys.DEFAULT_JOB_CONFIG_FILE_EXTENSIONS)));

    long pollingInterval = Long.parseLong(
        this.properties.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL_KEY,
            Long.toString(ConfigurationKeys.DEFAULT_JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL)));
    this.pathAlterationDetector = new PathAlterationObserverScheduler(pollingInterval);

    this.waitForJobCompletion = Boolean.parseBoolean(
        this.properties.getProperty(ConfigurationKeys.SCHEDULER_WAIT_FOR_JOB_COMPLETION_KEY,
            ConfigurationKeys.DEFAULT_SCHEDULER_WAIT_FOR_JOB_COMPLETION));

    if (this.properties.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY)) {
      this.jobConfigFileDirPath = new Path(this.properties.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY));
      this.listener = new PathAlterationListenerAdaptorForMonitor(jobConfigFileDirPath, this);
    } else {
      // This is needed because HelixJobScheduler does not use the same way of finding changed paths
      this.jobConfigFileDirPath = null;
      this.listener = null;
    }
  }

  @Override
  protected void startUp()
      throws Exception {
    LOG.info("Starting the job scheduler");

    try {
      this.scheduler.awaitRunning(30, TimeUnit.SECONDS);
    } catch (TimeoutException | IllegalStateException exc) {
      throw new IllegalStateException("Scheduler service is not running.");
    }

    // Note: This should not be mandatory, gobblin-cluster modes have their own job configuration managers
    if (this.properties.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY)
            || this.properties.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY)) {

      if (this.properties.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY) && !this.properties.containsKey(
              ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY)) {
        this.properties.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY,
                "file://" + this.properties.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY));
      }
      startServices();
    }
  }

  protected void startServices() throws Exception {
    startGeneralJobConfigFileMonitor();
    scheduleGeneralConfiguredJobs();
  }

  @Override
  protected void shutDown()
      throws Exception {
    LOG.info("Stopping the job scheduler");
    closer.close();
    cancelRequested = true;
    List<JobExecutionContext> currentExecutions = this.scheduler.getScheduler().getCurrentlyExecutingJobs();
    for (JobExecutionContext jobExecutionContext : currentExecutions) {
      try {
        this.scheduler.getScheduler().interrupt(jobExecutionContext.getFireInstanceId());
      } catch (UnableToInterruptJobException e) {
        LOG.error("Failed to cancel job " + jobExecutionContext.getJobDetail().getKey(), e);
      }
    }

    ExecutorsUtils.shutdownExecutorService(this.jobExecutor, Optional.of(LOG));
  }

  /**
   * Schedule a job.
   *
   * <p>
   *   This method calls the Quartz scheduler to scheduler the job.
   * </p>
   *
   * @param jobProps Job configuration properties
   * @param jobListener {@link JobListener} used for callback,
   *                    can be <em>null</em> if no callback is needed.
   * @throws JobException when there is anything wrong
   *                      with scheduling the job
   */
  public void scheduleJob(Properties jobProps, JobListener jobListener)
      throws JobException {
    try {
      scheduleJob(jobProps, jobListener, Maps.<String, Object>newHashMap(), GobblinJob.class);
    } catch (JobException | RuntimeException exc) {
      LOG.error("Could not schedule job " + jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY, "Unknown job"), exc);
    }
  }

  /**
   * Schedule a job immediately.
   *
   * <p>
   *   This method calls the Quartz scheduler to scheduler the job.
   * </p>
   *
   * @param jobProps Job configuration properties
   * @param jobListener {@link JobListener} used for callback,
   *                    can be <em>null</em> if no callback is needed.
   * @throws JobException when there is anything wrong
   *                      with scheduling the job
   */
  public Future<?> scheduleJobImmediately(Properties jobProps, JobListener jobListener, JobLauncher jobLauncher) {
    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        try {
          runJob(jobProps, jobListener, jobLauncher);
        } catch (JobException je) {
          LOG.error("Failed to run job " + jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), je);
        }
      }
    };
    final Future<?> future = this.jobExecutor.submit(runnable);
    return new Future() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        if (!cancelRequested) {
          return false;
        }
        boolean result = true;
        try {
          jobLauncher.cancelJob(jobListener);
        } catch (JobException e) {
          LOG.error("Failed to cancel job " + jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), e);
          result = false;
        }
        if (mayInterruptIfRunning) {
          result &= future.cancel(true);
        }
        return result;
      }

      @Override
      public boolean isCancelled() {
        return future.isCancelled();
      }

      @Override
      public boolean isDone() {
        return future.isDone();
      }

      @Override
      public Object get() throws InterruptedException, ExecutionException {
        return future.get();
      }

      @Override
      public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(timeout, unit);
      }
    };
  }

  /**
   * Submit a runnable to the {@link ExecutorService} of this {@link JobScheduler}.
   * @param runnable the runnable to submit to the job executor
   */
  public void submitRunnableToExecutor(Runnable runnable) {
    this.jobExecutor.execute(runnable);
  }

  /**
   * Schedule a job.
   *
   * <p>
   *   This method does what {@link #scheduleJob(Properties, JobListener)} does, and additionally it
   *   allows the caller to pass in additional job data and the {@link Job} implementation class.
   * </p>
   *
   * @param jobProps Job configuration properties
   * @param jobListener {@link JobListener} used for callback,
   *                    can be <em>null</em> if no callback is needed.
   * @param additionalJobData additional job data in a {@link Map}
   * @param jobClass Quartz job class
   * @throws JobException when there is anything wrong
   *                      with scheduling the job
   */
  public void scheduleJob(Properties jobProps, JobListener jobListener, Map<String, Object> additionalJobData,
      Class<? extends Job> jobClass)
      throws JobException {
    Preconditions.checkArgument(jobProps.containsKey(ConfigurationKeys.JOB_NAME_KEY),
        "A job must have a job name specified by job.name");
    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);

    if (this.scheduledJobs.containsKey(jobName)) {
      LOG.warn("Job " + jobName + " has already been scheduled");
      return;
    }

    // Check if the job has been disabled
    boolean disabled = Boolean.valueOf(jobProps.getProperty(ConfigurationKeys.JOB_DISABLED_KEY, "false"));
    if (disabled) {
      LOG.info("Skipping disabled job " + jobName);
      return;
    }

    if (!jobProps.containsKey(ConfigurationKeys.JOB_SCHEDULE_KEY)) {
      // Submit the job to run
      this.jobExecutor.execute(new NonScheduledJobRunner(jobProps, jobListener));
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
    jobDataMap.putAll(additionalJobData);

    // Build a Quartz job
    JobDetail job = JobBuilder.newJob(jobClass)
        .withIdentity(jobName, Strings.nullToEmpty(jobProps.getProperty(ConfigurationKeys.JOB_GROUP_KEY)))
        .withDescription(Strings.nullToEmpty(jobProps.getProperty(ConfigurationKeys.JOB_DESCRIPTION_KEY)))
        .usingJobData(jobDataMap)
        .build();

    try {
      // Schedule the Quartz job with a trigger built from the job configuration
      Trigger trigger = getTrigger(job.getKey(), jobProps);
      this.scheduler.getScheduler().scheduleJob(job, trigger);
      LOG.info(String.format("Scheduled job %s. Next run: %s.", job.getKey(), trigger.getNextFireTime()));
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
        this.scheduler.getScheduler().deleteJob(this.scheduledJobs.remove(jobName));
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
   *   This method runs the job immediately without going through the Quartz scheduler.
   *   This is particularly useful for testing.
   * </p>
   *
   * @param jobProps Job configuration properties
   * @param jobListener {@link JobListener} used for callback, can be <em>null</em> if no callback is needed.
   * @throws JobException when there is anything wrong with running the job
   */
  public void runJob(Properties jobProps, JobListener jobListener)
      throws JobException {
    try {
      runJob(jobProps, jobListener, JobLauncherFactory.newJobLauncher(this.properties, jobProps));
    } catch (Exception e) {
      throw new JobException("Failed to run job " + jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), e);
    }
  }

  /**
   * Run a job.
   *
   * <p>
   *   This method runs the job immediately without going through the Quartz scheduler.
   *   This is particularly useful for testing.
   * </p>
   *
   * <p>
   *   This method does what {@link #runJob(Properties, JobListener)} does, and additionally it allows
   *   the caller to pass in a {@link JobLauncher} instance used to launch the job to run.
   * </p>
   *
   * @param jobProps Job configuration properties
   * @param jobListener {@link JobListener} used for callback, can be <em>null</em> if no callback is needed.
   * @param jobLauncher a {@link JobLauncher} object used to launch the job to run
   * @throws JobException when there is anything wrong with running the job
   */
  public void runJob(Properties jobProps, JobListener jobListener, JobLauncher jobLauncher)
      throws JobException {
    Preconditions.checkArgument(jobProps.containsKey(ConfigurationKeys.JOB_NAME_KEY),
        "A job must have a job name specified by job.name");
    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);

    // Check if the job has been disabled
    boolean disabled = Boolean.valueOf(jobProps.getProperty(ConfigurationKeys.JOB_DISABLED_KEY, "false"));
    if (disabled) {
      LOG.info("Skipping disabled job " + jobName);
      return;
    }

    // Launch the job
    try (Closer closer = Closer.create()) {
      closer.register(jobLauncher).launchJob(jobListener);
      boolean runOnce = Boolean.valueOf(jobProps.getProperty(ConfigurationKeys.JOB_RUN_ONCE_KEY, "false"));
      if (runOnce && this.scheduledJobs.containsKey(jobName)) {
        this.scheduler.getScheduler().deleteJob(this.scheduledJobs.remove(jobName));
      }
    } catch (Throwable t) {
      throw new JobException("Failed to launch and run job " + jobName, t);
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
   * Schedule Gobblin jobs in general position
   */
  private void scheduleGeneralConfiguredJobs()
      throws ConfigurationException, JobException, IOException {
    LOG.info("Scheduling configured jobs");
    for (Properties jobProps : loadGeneralJobConfigs()) {

      if (!jobProps.containsKey(ConfigurationKeys.JOB_SCHEDULE_KEY)) {
        // A job without a cron schedule is considered a one-time job
        jobProps.setProperty(ConfigurationKeys.JOB_RUN_ONCE_KEY, "true");
      }

      boolean runOnce = Boolean.valueOf(jobProps.getProperty(ConfigurationKeys.JOB_RUN_ONCE_KEY, "false"));
      scheduleJob(jobProps, runOnce ? new RunOnceJobListener() : new EmailNotificationJobListener());
      this.listener.addToJobNameMap(jobProps);
    }
  }

  /**
   * Load job configuration file(s) from general source
   */
  private List<Properties> loadGeneralJobConfigs()
      throws ConfigurationException, IOException {
    List<Properties> jobConfigs = SchedulerUtils.loadGenericJobConfigs(this.properties);
    LOG.info(String.format("Loaded %d job configurations", jobConfigs.size()));
    return jobConfigs;
  }

  /**
   * Start the job configuration file monitor using generic file system API.
   *
   * <p>
   *   The job configuration file monitor currently only supports monitoring the following types of changes:
   *
   *   <ul>
   *     <li>New job configuration files.</li>
   *     <li>Changes to existing job configuration files.</li>
   *     <li>Changes to existing common properties file with a .properties extension.</li>
   *     <li>Deletion to existing job configuration files.</li>
   *     <li>Deletion to existing common properties file with a .properties extension.</li>
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
  private void startGeneralJobConfigFileMonitor()
      throws Exception {
    SchedulerUtils.addPathAlterationObserver(this.pathAlterationDetector, this.listener, jobConfigFileDirPath);
    this.pathAlterationDetector.start();
    this.closer.register(new Closeable() {
      @Override
      public void close() throws IOException {
        try {
          pathAlterationDetector.stop(1000);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
    });
  }

  /**
   * Get a {@link org.quartz.Trigger} from the given job configuration properties.
   */
  private Trigger getTrigger(JobKey jobKey, Properties jobProps) {
    // Build a trigger for the job with the given cron-style schedule
    return TriggerBuilder.newTrigger()
        .withIdentity(jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY),
            Strings.nullToEmpty(jobProps.getProperty(ConfigurationKeys.JOB_GROUP_KEY)))
        .forJob(jobKey)
        .withSchedule(CronScheduleBuilder.cronSchedule(jobProps.getProperty(ConfigurationKeys.JOB_SCHEDULE_KEY)))
        .build();
  }

  /**
   * A Gobblin job to be scheduled.
   */
  @DisallowConcurrentExecution
  @Slf4j
  public static class GobblinJob extends BaseGobblinJob implements InterruptableJob {
    @Override
    public void executeImpl(JobExecutionContext context)
        throws JobExecutionException {
      LOG.info("Starting job " + context.getJobDetail().getKey());
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

    @Override
    public void interrupt()
        throws UnableToInterruptJobException {
      log.info("Job was interrupted");
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
