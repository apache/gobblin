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

package gobblin.runtime.local;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.metastore.FsStateStore;
import gobblin.metastore.StateStore;
import gobblin.metrics.JobMetrics;
import gobblin.publisher.DataPublisher;
import gobblin.runtime.EmailNotificationJobListener;
import gobblin.runtime.JobException;
import gobblin.runtime.JobListener;
import gobblin.runtime.JobLock;
import gobblin.runtime.JobState;
import gobblin.runtime.RunOnceJobListener;
import gobblin.runtime.SourceDecorator;
import gobblin.runtime.TaskState;
import gobblin.runtime.WorkUnitManager;
import gobblin.source.Source;
import gobblin.source.extractor.JobCommitPolicy;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.JobLauncherUtils;
import gobblin.util.SchedulerUtils;


/**
 * A class for managing locally configured Gobblin jobs.
 *
 * <p>
 *     This class's responsibilities include scheduling and running locally configured
 *     Gobblin jobs to run, keeping track of their states, and committing successfully
 *     completed jobs. This is used only in single-node mode.
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
@Deprecated
public class LocalJobManager extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(LocalJobManager.class);

  private static final String JOB_MANAGER_KEY = "jobManager";
  private static final String PROPERTIES_KEY = "jobProps";
  private static final String JOB_LISTENER_KEY = "jobListener";

  private static final String TASK_STATE_STORE_TABLE_SUFFIX = ".tst";
  private static final String JOB_STATE_STORE_TABLE_SUFFIX = ".jst";

  // This is used to add newly generated work units
  private final WorkUnitManager workUnitManager;

  // Worker configuration properties
  private final Properties properties;

  // A Quartz scheduler
  private final Scheduler scheduler;

  // Mapping between jobs to job listeners associated with them
  private final Map<String, JobListener> jobListenerMap = Maps.newHashMap();

  // A map for all scheduled jobs
  private final Map<String, JobKey> scheduledJobs = Maps.newHashMap();

  // Mapping between jobs to the job locks they hold. This needs to be a
  // concurrent map because two scheduled runs of the same job (handled
  // by two separate threads) may access it concurrently.
  private final ConcurrentMap<String, JobLock> jobLockMap = Maps.newConcurrentMap();

  // Mapping between jobs to their job state objects
  private final Map<String, JobState> jobStateMap = Maps.newHashMap();

  // Mapping between jobs to the Source objects used to create work units
  private final Map<String, Source> jobSourceMap = Maps.newHashMap();

  // Mapping between jobs to the job IDs of their last runs
  private final Map<String, String> lastJobIdMap = Maps.newHashMap();

  // Set of supported job configuration file extensions
  private final Set<String> jobConfigFileExtensions;

  // Store for persisting job state
  private final StateStore jobStateStore;

  // Store for persisting task states
  private final StateStore taskStateStore;

  // A monitor for changes to job configuration files
  private final FileAlterationMonitor fileAlterationMonitor;

  public LocalJobManager(WorkUnitManager workUnitManager, Properties properties)
      throws Exception {

    this.workUnitManager = workUnitManager;
    this.properties = properties;
    this.scheduler = new StdSchedulerFactory().getScheduler();
    this.jobConfigFileExtensions = Sets.newHashSet(Splitter.on(",").omitEmptyStrings().split(this.properties
                .getProperty(ConfigurationKeys.JOB_CONFIG_FILE_EXTENSIONS_KEY,
                    ConfigurationKeys.DEFAULT_JOB_CONFIG_FILE_EXTENSIONS)));

    this.jobStateStore = new FsStateStore(properties.getProperty(ConfigurationKeys.STATE_STORE_FS_URI_KEY),
        properties.getProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY), JobState.class);
    this.taskStateStore = new FsStateStore(properties.getProperty(ConfigurationKeys.STATE_STORE_FS_URI_KEY),
        properties.getProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY), TaskState.class);

    long pollingInterval = Long.parseLong(this.properties.getProperty(
        ConfigurationKeys.JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL_KEY,
        Long.toString(ConfigurationKeys.DEFAULT_JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL)));
    this.fileAlterationMonitor = new FileAlterationMonitor(pollingInterval);

    restoreLastJobIdMap();
  }

  @Override
  protected void startUp()
      throws Exception {
    LOG.info("Starting the local job manager");
    this.scheduler.start();
    if (this.properties.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY)) {
      scheduleLocallyConfiguredJobs();
      startJobConfigFileMonitor();
    }
  }

  @Override
  protected void shutDown()
      throws Exception {
    LOG.info("Stopping the local job manager");
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
   * @param jobListener {@link JobListener} used for callback,
   *                    can be <em>null</em> if no callback is needed.
   * @throws gobblin.runtime.JobException when there is anything wrong
   *                      with scheduling the job
   */
  public void scheduleJob(Properties jobProps, JobListener jobListener)
      throws JobException {
    Preconditions.checkNotNull(jobProps);

    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);

    // Check if the job has been disabled
    boolean disabled = Boolean.valueOf(jobProps.getProperty(ConfigurationKeys.JOB_DISABLED_KEY, "false"));
    if (disabled) {
      LOG.info("Skipping disabled job " + jobName);
      return;
    }

    if (!jobProps.containsKey(ConfigurationKeys.JOB_SCHEDULE_KEY)) {
      // A job without a cron schedule is considered a one-time job
      jobProps.setProperty(ConfigurationKeys.JOB_RUN_ONCE_KEY, "true");
      // Run the job without going through the scheduler
      runJob(jobProps, jobListener);
      return;
    }

    if (jobListener != null) {
      this.jobListenerMap.put(jobName, jobListener);
    }

    // Build a data map that gets passed to the job
    JobDataMap jobDataMap = new JobDataMap();
    jobDataMap.put(JOB_MANAGER_KEY, this);
    jobDataMap.put(PROPERTIES_KEY, jobProps);
    jobDataMap.put(JOB_LISTENER_KEY, jobListener);

    // Build a Quartz job
    JobDetail job = JobBuilder.newJob(GobblinJob.class)
        .withIdentity(jobName, Strings.nullToEmpty(jobProps.getProperty(ConfigurationKeys.JOB_GROUP_KEY)))
        .withDescription(Strings.nullToEmpty(jobProps.getProperty(ConfigurationKeys.JOB_DESCRIPTION_KEY)))
        .usingJobData(jobDataMap).build();

    try {
      if (this.scheduler.checkExists(job.getKey())) {
        throw new JobException(String.format("Job %s has already been scheduled", jobName));
      }

      // Schedule the Quartz job with a trigger built from the job configuration
      this.scheduler.scheduleJob(job, getTrigger(job.getKey(), jobProps));
    } catch (SchedulerException se) {
      LOG.error("Failed to schedule job " + jobName, se);
      throw new JobException("Failed to schedule job " + jobName, se);
    }

    this.scheduledJobs.put(jobName, job.getKey());
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
  @SuppressWarnings("unchecked")
  public void runJob(Properties jobProps, JobListener jobListener)
      throws JobException {
    Preconditions.checkNotNull(jobProps);

    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    if (jobListener != null) {
      this.jobListenerMap.put(jobName, jobListener);
    }

    // Try acquiring a job lock before proceeding
    if (!acquireJobLock(jobName)) {
      LOG.info(String.format("Previous instance of job %s is still running, skipping this scheduled run", jobName));
      return;
    }

    // If this is a run-once job
    boolean runOnce = Boolean.valueOf(jobProps.getProperty(ConfigurationKeys.JOB_RUN_ONCE_KEY, "false"));

    String jobId = JobLauncherUtils.newJobId(jobName);
    JobState jobState = new JobState(jobName, jobId);
    // Add all job configuration properties of this job
    jobState.addAll(jobProps);
    jobState.setState(JobState.RunningState.PENDING);

    LOG.info("Starting job " + jobId);

    Optional<SourceState> sourceStateOptional = Optional.absent();
    try {
      // Initialize the source
      SourceState sourceState = new SourceState(jobState, getPreviousWorkUnitStates(jobName));
      Source<?, ?> sourceInstance =
          (Source<?, ?>) Class.forName(jobProps.getProperty(ConfigurationKeys.SOURCE_CLASS_KEY)).newInstance();
      Source<?, ?> source = new SourceDecorator(sourceInstance, jobId, LOG);
      sourceStateOptional = Optional.of(sourceState);

      // Generate work units based on all previous work unit states
      Optional<List<WorkUnit>> workUnits = Optional.fromNullable(source.getWorkunits(sourceState));
      if (!workUnits.isPresent()) {
        // The absence means there is something wrong getting the work units
        source.shutdown(sourceState);
        unlockJob(jobName, runOnce);
        throw new JobException("Failed to get work units for job " + jobId);
      }

      if (workUnits.get().isEmpty()) {
        // No real work to do
        LOG.warn("No work units have been created for job " + jobId);
        source.shutdown(sourceState);
        unlockJob(jobName, runOnce);
        callJobListener(jobName, jobState, runOnce);
        return;
      }

      jobState.setTasks(workUnits.get().size());
      jobState.setStartTime(System.currentTimeMillis());
      jobState.setState(JobState.RunningState.RUNNING);

      this.jobStateMap.put(jobId, jobState);
      this.jobSourceMap.put(jobId, source);

      // Add all generated work units
      int sequence = 0;
      for (WorkUnit workUnit : workUnits.get()) {
        String taskId = JobLauncherUtils.newTaskId(jobId, sequence++);
        workUnit.setId(taskId);
        WorkUnitState workUnitState = new WorkUnitState(workUnit);
        workUnitState.setId(taskId);
        workUnitState.setProp(ConfigurationKeys.JOB_ID_KEY, jobId);
        workUnitState.setProp(ConfigurationKeys.TASK_ID_KEY, taskId);
        this.workUnitManager.addWorkUnit(workUnitState);
      }
    } catch (Throwable t) {
      String errMsg = "Failed to run job " + jobId;
      LOG.error(errMsg, t);
      // Shutdown the source if the source has already been initialized
      if (this.jobSourceMap.containsKey(jobId) && sourceStateOptional.isPresent()) {
        this.jobSourceMap.remove(jobId).shutdown(sourceStateOptional.get());
      }
      // Remove the cached job state
      this.jobStateMap.remove(jobId);
      // Finally release the job lock
      unlockJob(jobName, runOnce);
      throw new JobException(errMsg, t);
    }
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
   * Get the names of the scheduled jobs.
   *
   * @return names of the scheduled jobs
   */
  public Collection<String> getScheduledJobs() {
    return this.scheduledJobs.keySet();
  }

  /**
   * Callback method when a task is completed.
   *
   * @param jobId Job ID of the given job
   * @param taskState {@link TaskState}
   */
  public synchronized void onTaskCompletion(String jobId, TaskState taskState) {
    if (!this.jobStateMap.containsKey(jobId)) {
      LOG.error(String.format("Job %s could not be found", jobId));
      return;
    }

    if (JobMetrics.isEnabled(this.properties)) {
      // Remove all task-level metrics after the task is done
      taskState.removeMetrics();
    }

    JobState jobState = this.jobStateMap.get(jobId);
    jobState.addTaskState(taskState);
    // If all the tasks of the job have completed (regardless of
    // success or failure), then trigger job committing.
    if (jobState.getCompletedTasks() == jobState.getTasks()) {
      LOG.info(String.format("All tasks of job %s have completed, committing it", jobId));
      String jobName = taskState.getWorkunit().getProp(ConfigurationKeys.JOB_NAME_KEY);
      try {
        commitJob(jobId, jobName, getFinalJobState(jobState));
      } catch (Throwable t) {
        LOG.error("Failed to commit job " + jobId, t);
      }
    }
  }

  /**
   * Restore the lastJobIdMap.
   */
  private void restoreLastJobIdMap()
      throws IOException {
    FileSystem fs = FileSystem
        .get(URI.create(this.properties.getProperty(ConfigurationKeys.STATE_STORE_FS_URI_KEY)), new Configuration());

    // Root directory of task states store
    Path taskStateStoreRootDir = new Path(this.properties.getProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY));
    if (!fs.exists(taskStateStoreRootDir)) {
      return;
    }

    // List subdirectories (one for each job) under the root directory
    FileStatus[] rootStatuses = fs.listStatus(taskStateStoreRootDir);
    if (rootStatuses == null || rootStatuses.length == 0) {
      return;
    }

    LOG.info("Restoring the mapping between jobs and IDs of their last runs");

    for (FileStatus status : rootStatuses) {
      // List the task states files under each subdirectory corresponding to a job
      FileStatus[] statuses = fs.listStatus(status.getPath(), new PathFilter() {
        @Override
        public boolean accept(Path path) {
          return !path.getName().startsWith("current") && path.getName().endsWith(TASK_STATE_STORE_TABLE_SUFFIX);
        }
      });

      if (statuses == null || statuses.length == 0) {
        continue;
      }

      // Sort the task states files by timestamp in descending order
      Arrays.sort(statuses, new Comparator<FileStatus>() {
        @Override
        public int compare(FileStatus fileStatus1, FileStatus fileStatus2) {
          String fileName1 = fileStatus1.getPath().getName();
          String taskId1 = fileName1.substring(0, fileName1.indexOf('.'));
          String fileName2 = fileStatus2.getPath().getName();
          String taskId2 = fileName2.substring(0, fileName2.indexOf('.'));

          Long ts1 = Long.parseLong(taskId1.substring(taskId1.lastIndexOf('_') + 1));
          Long ts2 = Long.parseLong(taskId2.substring(taskId2.lastIndexOf('_') + 1));

          return -ts1.compareTo(ts2);
        }
      });

      // Each subdirectory is for one job, and the directory name is the job name.
      String jobName = status.getPath().getName();
      // The first task states file after sorting has the latest timestamp
      String fileName = statuses[0].getPath().getName();
      String lastJobId = fileName.substring(0, fileName.indexOf('.'));
      LOG.info(String.format("Restored last job ID %s for job %s", lastJobId, jobName));
      this.lastJobIdMap.put(jobName, lastJobId);
    }
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
   *     The job configuration file monitor currently only supports monitoring
   *     newly added job configuration files.
   * </p>
   */
  private void startJobConfigFileMonitor()
      throws Exception {
    File jobConfigFileDir = new File(this.properties.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY));
    FileAlterationObserver observer = new FileAlterationObserver(jobConfigFileDir);
    FileAlterationListener listener = new FileAlterationListenerAdaptor() {
      /**
       * Called when a new job configuration file is dropped in.
       */
      @Override
      public void onFileCreate(File file) {
        int pos = file.getName().lastIndexOf(".");
        String fileExtension = pos >= 0 ? file.getName().substring(pos + 1) : "";
        if (!jobConfigFileExtensions.contains(fileExtension)) {
          // Not a job configuration file, ignore.
          return;
        }

        LOG.info("Detected new job configuration file " + file.getAbsolutePath());
        Properties jobProps = new Properties();
        // First add framework configuration properties
        jobProps.putAll(properties);
        // Then load job configuration properties from the new job configuration file
        loadJobConfig(jobProps, file);

        // Schedule the new job
        try {
          boolean runOnce = Boolean.valueOf(jobProps.getProperty(ConfigurationKeys.JOB_RUN_ONCE_KEY, "false"));
          scheduleJob(jobProps, runOnce ? new RunOnceJobListener() : new EmailNotificationJobListener());
        } catch (Throwable t) {
          LOG.error("Failed to schedule new job loaded from job configuration file " + file.getAbsolutePath(), t);
        }
      }

      /**
       * Called when a job configuration file is changed.
       */
      @Override
      public void onFileChange(File file) {
        int pos = file.getName().lastIndexOf(".");
        String fileExtension = pos >= 0 ? file.getName().substring(pos + 1) : "";
        if (!jobConfigFileExtensions.contains(fileExtension)) {
          // Not a job configuration file, ignore.
          return;
        }

        LOG.info("Detected change to job configuration file " + file.getAbsolutePath());
        Properties jobProps = new Properties();
        // First add framework configuration properties
        jobProps.putAll(properties);
        // Then load the updated job configuration properties
        loadJobConfig(jobProps, file);

        String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
        try {
          // First unschedule and delete the old job
          unscheduleJob(jobName);
          boolean runOnce = Boolean.valueOf(jobProps.getProperty(ConfigurationKeys.JOB_RUN_ONCE_KEY, "false"));
          // Reschedule the job with the new job configuration
          scheduleJob(jobProps, runOnce ? new RunOnceJobListener() : new EmailNotificationJobListener());
        } catch (Throwable t) {
          LOG.error("Failed to update existing job " + jobName, t);
        }
      }

      private void loadJobConfig(Properties jobProps, File file) {
        try {
          jobProps.load(new InputStreamReader(new FileInputStream(file), Charset.forName(
              ConfigurationKeys.DEFAULT_CHARSET_ENCODING)));
          jobProps.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY, file.getAbsolutePath());
        } catch (Exception e) {
          LOG.error("Failed to load job configuration from file " + file.getAbsolutePath(), e);
        }
      }
    };

    observer.addListener(listener);
    this.fileAlterationMonitor.addObserver(observer);
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
   * Try acquiring the job lock and return whether the lock is successfully locked.
   */
  private boolean acquireJobLock(String jobName) {
    try {
      this.jobLockMap.putIfAbsent(jobName, new LocalJobLock());
      JobLock lock = this.jobLockMap.get(jobName);
      return lock.tryLock();
    } catch (IOException ioe) {
      LOG.error("Failed to acquire the job lock for job " + jobName, ioe);
      return false;
    }
  }

  /**
   * Unlock a completed or failed job.
   */
  private void unlockJob(String jobName, boolean runOnce) {
    try {
      if (runOnce) {
        // Unlock so the next run of the same job can proceed
        this.jobLockMap.remove(jobName).unlock();
      } else {
        // Unlock and remove the lock as it is no longer needed
        this.jobLockMap.get(jobName).unlock();
      }
    } catch (IOException ioe) {
      LOG.error("Failed to unlock for job " + jobName, ioe);
    }
  }

  /**
   * Commit a finished job.
   */
  @SuppressWarnings("unchecked")
  private void commitJob(String jobId, String jobName, JobState jobState)
      throws Exception {
    JobCommitPolicy commitPolicy = JobCommitPolicy.forName(
        jobState.getProp(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, ConfigurationKeys.DEFAULT_JOB_COMMIT_POLICY));

    DataPublisher publisher = null;
    try {
      // Do job publishing based on the job commit policy
      if (commitPolicy == JobCommitPolicy.COMMIT_ON_PARTIAL_SUCCESS || (
          commitPolicy == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS
              && jobState.getState() == JobState.RunningState.SUCCESSFUL)) {

        LOG.info("Publishing job data of job " + jobId + " with commit policy " + commitPolicy);

        Class<? extends DataPublisher> dataPublisherClass =
            (Class<? extends DataPublisher>) Class.forName(jobState.getProp(ConfigurationKeys.DATA_PUBLISHER_TYPE,
                ConfigurationKeys.DEFAULT_DATA_PUBLISHER_TYPE));
        Constructor<? extends DataPublisher> dataPublisherConstructor =
            dataPublisherClass.getConstructor(gobblin.configuration.State.class);
        publisher = dataPublisherConstructor.newInstance(jobState);

        publisher.initialize();
        publisher.publish(jobState.getTaskStates());
        jobState.setState(JobState.RunningState.COMMITTED);
      } else {
        LOG.info("Job data will not be committed due to commit policy: " + commitPolicy);
      }
    } catch (Exception e) {
      jobState.setState(JobState.RunningState.FAILED);
      LOG.error("Failed to publish job data of job " + jobId, e);
      throw e;
    } finally {
      if (JobMetrics.isEnabled(this.properties)) {
        // Remove all job-level metrics after the job is done
        jobState.removeMetrics();
      }
      boolean runOnce = Boolean.valueOf(jobState.getProp(ConfigurationKeys.JOB_RUN_ONCE_KEY, "false"));
      persistJobState(jobState);
      cleanupJobOnCompletion(jobState, runOnce);
      unlockJob(jobName, runOnce);
      callJobListener(jobName, jobState, runOnce);
      if (publisher != null) {
        publisher.close();
      }
    }
  }

  /**
   * Build a {@link JobState} object capturing the state of the given job.
   */
  private JobState getFinalJobState(JobState jobState) {
    jobState.setEndTime(System.currentTimeMillis());
    jobState.setDuration(jobState.getEndTime() - jobState.getStartTime());
    jobState.setState(JobState.RunningState.SUCCESSFUL);

    for (TaskState taskState : jobState.getTaskStates()) {
      // The job is considered failed if any task failed
      if (taskState.getWorkingState() == WorkUnitState.WorkingState.FAILED) {
        jobState.setState(JobState.RunningState.FAILED);
        break;
      }

      // The job is considered cancelled if any task is cancelled
      if (taskState.getWorkingState() == WorkUnitState.WorkingState.CANCELLED) {
        jobState.setState(JobState.RunningState.CANCELLED);
        break;
      }
    }

    return jobState;
  }

  /**
   * Persist job/task states of a completed job.
   */
  private void persistJobState(JobState jobState) {
    JobState.RunningState runningState = jobState.getState();
    if (runningState == JobState.RunningState.PENDING ||
        runningState == JobState.RunningState.RUNNING ||
        runningState == JobState.RunningState.CANCELLED) {
      // Do not persist job state if the job has not completed
      return;
    }

    String jobName = jobState.getJobName();
    String jobId = jobState.getJobId();

    LOG.info("Persisting job/task states of job " + jobId);
    try {
      this.taskStateStore.putAll(jobName, jobId + TASK_STATE_STORE_TABLE_SUFFIX, jobState.getTaskStates());
      this.jobStateStore.put(jobName, jobId + JOB_STATE_STORE_TABLE_SUFFIX, jobState);
    } catch (IOException ioe) {
      LOG.error("Failed to persist job/task states of job " + jobId, ioe);
    }
  }

  /**
   * Cleanup a job upon its completion (successful or failed).
   */
  private void cleanupJobOnCompletion(JobState jobState, boolean runOnce) {
    String jobName = jobState.getJobName();
    String jobId = jobState.getJobId();

    LOG.info("Performing cleanup for job " + jobId);

    if (this.jobSourceMap.containsKey(jobId)) {
      // Remove all state bookkeeping information of this scheduled job run
      this.jobSourceMap.remove(jobId).shutdown(jobState);
    }
    this.jobStateMap.remove(jobId);

    try {
      if (runOnce && this.scheduledJobs.containsKey(jobName)) {
        // Delete the job from the Quartz scheduler
        this.scheduler.deleteJob(this.scheduledJobs.remove(jobName));
      } else {
        // Remember the job ID of this most recent run of the job
        this.lastJobIdMap.put(jobName, jobId);
      }
    } catch (Exception e) {
      LOG.error("Failed to cleanup job " + jobId, e);
    }
  }

  /**
   * Get work unit states of the most recent run of the job.
   */
  @SuppressWarnings("unchecked")
  private List<WorkUnitState> getPreviousWorkUnitStates(String jobName)
      throws IOException {

    // This is the first run of the job
    if (!this.lastJobIdMap.containsKey(jobName)) {
      return Lists.newArrayList();
    }

    LOG.info("Loading task states of the most recent run of job " + jobName);
    // Read the task states of the most recent run of the job
    return (List<WorkUnitState>) this.taskStateStore
        .getAll(jobName, this.lastJobIdMap.get(jobName) + TASK_STATE_STORE_TABLE_SUFFIX);
  }

  /**
   * Call {@link JobListener} associated with the job.
   */
  private void callJobListener(String jobName, JobState jobState, boolean runOnce) {
    JobListener jobListener = runOnce ? this.jobListenerMap.remove(jobName) : this.jobListenerMap.get(jobName);
    if (jobListener != null) {
      jobListener.jobCompleted(jobState);
    }
  }

  /**
   * A Gobblin job to schedule locally.
   */
  @DisallowConcurrentExecution
  public static class GobblinJob implements Job {

    @Override
    public void execute(JobExecutionContext context)
        throws JobExecutionException {
      JobDataMap dataMap = context.getJobDetail().getJobDataMap();
      LocalJobManager jobManager = (LocalJobManager) dataMap.get(JOB_MANAGER_KEY);
      Properties jobProps = (Properties) dataMap.get(PROPERTIES_KEY);
      JobListener jobListener = (JobListener) dataMap.get(JOB_LISTENER_KEY);

      try {
        jobManager.runJob(jobProps, jobListener);
      } catch (Throwable t) {
        throw new JobExecutionException(t);
      }
    }
  }
}
