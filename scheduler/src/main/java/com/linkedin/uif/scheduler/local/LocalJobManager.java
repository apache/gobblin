package com.linkedin.uif.scheduler.local;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

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

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.metastore.FsStateStore;
import com.linkedin.uif.metastore.StateStore;
import com.linkedin.uif.publisher.DataPublisher;
import com.linkedin.uif.scheduler.JobCommitPolicy;
import com.linkedin.uif.scheduler.JobException;
import com.linkedin.uif.scheduler.JobListener;
import com.linkedin.uif.scheduler.JobLock;
import com.linkedin.uif.scheduler.JobState;
import com.linkedin.uif.scheduler.Metrics;
import com.linkedin.uif.scheduler.RunOnceJobListener;
import com.linkedin.uif.scheduler.SourceWrapperBase;
import com.linkedin.uif.scheduler.TaskState;
import com.linkedin.uif.scheduler.WorkUnitManager;
import com.linkedin.uif.source.Source;
import com.linkedin.uif.source.workunit.WorkUnit;

/**
 * A class for managing locally configured UIF jobs.
 *
 * <p>
 *     This class's responsibilities include scheduling and running locally configured
 *     UIF jobs to run, keeping track of their states, and committing successfully
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
public class LocalJobManager extends AbstractIdleService {

    private static final Logger LOG = LoggerFactory.getLogger(LocalJobManager.class);

    private static final String JOB_CONFIG_FILE_EXTENSION = ".pull";
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

    // A map for remembering run-once jobs
    private final Map<String, JobKey> runOnceJobs = Maps.newHashMap();

    // A map for remembering config file paths of run-once jobs
    private final Map<String, String> runOnceJobConfigFiles = Maps.newHashMap();

    // Mapping between Source wrapper keys and Source wrapper classes
    private final Map<String, Class<SourceWrapperBase>> sourceWrapperMap = Maps.newHashMap();

    // Mapping between jobs to the job locks they hold. This needs to be a
    // concurrent map because two scheduled runs of the same job (handled
    // by two separate threds) may access it concurrently.
    private final ConcurrentMap<String, JobLock> jobLockMap = Maps.newConcurrentMap();

    // Mapping between jobs to their job state objects
    private final Map<String, JobState> jobStateMap = Maps.newHashMap();

    // Mapping between jobs to the Source objects used to create work units
    private final Map<String, Source> jobSourceMap = Maps.newHashMap();

    // Mapping between jobs to the job IDs of their last runs
    private final Map<String, String> lastJobIdMap = Maps.newHashMap();

    // Store for persisting job state
    private final StateStore jobStateStore;

    // Store for persisting task states
    private final StateStore taskStateStore;

    // A monitor for changes to job configuration files
    private FileAlterationMonitor fileAlterationMonitor;

    public LocalJobManager(WorkUnitManager workUnitManager, Properties properties)
            throws Exception {

        this.workUnitManager = workUnitManager;
        this.properties = properties;
        this.scheduler = new StdSchedulerFactory().getScheduler();

        this.jobStateStore = new FsStateStore(
                properties.getProperty(ConfigurationKeys.STATE_STORE_FS_URI_KEY),
                properties.getProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY),
                JobState.class);
        this.taskStateStore = new FsStateStore(
                properties.getProperty(ConfigurationKeys.STATE_STORE_FS_URI_KEY),
                properties.getProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY),
                TaskState.class);

        restoreLastJobIdMap();
        populateSourceWrapperMap();
    }

    @Override
    protected void startUp() throws Exception {
        LOG.info("Starting the local job manager");
        this.scheduler.start();
        if (this.properties.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY)) {
            scheduleLocallyConfiguredJobs();
            startJobConfigFileMonitor();
        }
    }

    @Override
    protected void shutDown() throws Exception {
        LOG.info("Stopping the local job manager");
        this.scheduler.shutdown(true);
        this.fileAlterationMonitor.stop();
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
     * @throws JobException when there is anything wrong
     *                      with scheduling the job
     */
    public void scheduleJob(Properties jobProps, JobListener jobListener) throws JobException {
        Preconditions.checkNotNull(jobProps);

        String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);

        // Check if the job has been disabled
        boolean disabled = Boolean.valueOf(
                jobProps.getProperty(ConfigurationKeys.JOB_DISABLED_KEY, "false"));
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

        // If this is a run-once job
        boolean runOnce = Boolean.valueOf(
                jobProps.getProperty(ConfigurationKeys.JOB_RUN_ONCE_KEY, "false"));

        // Build a data map that gets passed to the job
        JobDataMap jobDataMap = new JobDataMap();
        jobDataMap.put(JOB_MANAGER_KEY, this);
        jobDataMap.put(PROPERTIES_KEY, jobProps);
        jobDataMap.put(JOB_LISTENER_KEY, jobListener);

        // Build a Quartz job
        JobDetail job = JobBuilder.newJob(UIFJob.class)
                .withIdentity(jobName, Strings.nullToEmpty(jobProps.getProperty(
                        ConfigurationKeys.JOB_GROUP_KEY)))
                .withDescription(Strings.nullToEmpty(jobProps.getProperty(
                        ConfigurationKeys.JOB_DESCRIPTION_KEY)))
                .usingJobData(jobDataMap)
                .build();

        try {
            // Schedule the Quartz job with a trigger built from the job configuration
            this.scheduler.scheduleJob(job, getTrigger(job.getKey(), jobProps));
        } catch (SchedulerException se) {
            LOG.error("Failed to schedule job " + jobName, se);
            throw new JobException("Failed to schedule job " + jobName, se);
        }

        this.scheduledJobs.put(jobName, job.getKey());

        // If the job should run only once, remember so it can be deleted after its
        // single run is done.
        if (runOnce) {
            this.runOnceJobs.put(jobName, job.getKey());
            this.runOnceJobConfigFiles.put(jobName,
                    jobProps.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY));
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
    public void runJob(Properties jobProps, JobListener jobListener) throws JobException {
        Preconditions.checkNotNull(jobProps);

        String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
        if (jobListener != null) {
            this.jobListenerMap.put(jobName, jobListener);
        }

        // Try acquiring a job lock before proceeding
        if (!acquireJobLock(jobName)) {
            LOG.info(String.format(
                    "Previous instance of job %s is still running, skipping this scheduled run",
                    jobName));
            return;
        }

        // If this is a run-once job
        boolean runOnce = Boolean.valueOf(
                jobProps.getProperty(ConfigurationKeys.JOB_RUN_ONCE_KEY, "false"));

        // Job ID in the form of job_<job_id_suffix>
        // <job_id_suffix> is in the form of <job_name>_<current_timestamp>
        String jobIdSuffix = String.format("%s_%d", jobName, System.currentTimeMillis());
        String jobId = "job_" + jobIdSuffix;

        JobState jobState = new JobState(jobName, jobId);
        // Add all job configuration properties of this job
        jobState.addAll(jobProps);

        LOG.info("Starting job " + jobId);

        SourceWrapperBase source;
        SourceState sourceState;
        try {
            source = this.sourceWrapperMap.get(jobProps.getProperty(
                    ConfigurationKeys.SOURCE_WRAPPER_CLASS_KEY,
                    ConfigurationKeys.DEFAULT_SOURCE_WRAPPER)
                    .toLowerCase()).newInstance();
            sourceState = new SourceState(jobState, getPreviousWorkUnitStates(jobName));
            source.init(sourceState);
        } catch (Exception e) {
            LOG.error("Failed to instantiate source for job " + jobId, e);
            unlockJob(jobName, runOnce);
            throw new JobException("Failed to run job " + jobId, e);
        }

        // Generate work units based on all previous work unit states
        List<WorkUnit> workUnits = source.getWorkunits(sourceState);
        // If no real work to do
        if (workUnits == null || workUnits.isEmpty()) {
            LOG.warn("No work units have been created for job " + jobId);
            source.shutdown(jobState);
            unlockJob(jobName, runOnce);
            callJobListener(jobName, jobState, runOnce);
            return;
        }

        jobState.setTasks(workUnits.size());
        jobState.setStartTime(System.currentTimeMillis());
        jobState.setState(JobState.RunningState.WORKING);

        this.jobStateMap.put(jobId, jobState);
        this.jobSourceMap.put(jobId, source);

        // Add all generated work units
        int sequence = 0;
        for (WorkUnit workUnit : workUnits) {
            // Task ID in the form of task_<job_id_suffix>_<task_sequence_number>
            String taskId = String.format("task_%s_%d", jobIdSuffix, sequence++);
            workUnit.setId(taskId);
            WorkUnitState workUnitState = new WorkUnitState(workUnit);
            workUnitState.setId(taskId);
            workUnitState.setProp(ConfigurationKeys.JOB_ID_KEY, jobId);
            workUnitState.setProp(ConfigurationKeys.TASK_ID_KEY, taskId);
            this.workUnitManager.addWorkUnit(workUnitState);
        }
    }

    /**
     * Unschedule and delete a job.
     *
     * @param jobName Job name
     * @throws JobException when there is anything wrong unscheduling the job
     */
    public void unscheduleJob(String jobName) throws JobException {
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

        if (Metrics.isEnabled(this.properties)) {
            // Remove all task-level metrics after the task is done
            taskState.removeMetrics();
        }

        JobState jobState = this.jobStateMap.get(jobId);
        jobState.addTaskState(taskState);
        // If all the tasks of the job have completed (regardless of
        // success or failure), then trigger job committing.
        if (jobState.getCompletedTasks() == jobState.getTasks()) {
            LOG.info(String.format(
                    "All tasks of job %s have completed, committing it", jobId));
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
    private void restoreLastJobIdMap() throws IOException {
        FileSystem fs = FileSystem.get(
                URI.create(this.properties.getProperty(ConfigurationKeys.STATE_STORE_FS_URI_KEY)),
                new Configuration());
        // Root directory of task states store
        Path taskStateStoreRootDir = new Path(
                this.properties.getProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY));

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
                    return path.getName().endsWith(TASK_STATE_STORE_TABLE_SUFFIX);
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
     * Schedule locally configured UIF jobs.
     */
    private void scheduleLocallyConfiguredJobs() throws JobException {
        LOG.info("Scheduling locally configured jobs");
        for (Properties jobProps : loadLocalJobConfigs()) {
            boolean runOnce = Boolean.valueOf(jobProps.getProperty(
                    ConfigurationKeys.JOB_RUN_ONCE_KEY, "false"));
            scheduleJob(jobProps, runOnce ? new RunOnceJobListener() : null);
        }
    }

    /**
     * Load local job configurations.
     */
    private List<Properties> loadLocalJobConfigs() {
        File jobConfigFileDir = new File(this.properties.getProperty(
                ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY));
        // Find all job configuration files
        String[] jobConfigFiles = jobConfigFileDir.list(new FilenameFilter() {

            @Override
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(JOB_CONFIG_FILE_EXTENSION);
            }
        });

        List<Properties> jobConfigs = Lists.newArrayList();
        if (jobConfigFiles == null) {
            LOG.info("No job configuration files found");
            return jobConfigs;
        }

        LOG.info("Loading job configurations from directory " + jobConfigFileDir);
        // Load all job configurations
        for (String jobConfigFile : jobConfigFiles) {
            Properties jobProps = new Properties();
            jobProps.putAll(this.properties);
            try {
                File file = new File(jobConfigFileDir, jobConfigFile);
                jobProps.load(new FileReader(file));
                jobProps.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY,
                        file.getAbsolutePath());
                jobConfigs.add(jobProps);
            } catch (FileNotFoundException fnfe) {
                LOG.error("Job configuration file " + jobConfigFile + " not found", fnfe);
            } catch (IOException ioe) {
                LOG.error("Failed to load job configuration from file " + jobConfigFile, ioe);
            }
        }

        LOG.info(String.format(
                jobConfigs.size() <= 1 ?
                        "Loaded %d job configuration" :
                        "Loaded %d job configurations",
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
    private void startJobConfigFileMonitor() throws Exception {
        File jobConfigFileDir = new File(this.properties.getProperty(
                ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY));
        long pollingInterval = Long.parseLong(this.properties.getProperty(
                ConfigurationKeys.JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL_KEY,
                ConfigurationKeys.DEFAULT_JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL));

        FileAlterationObserver observer = new FileAlterationObserver(jobConfigFileDir);
        this.fileAlterationMonitor = new FileAlterationMonitor(pollingInterval);
        FileAlterationListener listener = new FileAlterationListenerAdaptor() {
            /**
             * Called when a new job configuration file is dropped in.
             */
            @Override
            public void onFileCreate(File file) {
                if (!file.getName().endsWith(JOB_CONFIG_FILE_EXTENSION)) {
                    // Not a job configuration file, ignore.
                    return;
                }

                LOG.info("Detected new job configuration file " + file.getAbsolutePath());
                // Load job configuration from the new job configuration file
                Properties jobProps = loadJobConfig(file);
                if (jobProps == null) {
                    return;
                }

                jobProps.putAll(properties);
                jobProps.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY,
                        file.getAbsolutePath());
                // Schedule the new job
                try {
                    boolean runOnce = Boolean.valueOf(
                            jobProps.getProperty(ConfigurationKeys.JOB_RUN_ONCE_KEY, "false"));
                    scheduleJob(jobProps, runOnce ? new RunOnceJobListener() : null);
                } catch (Throwable t) {
                    LOG.error(
                            "Failed to schedule new job loaded from job configuration file " +
                                    file.getAbsolutePath(),
                            t);
                }
            }

            /**
             * Called when a job configuration file is changed.
             */
            @Override
            public void onFileChange(File file) {
                if (!file.getName().endsWith(JOB_CONFIG_FILE_EXTENSION)) {
                    // Not a job configuration file, ignore.
                    return;
                }

                LOG.info("Detected change to job configuration file " + file.getAbsolutePath());
                Properties jobProps = loadJobConfig(file);
                if (jobProps == null) {
                    return;
                }

                jobProps.putAll(properties);
                String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
                try {
                    // First unschedule and delete the old job
                    unscheduleJob(jobName);
                    boolean runOnce = Boolean.valueOf(
                            jobProps.getProperty(ConfigurationKeys.JOB_RUN_ONCE_KEY, "false"));
                    // Reschedule the job with the new job configuration
                    scheduleJob(jobProps, runOnce ? new RunOnceJobListener() : null);
                } catch (Throwable t) {
                    LOG.error("Failed to update existing job " + jobName, t);
                }
            }

            private Properties loadJobConfig(File file) {
                // Load job configuration from the new job configuration file
                Properties jobProps = new Properties();
                try {
                    jobProps.load(new FileReader(file));
                } catch (Exception e) {
                    LOG.error("Failed to load job configuration from file " + file.getAbsolutePath());
                    return null;
                }

                return jobProps;
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
        return TriggerBuilder.newTrigger()
                .withIdentity(
                        jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY),
                        Strings.nullToEmpty(jobProps.getProperty(
                                ConfigurationKeys.JOB_GROUP_KEY)))
                .forJob(jobKey)
                .withSchedule(CronScheduleBuilder.cronSchedule(
                        jobProps.getProperty(ConfigurationKeys.JOB_SCHEDULE_KEY)))
                .build();
    }

    /**
     * Try acquring the job lock and return whether the lock is successfully locked.
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
    private void commitJob(String jobId, String jobName, JobState jobState) throws Exception {
        JobCommitPolicy commitPolicy = JobCommitPolicy.forName(jobState.getProp(
                ConfigurationKeys.JOB_COMMIT_POLICY_KEY,
                ConfigurationKeys.DEFAULT_JOB_COMMIT_POLICY));

        DataPublisher publisher = null;
        try {
            // Do job publishing based on the job commit policy
            if (commitPolicy == JobCommitPolicy.COMMIT_ON_PARTIAL_SUCCESS ||
                    (commitPolicy == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS &&
                            jobState.getState() == JobState.RunningState.SUCCESSFUL)) {

                LOG.info("Publishing job data of job " + jobId + " with commit policy " + commitPolicy);

                Class<? extends DataPublisher> dataPublisherClass = (Class<? extends DataPublisher>)
                        Class.forName(jobState.getProp(ConfigurationKeys.DATA_PUBLISHER_TYPE));
                Constructor<? extends DataPublisher> dataPublisherConstructor =
                        dataPublisherClass.getConstructor(com.linkedin.uif.configuration.State.class);
                publisher = dataPublisherConstructor.newInstance(jobState);

                publisher.initialize();
                if (publisher.publish(jobState.getTaskStates())) {
                   jobState.setState(JobState.RunningState.COMMITTED);
                }
            } else {
                LOG.info("Job data will not be committed due to commit policy: " + commitPolicy);
            }
        } catch (Exception e) {
            jobState.setState(JobState.RunningState.FAILED);
            LOG.error("Failed to publish job data of job " + jobId, e);
            throw e;
        } finally {
            if (Metrics.isEnabled(this.properties)) {
                // Remove all job-level metrics after the job is done
                jobState.removeMetrics();
            }
            boolean runOnce = this.runOnceJobs.containsKey(jobName);
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

            // The job is considered aborted if any task is aborted
            if (taskState.getWorkingState() == WorkUnitState.WorkingState.ABORTED) {
                jobState.setState(JobState.RunningState.ABORTED);
                break;
            }
        }

        return jobState;
    }

    /**
     * Persiste job/task states of a completed job.
     */
    private void persistJobState(JobState jobState) {
        String jobName = jobState.getJobName();
        String jobId = jobState.getJobId();

        LOG.info("Persisting job/task states of job " + jobId);
        try {
            this.taskStateStore.putAll(
                    jobName, jobId + TASK_STATE_STORE_TABLE_SUFFIX, jobState.getTaskStates());
            this.jobStateStore.put(jobName, jobId + JOB_STATE_STORE_TABLE_SUFFIX,
                    jobState);
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
            if (runOnce) {
                this.scheduledJobs.remove(jobName);
                if (this.runOnceJobs.containsKey(jobName)) {
                    // Delete the job from the Quartz scheduler
                    this.scheduler.deleteJob(this.runOnceJobs.remove(jobName));
                }
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
    private List<WorkUnitState> getPreviousWorkUnitStates(String jobName) throws IOException {

        // This is the first run of the job
        if (!this.lastJobIdMap.containsKey(jobName)) {
            return Lists.newArrayList();
        }

        LOG.info("Loading task states of the most recent run of job " + jobName);
        // Read the task states of the most recent run of the job
        return (List<WorkUnitState>) this.taskStateStore.getAll(
                jobName, this.lastJobIdMap.get(jobName) + TASK_STATE_STORE_TABLE_SUFFIX);
    }

    /**
     * Call {@link JobListener} associated with the job.
     */
    private void callJobListener(String jobName, JobState jobState, boolean runOnce) {
        JobListener jobListener = runOnce ?
                this.jobListenerMap.remove(jobName) : this.jobListenerMap.get(jobName);
        if (jobListener != null) {
            jobListener.jobCompleted(jobState);
        }
    }

    /**
     * Populates map of String keys to {@link SourceWrapperBase} classes.
     */
    @SuppressWarnings("unchecked")
    private void populateSourceWrapperMap() throws ClassNotFoundException {
        // Default must be defined, but properties can overwrite if needed.
        this.sourceWrapperMap.put(ConfigurationKeys.DEFAULT_SOURCE_WRAPPER,
                SourceWrapperBase.class);

        String propStr = this.properties.getProperty(
                ConfigurationKeys.SOURCE_WRAPPERS,
                "default:" + SourceWrapperBase.class.getName());
        for (String entry : Splitter.on(";").trimResults().split(propStr)) {
            List<String> tokens = Splitter.on(":").trimResults().splitToList(entry);
            this.sourceWrapperMap.put(
                    tokens.get(0).toLowerCase(),
                    (Class<SourceWrapperBase>) Class.forName(tokens.get(1)));
        }
    }

    /**
     * A UIF job to schedule locally.
     */
    @DisallowConcurrentExecution
    public static class UIFJob implements Job {

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
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
