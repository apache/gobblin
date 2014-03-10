package com.linkedin.uif.scheduler.local;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import com.linkedin.uif.scheduler.JobListener;
import com.linkedin.uif.scheduler.JobLock;
import com.linkedin.uif.scheduler.JobState;
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
 *     This class's responsibilities include scheduling locally configured UIF
 *     jobs to run, keeping track of their states, and committing successfully
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

    private static final Log LOG = LogFactory.getLog(LocalJobManager.class);

    private static final String JOB_CONFIG_FILE_EXTENSION = ".pull";
    private static final String PROPERTIES_KEY = "properties";
    private static final String WORK_UNIT_MANAGER_KEY = "workUnitManager";
    private static final String JOB_LOCK_MAP_KEY = "jobLockMap";
    private static final String JOB_STATE_MAP_KEY = "jobStateMap";
    private static final String JOB_SOURCE_MAP_KEY = "jobSourceMap";
    private static final String SOURCE_WRAPPER_MAP_KEY = "sourceWrapperMap";
    private static final String LAST_JOB_ID_MAP_KEY = "lastJobIdMap";
    private static final String TASK_STATE_STORE_KEY = "taskStateStore";

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

    // A Map for remembering run-once jobs
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

        populateSourceWrapperMap();
    }

    @Override
    protected void startUp() throws Exception {
        LOG.info("Starting the local job manager");
        this.scheduler.start();
        if (this.properties.containsKey(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY)) {
            scheduleLocallyConfiguredJobs();
        }
    }

    @Override
    protected void shutDown() throws Exception {
        LOG.info("Stopping the local job manager");
        this.scheduler.shutdown(true);
    }

    /**
     * Schedule a job.
     *
     * @param jobProps Job configuration properties
     * @param jobListener {@link JobListener} used for callback,
     *                    can be <em>null</em> if no callback is needed.
     * @throws SchedulerException
     */
    public void scheduleJob(Properties jobProps, JobListener jobListener)
            throws SchedulerException {

        String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
        if (jobListener != null) {
            this.jobListenerMap.put(jobName, jobListener);
        }

        // Build a data map that gets passed to the job
        JobDataMap jobDataMap = new JobDataMap();
        jobDataMap.put(PROPERTIES_KEY, jobProps);
        jobDataMap.put(WORK_UNIT_MANAGER_KEY, this.workUnitManager);
        jobDataMap.put(SOURCE_WRAPPER_MAP_KEY, this.sourceWrapperMap);
        jobDataMap.put(JOB_LOCK_MAP_KEY, this.jobLockMap);
        jobDataMap.put(JOB_STATE_MAP_KEY, this.jobStateMap);
        jobDataMap.put(JOB_SOURCE_MAP_KEY, this.jobSourceMap);
        jobDataMap.put(LAST_JOB_ID_MAP_KEY, this.lastJobIdMap);
        jobDataMap.put(TASK_STATE_STORE_KEY, this.taskStateStore);

        // Build a Quartz job
        JobDetail job = JobBuilder.newJob(UIFJob.class)
                .withIdentity(jobName, Strings.nullToEmpty(jobProps.getProperty(
                        ConfigurationKeys.JOB_GROUP_KEY)))
                .withDescription(Strings.nullToEmpty(jobProps.getProperty(
                        ConfigurationKeys.JOB_DESCRIPTION_KEY)))
                .usingJobData(jobDataMap)
                .build();

        // Schedule the Quartz job with a trigger built from the job configuration
        this.scheduler.scheduleJob(job, getTrigger(job.getKey(), jobProps));

        // If the job should run only once, remember so it can be deleted after its
        // single run is done.
        boolean runOnce = Boolean.valueOf(jobProps.getProperty(
                ConfigurationKeys.JOB_RUN_ONCE_KEY, "false"));
        if (runOnce) {
            this.runOnceJobs.put(jobName, job.getKey());
            this.runOnceJobConfigFiles.put(jobName,
                    jobProps.getProperty(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY));
        }
    }

    /**
     * Callback method when a task is completed.
     *
     * @param jobId Job ID of the given job
     * @param taskState {@link TaskState}
     */
    public void onTaskCompletion(String jobId, TaskState taskState) {
        if (!this.jobStateMap.containsKey(jobId)) {
            LOG.error(String.format("Job %s could not be found", jobId));
            return;
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
            } catch (Exception e) {
                LOG.error("Failed to commit job " + jobId, e);
            }
        }
    }

    /**
     * Schedule locally configured UIF jobs.
     */
    private void scheduleLocallyConfiguredJobs() throws SchedulerException {
        LOG.info("Scheduling locally configured jobs");
        for (Properties jobProps : loadLocalJobConfigs()) {
            boolean runOnce = Boolean.valueOf(jobProps.getProperty(
                    ConfigurationKeys.JOB_RUN_ONCE_KEY, "false"));
            if (runOnce) {
                scheduleJob(jobProps, new RunOnceJobListener());
            } else {
                scheduleJob(jobProps, null);
            }
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
            Properties properties = new Properties(this.properties);
            try {
                File file = new File(jobConfigFileDir, jobConfigFile);
                properties.load(new FileReader(file));
                properties.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY,
                        file.getAbsolutePath());
                jobConfigs.add(properties);
            } catch (FileNotFoundException fnfe) {
                LOG.error("Job configuration file " + jobConfigFile + " not found", fnfe);
            } catch (IOException ioe) {
                LOG.error("Failed to load job configuration from file " + jobConfigFile, ioe);
            }
        }

        LOG.info(String.format(
                jobConfigs.size() == 1 ?
                        "Loaded %d job configuration" :
                        "Loaded %d job configurations",
                jobConfigs.size()));

        return jobConfigs;
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
     * Commit a finished job.
     */
    @SuppressWarnings("unchecked")
    private void commitJob(String jobId, String jobName, JobState jobState) throws Exception {
        try {
            // Do job publishing based on the job commit policy
            JobCommitPolicy commitPolicy = JobCommitPolicy.forName(jobState.getProp(
                    ConfigurationKeys.JOB_COMMIT_POLICY_KEY,
                    ConfigurationKeys.DEFAULT_JOB_COMMIT_POLICY));
            if (commitPolicy == JobCommitPolicy.COMMIT_ON_PARTIAL_SUCCESS ||
                    (commitPolicy == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS &&
                            jobState.getState() == JobState.RunningState.SUCCESSFUL)) {

                LOG.info("Publishing job data of job " + jobId);

                Class<? extends DataPublisher> dataPublisherClass = (Class<? extends DataPublisher>)
                        Class.forName(jobState.getProp(ConfigurationKeys.DATA_PUBLISHER_TYPE));
                Constructor<? extends DataPublisher> dataPublisherConstructor =
                        dataPublisherClass.getConstructor(JobState.class);
                DataPublisher publisher = dataPublisherConstructor.newInstance(jobState);

                publisher.initialize();
                if (publisher.publish(jobState.getTaskStates())) {
                   jobState.setState(JobState.RunningState.COMMITTED);
                }
            }
        } catch (Exception e) {
            jobState.setState(JobState.RunningState.FAILED);
            LOG.error("Failed to publish job data of job " + jobId, e);
        } finally {
            boolean runOnce = this.runOnceJobs.containsKey(jobName);
            persistJobState(jobState);
            cleanupJob(jobState, runOnce);

            JobListener jobListener = runOnce ?
                    this.jobListenerMap.remove(jobName) : this.jobListenerMap.get(jobName);
            // Callback on job completion
            if (jobListener != null) {
                jobListener.jobCompleted(jobState);
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
     * Cleanup a completed job.
     */
    private void cleanupJob(JobState jobState, boolean runOnce) {
        String jobName = jobState.getJobName();
        String jobId = jobState.getJobId();

        // Remove all state bookkeeping information of this scheduled job run
        this.jobSourceMap.remove(jobId).shutdown(jobState);
        this.jobStateMap.remove(jobId);

        LOG.info("Performing cleanup for job " + jobId);
        try {
            if (!runOnce) {
                // Remember the job ID of this most recent run of the job
                this.lastJobIdMap.put(jobName, jobId);
                // Unlock so the next run of the same job can proceed
                this.jobLockMap.get(jobName).unlock();
            } else {
                // Delete the job from the Quartz scheduler and unlock and remove the job lock
                this.scheduler.deleteJob(this.runOnceJobs.remove(jobName));
                // Unlock and remove the lock as it is no longer needed
                this.jobLockMap.remove(jobName).unlock();
            }
        } catch (Exception e) {
            LOG.error("Failed to cleanup job " + jobId, e);
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
        @SuppressWarnings("unchecked")
        public void execute(JobExecutionContext context) throws JobExecutionException {
            JobDataMap dataMap = context.getJobDetail().getJobDataMap();
            Properties jobProps = (Properties) dataMap.get(PROPERTIES_KEY);
            String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);

            ConcurrentMap<String, JobLock> jobLockMap =
                    (ConcurrentMap<String, JobLock>) dataMap.get(JOB_LOCK_MAP_KEY);
            // Try acquiring a job lock before proceeding
            if (!acquireJobLock(jobLockMap, jobName)) {
                LOG.info("Failed to acquire the job lock for job " + jobName);
                return;
            }

            WorkUnitManager workUnitManager = (WorkUnitManager) dataMap.get(
                    WORK_UNIT_MANAGER_KEY);
            Map<String, Class<SourceWrapperBase>> sourceWrapperMap =
                    (Map<String, Class<SourceWrapperBase>>) dataMap.get(SOURCE_WRAPPER_MAP_KEY);
            Map<String, JobState> jobStateMap = (Map<String, JobState>) dataMap.get(
                    JOB_STATE_MAP_KEY);
            Map<String, Source> jobSourceMap = (Map<String, Source>) dataMap.get(
                    JOB_SOURCE_MAP_KEY);
            Map<String, String> lastJobIdMap = (Map<String, String>) dataMap.get(
                    LAST_JOB_ID_MAP_KEY);
            StateStore taskStateStore = (StateStore) dataMap.get(TASK_STATE_STORE_KEY);

            /*
             * Construct job ID, which is in the form of job_<job_id_suffix>
             * <job_id_suffix> is in the form of <job_name>_<current_timestamp>
             */
            String jobIdSuffix = String.format("%s_%d", jobName, System.currentTimeMillis());
            String jobId = "job_" + jobIdSuffix;

            // If this is a run-once job
            boolean runOnce = Boolean.valueOf(jobProps.getProperty(
                    ConfigurationKeys.JOB_RUN_ONCE_KEY, "false"));

            LOG.info("Starting job " + jobId);
            try {
                JobState jobState = new JobState(jobName, jobId);
                // Add all job configuration properties of this job
                jobState.addAll(jobProps);

                SourceWrapperBase source = sourceWrapperMap.get(
                        jobProps.getProperty(ConfigurationKeys.SOURCE_WRAPPER_CLASS_KEY,
                                ConfigurationKeys.DEFAULT_SOURCE_WRAPPER)
                                .toLowerCase())
                        .newInstance();
                SourceState sourceState = new SourceState(jobState, getPreviousWorkUnitStates(
                        jobName, lastJobIdMap, taskStateStore));
                source.init(sourceState);

                // Generate work units based on all previous work unit states
                List<WorkUnit> workUnits = source.getWorkunits(sourceState);
                // If no real work to do
                if (workUnits == null || workUnits.isEmpty()) {
                    LOG.warn("No work units have been created for job " + jobId);
                    source.shutdown(jobState);
                    if (!runOnce) {
                        // Unlock so the next run of the same job can proceed
                        jobLockMap.get(jobName).unlock();
                    } else {
                        // Unlock and remove the lock as it is no longer needed
                        jobLockMap.remove(jobName).unlock();
                    }

                    return;
                }

                jobState.setTasks(workUnits.size());
                jobState.setStartTime(System.currentTimeMillis());
                jobState.setState(JobState.RunningState.WORKING);

                jobStateMap.put(jobId, jobState);
                jobSourceMap.put(jobId, source);

                // Add all generated work units
                int sequence = 0;
                for (WorkUnit workUnit : workUnits) {
                    /*
                     * Construct task ID, which is in the form of
                     * task_<job_id_suffix>_<task_sequence_number>
                     */
                    String taskId = String.format("task_%s_%d", jobIdSuffix, sequence++);
                    WorkUnitState workUnitState = new WorkUnitState(workUnit);
                    workUnitState.setProp(ConfigurationKeys.JOB_ID_KEY, jobId);
                    workUnitState.setProp(ConfigurationKeys.TASK_ID_KEY, taskId);
                    workUnitManager.addWorkUnit(workUnitState);
                }
            } catch (Exception e) {
                LOG.error("Failed to run job " + jobId, e);

                if (jobSourceMap.containsKey(jobId)) {
                    // Remove all state bookkeeping information of this scheduled job run
                    jobSourceMap.remove(jobId).shutdown(jobStateMap.remove(jobId));
                }

                try {
                    if (!runOnce) {
                        // Unlock so the next run of the same job can proceed
                        jobLockMap.get(jobName).unlock();
                    } else {
                        // Unlock and remove the lock as it is no longer needed
                        jobLockMap.remove(jobName).unlock();
                    }
                } catch (IOException ioe) {
                    // Ignored
                }

                throw new JobExecutionException(e);
            }
        }
        
        /**
         * Try acquring the job lock and return whether the lock is successfully locked.
         */
        private boolean acquireJobLock(ConcurrentMap<String, JobLock> jobLock, String jobName) {
            try {
                jobLock.putIfAbsent(jobName, new LocalJobLock());
                JobLock lock = jobLock.get(jobName);
                return lock.tryLock();
            } catch (IOException ioe) {
                LOG.error("Failed to acquire the job lock for job " + jobName, ioe);
                return false;
            }
        }

        /**
         * Get work unit states of the most recent run of this job.
         */
        @SuppressWarnings("unchecked")
        private List<WorkUnitState> getPreviousWorkUnitStates(String jobName,
                Map<String, String> lastJobIdMap, StateStore taskStateStore)
                throws IOException {

            // This is the first run of the job
            if (!lastJobIdMap.containsKey(jobName)) {
                return Lists.newArrayList();
            }

            LOG.info("Loading task states of the most recent run of job " + jobName);
            // Read the task states of the most recent run of the job
            return (List<WorkUnitState>) taskStateStore.getAll(
                    jobName, lastJobIdMap.get(jobName) + TASK_STATE_STORE_TABLE_SUFFIX);
        }
    }
}
