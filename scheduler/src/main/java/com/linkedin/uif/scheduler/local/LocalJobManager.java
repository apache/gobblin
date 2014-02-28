package com.linkedin.uif.scheduler.local;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;

import com.linkedin.uif.metastore.FsStateStore;
import com.linkedin.uif.metastore.StateStore;
import com.linkedin.uif.publisher.DataPublisher;
import com.linkedin.uif.publisher.HDFSDataPublisher;
import com.linkedin.uif.scheduler.JobLock;
import com.linkedin.uif.scheduler.TaskState;
import com.linkedin.uif.scheduler.WorkUnitManager;
import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.WorkUnitState;
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
    private static final String JOB_SOURCE_MAP_KEY = "jobSourceMap";
    private static final String JOB_TASK_COUNT_MAP_KEY = "jobTaskCountMap";
    private static final String JOB_TASK_STATES_MAP_KEY = "jobTaskStatesMap";
    private static final String LAST_JOB_ID_MAP_KEY = "lastJobIdMap";
    private static final String TASK_STATE_STORE_KEY = "taskStateStore";

    // This is used to add newly generated work units
    private final WorkUnitManager workUnitManager;

    // Worker configuration properties
    private final Properties properties;

    // A Quartz scheduler
    private final Scheduler scheduler;

    // Mapping between jobs to the job locks they hold
    private final ConcurrentMap<String, JobLock> jobLockMap;

    // Mapping between jobs to the Source objects used to create work units
    private final Map<String, Source> jobSourceMap;

    // Mapping between jobs to the numbers of tasks
    private final Map<String, Integer> jobTaskCountMap;

    // Mapping between jobs to the tasks comprising each job
    private final Map<String, List<TaskState>> jobTaskStatesMap;

    // Mapping between jobs to the job IDs of their last runs
    private final Map<String, String> lastJobIdMap;

    // Store for persisting task states
    private final StateStore taskStateStore;

    public LocalJobManager(WorkUnitManager workUnitManager, Properties properties)
            throws Exception {

        this.workUnitManager = workUnitManager;
        this.properties = properties;
        this.scheduler = new StdSchedulerFactory().getScheduler();
        // This needs to be a concurrent map because two scheduled runs of the
        // same job (handled by two separate threds) may access it concurrently
        this.jobLockMap = Maps.newConcurrentMap();
        this.jobSourceMap = Maps.newHashMap();
        this.jobTaskCountMap = Maps.newHashMap();
        this.jobTaskStatesMap = Maps.newHashMap();
        this.lastJobIdMap = Maps.newHashMap();
        this.taskStateStore = new FsStateStore(
                properties.getProperty(ConfigurationKeys.TASK_STATE_STORE_FS_URI_KEY),
                properties.getProperty(ConfigurationKeys.TASK_STATE_STORE_ROOT_DIR_KEY),
                TaskState.class);
    }

    @Override
    protected void startUp() throws Exception {
        LOG.info("Starting the local job manager");
        this.scheduler.start();
        scheduleLocallyConfiguredJobs();
    }

    @Override
    protected void shutDown() throws Exception {
        LOG.info("Stopping the local job manager");
        this.scheduler.shutdown(true);
    }

    /**
     * Callback method when a task is completed.
     *
     * @param jobId Job ID of the given job
     * @param taskState {@link TaskState}
     */
    public void onTaskCompletion(String jobId, TaskState taskState) {
        if (!this.jobTaskStatesMap.containsKey(jobId)) {
            LOG.error(String.format("Job %s could not be found", jobId));
            return;
        }

        this.jobTaskStatesMap.get(jobId).add(taskState);
        // If all the tasks of the job have completed (regardless of success or failure),
        // then trigger job committer
        if (this.jobTaskStatesMap.get(jobId).size() == this.jobTaskCountMap.get(jobId)) {
            LOG.info(String.format(
                    "All tasks of job %s have completed, committing it", jobId));
            String jobName = taskState.getWorkunit().getProp(
                    ConfigurationKeys.JOB_NAME_KEY);
            try {
                commitJob(jobId, jobName, this.jobTaskStatesMap.get(jobId));
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
        for (Properties properties : loadLocalJobConfigs()) {
            // Build a data map that gets passed to the job
            JobDataMap jobDataMap = new JobDataMap();
            jobDataMap.put(PROPERTIES_KEY, properties);
            jobDataMap.put(WORK_UNIT_MANAGER_KEY, this.workUnitManager);
            jobDataMap.put(JOB_LOCK_MAP_KEY, this.jobLockMap);
            jobDataMap.put(JOB_SOURCE_MAP_KEY, this.jobSourceMap);
            jobDataMap.put(JOB_TASK_COUNT_MAP_KEY, this.jobTaskCountMap);
            jobDataMap.put(JOB_TASK_STATES_MAP_KEY, this.jobTaskStatesMap);
            jobDataMap.put(LAST_JOB_ID_MAP_KEY, this.lastJobIdMap);
            jobDataMap.put(TASK_STATE_STORE_KEY, this.taskStateStore);

            // Build a Quartz job
            JobDetail job = JobBuilder.newJob(UIFJob.class)
                    .withIdentity(
                            properties.getProperty(ConfigurationKeys.JOB_NAME_KEY),
                            Strings.nullToEmpty(properties.getProperty(
                                    ConfigurationKeys.JOB_GROUP_KEY)))
                    .withDescription(Strings.nullToEmpty(properties.getProperty(
                            ConfigurationKeys.JOB_DESCRIPTION_KEY)))
                    .usingJobData(jobDataMap)
                    .build();

            // Schedule the Quartz job with a trigger built from the job configuration
            this.scheduler.scheduleJob(job, getTrigger(job.getKey(), properties));
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
                properties.load(new FileReader(new File(jobConfigFileDir, jobConfigFile)));
                jobConfigs.add(properties);
            } catch (FileNotFoundException e) {
                LOG.error("Job configuration file " + jobConfigFile + " not found");
            } catch (IOException e) {
                LOG.error("Failed to load job configuration from file " + jobConfigFile);
            }
        }

        LOG.info(String.format("Loaded %d job configurations", jobConfigs.size()));

        return jobConfigs;
    }

    /**
     * Get a {@link org.quartz.Trigger} from the given job configuration properties.
     */
    private Trigger getTrigger(JobKey jobKey, Properties properties) {
        // Build a trigger for the job with the given cron-style schedule
        return TriggerBuilder.newTrigger()
                .withIdentity(
                        properties.getProperty(ConfigurationKeys.JOB_NAME_KEY),
                        Strings.nullToEmpty(properties.getProperty(
                                ConfigurationKeys.JOB_GROUP_KEY)))
                .forJob(jobKey)
                .withSchedule(CronScheduleBuilder.cronSchedule(
                        properties.getProperty(ConfigurationKeys.JOB_SCHEDULE_KEY)))
                .build();
    }

    /**
     * Commit a finished job.
     */
    private void commitJob(String jobId, String jobName, List<TaskState> taskStates)
            throws Exception {

        // TODO: complete the implementation
        DataPublisher publisher = new HDFSDataPublisher(taskStates.get(0));
        publisher.initialize();
        publisher.publishData(taskStates);

        LOG.info("Persisting task states of job " + jobId);
        this.taskStateStore.putAll(jobName, jobId, taskStates);

        // Remove all state bookkeeping information of this scheduled job run
        this.jobSourceMap.remove(jobId);
        this.jobTaskCountMap.remove(jobId);
        this.jobTaskStatesMap.remove(jobId);

        // Remember the job ID of this most recent run of the job
        this.lastJobIdMap.put(jobName, jobId);

        // Unlock so the next run of the same job can proceed
        this.jobLockMap.get(jobName).unlock();
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
            Properties properties = (Properties) dataMap.get(PROPERTIES_KEY);
            String jobName = properties.getProperty(ConfigurationKeys.JOB_NAME_KEY);

            ConcurrentMap<String, JobLock> jobLockMap =
                    (ConcurrentMap<String, JobLock>) dataMap.get(JOB_LOCK_MAP_KEY);
            // Try acquiring a job lock before proceeding
            if (!acquireJobLock(jobLockMap, jobName)) {
                LOG.info("Failed to acquire the job lock for job " + jobName);
                return;
            }

            WorkUnitManager workUnitManager = (WorkUnitManager) dataMap.get(
                    WORK_UNIT_MANAGER_KEY);
            Map<String, Source> jobSourceMap = (Map<String, Source>) dataMap.get(
                    JOB_SOURCE_MAP_KEY);
            Map<String, Integer> jobTaskCountMap = (Map<String, Integer>) dataMap.get(
                    JOB_TASK_COUNT_MAP_KEY);
            Map<String, List<TaskState>> jobTaskStatesMap =
                    (Map<String, List<TaskState>>) dataMap.get(JOB_TASK_STATES_MAP_KEY);
            Map<String, String> lastJobIdMap = (Map<String, String>) dataMap.get(
                    LAST_JOB_ID_MAP_KEY);
            StateStore taskStateStore = (StateStore) dataMap.get(TASK_STATE_STORE_KEY);

            /*
             * Construct job ID, which is in the form of job_<job_id_suffix>
             * <job_id_suffix> is in the form of <job_name>_<current_timestamp>
             */
            String jobIdSuffix = String.format("%s_%d", jobName, System.currentTimeMillis());
            String jobId = "job_" + jobIdSuffix;
            LOG.info("Starting job " + jobId);

            try {
                com.linkedin.uif.configuration.State state =
                        new com.linkedin.uif.configuration.State();
                // Add all job configuration properties of this job
                state.addAll(properties);

                Source<?, ?> source = (Source<?, ?>) Class.forName(
                        properties.getProperty(ConfigurationKeys.SOURCE_CLASS_KEY))
                        .newInstance();
                // Generate work units based on all previous work unit states
                List<WorkUnit> workUnits = source.getWorkunits(
                        new SourceState(state, getPreviousWorkUnitStates(
                                jobName, lastJobIdMap, taskStateStore)));

                // If no real work to do
                if (workUnits == null || workUnits.isEmpty()) {
                    LOG.warn("No work units have been created for job " + jobId);
                    // Unlock so the next run of the same job can proceed
                    jobLockMap.get(jobName).unlock();
                    return;
                }

                jobSourceMap.put(jobId, source);
                jobTaskCountMap.put(jobId, workUnits.size());
                jobTaskStatesMap.put(jobId, new ArrayList<TaskState>(workUnits.size()));

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
                // Remove all state bookkeeping information of this scheduled job run
                jobSourceMap.remove(jobId);
                jobTaskCountMap.remove(jobId);
                jobTaskStatesMap.remove(jobId);

                try {
                    // Unlock so the next run of the same job can proceed
                    jobLockMap.get(jobName).unlock();
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
                    jobName, lastJobIdMap.get(jobName));
        }
    }
}
