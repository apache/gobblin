package com.linkedin.uif.scheduler;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

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

    // This is used to add newly generated work units
    private final WorkUnitManager workUnitManager;

    // Worker configuration properties
    private final Properties properties;

    // A Quartz scheduler
    private final Scheduler scheduler;

    // Mapping between jobs to the job locks they hold
    private final Map<String, JobLock> jobLockMap;

    // Mapping between jobs to the Source objects used to create work units
    private final Map<String, Source> jobSourceMap;

    // Mapping between jobs to the numbers of tasks
    private final Map<String, Integer> jobTaskCountMap;

    // Mapping between jobs to the tasks comprising each job
    private final Map<String, List<TaskState>> jobTaskStatesMap;

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
    }

    @Override
    protected void startUp() throws Exception {
        LOG.info("Starting the local job scheduler");
        this.scheduler.start();
        scheduleLocalluConfiguredJobs();
    }

    @Override
    protected void shutDown() throws Exception {
        LOG.info("Starting the local job scheduler");
        this.scheduler.shutdown(true);
    }

    /**
     * Report the {@link TaskState} of a {@link Task} of the given job.
     *
     * @param jobId Job ID of the given job
     * @param taskState {@link TaskState}
     */
    public void reportTaskState(String jobId, TaskState taskState) throws IOException {
        if (!this.jobTaskStatesMap.containsKey(jobId)) {
            LOG.error(String.format("Job %s could not be found", jobId));
            return;
        }

        this.jobTaskStatesMap.get(jobId).add(taskState);
        // If all the tasks of the job has completed, then trigger job committer
        if (this.jobTaskStatesMap.get(jobId).size() == this.jobTaskCountMap.get(jobId)) {
            LOG.info("Committing job " + jobId);
            String jobName = taskState.getWorkunit().getProp(ConfigurationKeys.JOB_NAME_KEY);
            commitJob(jobId, jobName, this.jobTaskStatesMap.get(jobId));
        }
    }

    /**
     * Schedule locally configured UIF jobs.
     */
    private void scheduleLocalluConfiguredJobs() throws SchedulerException {
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

        return jobConfigs;
    }

    /**
     * Get a {@link org.quartz.Trigger} from the given job configuration properties.
     */
    private Trigger getTrigger(JobKey jobKey, Properties properties) {
        // Build a trigger for the job with the given cron-style schedule
        Trigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(
                        properties.getProperty(ConfigurationKeys.JOB_NAME_KEY),
                        Strings.nullToEmpty(properties.getProperty(
                                ConfigurationKeys.JOB_GROUP_KEY)))
                .forJob(jobKey)
                .withSchedule(CronScheduleBuilder.cronSchedule(
                        properties.getProperty(ConfigurationKeys.JOB_SCHEDULE_KEY)))
                .build();

        return trigger;
    }

    /**
     * Commit a finished job.
     */
    private void commitJob(String jobId, String jobName, List<TaskState> taskStates)
            throws IOException {

        // Unlock the job lock after committing the job
        this.jobLockMap.get(jobName).unlock();
        this.jobLockMap.remove(jobName);

        // Remove all state bookkeeping information of this scheduled job run
        this.jobSourceMap.remove(jobId);
        this.jobTaskCountMap.remove(jobId);
        this.jobTaskStatesMap.remove(jobId);
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

            Map<String, JobLock> jobLockMap = (Map<String, JobLock>) dataMap.get(JOB_LOCK_MAP_KEY);
            // Try acquiring a job lock before proceeding
            if (!acquireJobLock(jobLockMap, jobName)) {
                LOG.error("Failed to acquire the job lock for job " + jobName);
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
                        new SourceState(state, getPreviousWorkUnitStates(properties)));

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
                throw new JobExecutionException(e);
            }
        }

        /**
         * Try acquring the job lock and return whether the lock is successfully locked.
         */
        private boolean acquireJobLock(Map<String, JobLock> jobLock, String jobName) {
            try {
                if (jobLock.containsKey(jobName)) {
                    if (!jobLock.get(jobName).isLocked()) {
                        // Job lock for the job exists but is not locked
                        LOG.warn(String.format("The most recent run of job %s did not " +
                                "successfully acquire the job lock", jobName));
                        // Simple remove the lock because in this case the most recent
                        // scheduled run of this job should have not proceeded.
                        jobLock.remove(jobName);
                    } else {
                        // The most recent scheduled run of this job has not finished yet
                        return false;
                    }
                }

                // Acquire the job lock and return whether the lock is indeed locked
                JobLock lock = new LocalJobLock();
                lock.lock();
                jobLock.put(jobName, lock);
                return lock.isLocked();
            } catch (IOException ioe) {
                LOG.error("Failed to acquire the job lock for job " + jobName, ioe);
                return false;
            }
        }

        /**
         * Get work unit states of the most recent run of this job.
         */
        private List<WorkUnitState> getPreviousWorkUnitStates(Properties properties) {
            return Lists.newArrayList();
        }
    }
}
