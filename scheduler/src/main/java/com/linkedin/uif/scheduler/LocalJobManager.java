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
    private static final String JOB_SOURCE_KEY = "jobSource";
    private static final String JOB_TASK_COUNT_KEY = "jobTaskCount";
    private static final String JOB_TASK_STATES_KEY = "jobTaskStates";

    // This is used to add newly generated work units
    private final WorkUnitManager workUnitManager;

    // Worker configuration properties
    private final Properties properties;

    // A Quartz scheduler
    private final Scheduler scheduler;

    private final Map<String, Source> jobSource;
    private final Map<String, Integer> jobTaskCount;
    private final Map<String, List<TaskState>> jobTaskStates;

    public LocalJobManager(WorkUnitManager workUnitManager, Properties properties)
            throws Exception {

        this.workUnitManager = workUnitManager;
        this.properties = properties;
        this.scheduler = new StdSchedulerFactory().getScheduler();
        this.jobSource = Maps.newHashMap();
        this.jobTaskCount = Maps.newHashMap();
        this.jobTaskStates = Maps.newHashMap();
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
        this.scheduler.shutdown();
    }

    /**
     * Report the {@link TaskState} of a {@link Task} of the given job.
     *
     * @param jobId Job ID of the given job
     * @param state {@link TaskState}
     */
    public void reportTaskState(String jobId, TaskState state) {
        if (!this.jobTaskStates.containsKey(jobId)) {
            LOG.error(String.format("Job %s could not be found", jobId));
            return;
        }

        this.jobTaskStates.get(jobId).add(state);
        // If all the tasks of the job has completed, then trigger job committer
        if (this.jobTaskStates.get(jobId).size() == this.jobTaskCount.get(jobId)) {

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
            jobDataMap.put(JOB_SOURCE_KEY, this.jobSource);
            jobDataMap.put(JOB_TASK_COUNT_KEY, this.jobTaskCount);
            jobDataMap.put(JOB_TASK_STATES_KEY, this.jobTaskStates);

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
     * A UIF job to schedule locally.
     */
    private static class UIFJob implements Job {

        @Override
        @SuppressWarnings("unchecked")
        public void execute(JobExecutionContext context) throws JobExecutionException {
            JobDataMap dataMap = context.getJobDetail().getJobDataMap();
            Properties properties = (Properties) dataMap.get(PROPERTIES_KEY);
            // We need work unit manager to add and schedule generated work units
            WorkUnitManager workUnitManager = (WorkUnitManager) dataMap.get(
                    WORK_UNIT_MANAGER_KEY);
            Map<String, Source> jobSource = (Map<String, Source>) dataMap.get(
                    JOB_SOURCE_KEY);
            Map<String, Integer> jobTaskCount = (Map<String, Integer>) dataMap.get(
                    JOB_TASK_COUNT_KEY);
            Map<String, List<TaskState>> jobTaskStates =
                    (Map<String, List<TaskState>>) dataMap.get(JOB_TASK_STATES_KEY);

            // Construct job ID, which is in the form of job_<job_id_suffix>
            // <job_id_suffix> is in the form of <job_name>_<current_timestamp>
            String jobIdSuffix = String.format("%s_%d",
                    properties.getProperty(ConfigurationKeys.JOB_NAME_KEY),
                    System.currentTimeMillis());
            String jobId = "job_" + jobIdSuffix;
            if (jobTaskStates.containsKey(jobId)) {
                throw new RuntimeException();
            }
            jobTaskStates.put(jobId, new ArrayList<TaskState>());

            try {
                Source<?, ?> source = (Source<?, ?>) Class.forName(
                        properties.getProperty(ConfigurationKeys.SOURCE_CLASS_KEY))
                        .newInstance();
                jobSource.put(jobId, source);
                com.linkedin.uif.configuration.State state =
                        new com.linkedin.uif.configuration.State();
                // Add all job configuration properties of this job
                state.addAll(properties);

                // Generate work units based on all previous work unit states
                List<WorkUnit> workUnits = source.getWorkunits(
                        new SourceState(state, getPreviousWorkUnitStates(properties)));
                jobTaskCount.put(jobId, workUnits.size());
                // Add all generated work units
                int sequence = 0;
                for (WorkUnit workUnit : workUnits) {
                    // Construct task ID, which is in the form of
                    // task_<job_id_suffix>_<task_sequence_number>
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
         * Get work unit states of the most recent run of this job.
         */
        private List<WorkUnitState> getPreviousWorkUnitStates(Properties properties) {
            return Lists.newArrayList();
        }
    }
}
