package com.linkedin.uif.scheduler;

import java.io.*;
import java.util.List;
import java.util.Properties;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
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
 * A class for scheduling locally configured UIF jobs to run.
 *
 * <p>
 *     This class is backed by a Quartz {@link org.quartz.Scheduler}. Each job
 *     is associated with a cron schedule that is used to create a
 *     {@link org.quartz.Trigger} for the job.
 * </p>
 *
 * @author ynli
 */
public class LocalJobScheduler extends AbstractIdleService {

    private static final Log LOG = LogFactory.getLog(LocalJobScheduler.class);

    private static final String JOB_CONFIG_FILE_EXTENSION = ".pull";
    private static final String PROPERTIES_KEY = "properties";
    private static final String WORK_UNIT_MANAGER_KEY = "workUnitManager";

    // This is used to add newly generated work units
    private final WorkUnitManager workUnitManager;

    // Worker configuration properties
    private final Properties properties;

    // A Quartz scheduler
    private final Scheduler scheduler;

    public LocalJobScheduler(WorkUnitManager workUnitManager, Properties properties)
            throws Exception {

        this.workUnitManager = workUnitManager;
        this.properties = properties;
        this.scheduler = new StdSchedulerFactory().getScheduler();
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
     * Schedule locally configured UIF jobs.
     */
    private void scheduleLocalluConfiguredJobs() throws SchedulerException {
        LOG.info("Scheduling locally configured jobs");
        for (Properties properties : loadLocalJobConfigs()) {
            // Build a data map that gets passed to the job
            JobDataMap jobDataMap = new JobDataMap();
            jobDataMap.put(PROPERTIES_KEY, properties);
            jobDataMap.put(WORK_UNIT_MANAGER_KEY, this.workUnitManager);

            // Build a Quartz job
            JobDetail job = JobBuilder.newJob(UIFJob.class)
                    .withIdentity(
                            Strings.nullToEmpty(properties.getProperty(
                                    ConfigurationKeys.JOB_NAME_KEY)),
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
            Properties properties = new Properties();
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
                        Strings.nullToEmpty(properties.getProperty(
                                ConfigurationKeys.JOB_NAME_KEY)),
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
        public void execute(JobExecutionContext context) throws JobExecutionException {
            JobDataMap dataMap = context.getJobDetail().getJobDataMap();
            Properties properties = (Properties) dataMap.get(PROPERTIES_KEY);
            // We need work unit manager to add and schedule generated work units
            WorkUnitManager workUnitManager = (WorkUnitManager) dataMap.get(
                    WORK_UNIT_MANAGER_KEY);

            try {
                Source<?, ?> source = (Source<?, ?>) Class.forName(properties.getProperty(
                        ConfigurationKeys.SOURCE_CLASS_KEY)).newInstance();
                com.linkedin.uif.configuration.State state =
                        new com.linkedin.uif.configuration.State();
                // Add all job configuration properties of this job
                state.addAll(properties);
                // Generate work units based on all previous work unit states
                List<WorkUnit> workUnits = source.getWorkunits(
                        new SourceState(state, getPreviousWorkUnitStates(properties)));
                // Add all generated work units
                for (WorkUnit workUnit : workUnits) {
                    workUnitManager.addWorkUnit(new WorkUnitState(workUnit));
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
