package com.linkedin.uif.scheduler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;

import com.linkedin.uif.configuration.ConfigurationKeys;

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

    private static final String JOB_CONFIG_FILE_EXTENSION = ".pull";
    private static final String JOB_SCHEDULER_KEY = "jobScheduler";
    private static final String PROPERTIES_KEY = "jobProps";
    private static final String JOB_LISTENER_KEY = "jobListener";
    private static final String TASK_STATE_STORE_TABLE_SUFFIX = ".tst";

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

    // Mapping between jobs to the job IDs of their last runs
    private final Map<String, String> lastJobIdMap = Maps.newHashMap();

    // A monitor for changes to job configuration files
    private FileAlterationMonitor fileAlterationMonitor;

    public JobScheduler(Properties properties) throws Exception {
        this.properties = properties;
        this.scheduler = new StdSchedulerFactory().getScheduler();
        restoreLastJobIdMap();
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
        jobDataMap.put(JOB_SCHEDULER_KEY, this);
        jobDataMap.put(PROPERTIES_KEY, jobProps);
        jobDataMap.put(JOB_LISTENER_KEY, jobListener);

        // Build a Quartz job
        JobDetail job = JobBuilder.newJob(GobblinJob.class)
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
        String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
        // Populate the assigned job ID
        jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, JobLauncherUtil.newJobId(jobName));
        // Populate the job ID of the previous run of the job if it exists
        if (this.lastJobIdMap.containsKey(jobName)) {
            jobProps.setProperty(
                    ConfigurationKeys.JOB_PREVIOUS_RUN_ID_KEY,
                    this.lastJobIdMap.get(jobName));
        }

        // Launch the job
        try {
            JobLauncher jobLauncher = JobLauncherFactory.newJobLauncher(this.properties);
            jobLauncher.launchJob(jobProps);
        } catch (Throwable t) {
            String errMsg = "Failed to launch and run job " + jobName;
            LOG.error(errMsg, t);
            throw new JobException(errMsg, t);
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
     * A Gobblin job to be scheduled.
     */
    @DisallowConcurrentExecution
    public static class GobblinJob implements Job {

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
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
}
