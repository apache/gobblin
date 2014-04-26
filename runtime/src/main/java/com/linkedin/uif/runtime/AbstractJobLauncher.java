package com.linkedin.uif.runtime;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.metastore.FsStateStore;
import com.linkedin.uif.metastore.StateStore;
import com.linkedin.uif.publisher.DataPublisher;
import com.linkedin.uif.source.workunit.WorkUnit;

/**
 * An abstract implementation of {@link JobLauncher} for execution-framework-specific
 * implementations.
 *
 * @author ynli
 */
public abstract class AbstractJobLauncher implements JobLauncher {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractJobLauncher.class);

    private static final String TASK_STATE_STORE_TABLE_SUFFIX = ".tst";
    private static final String JOB_STATE_STORE_TABLE_SUFFIX = ".jst";

    // Framework configuration properties
    protected final Properties properties;

    // Mapping between Source wrapper keys and Source wrapper classes
    private final Map<String, Class<SourceWrapperBase>> sourceWrapperMap = Maps.newHashMap();

    // Store for persisting job state
    private final StateStore jobStateStore;

    // Store for persisting task states
    private final StateStore taskStateStore;

    public AbstractJobLauncher(Properties properties) throws Exception {
        this.properties = properties;

        this.jobStateStore = new FsStateStore(
                properties.getProperty(ConfigurationKeys.STATE_STORE_FS_URI_KEY,
                        ConfigurationKeys.LOCAL_FS_URI),
                properties.getProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY),
                JobState.class);
        this.taskStateStore = new FsStateStore(
                properties.getProperty(ConfigurationKeys.STATE_STORE_FS_URI_KEY,
                        ConfigurationKeys.LOCAL_FS_URI),
                properties.getProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY),
                TaskState.class);

        populateSourceWrapperMap();
    }

    @Override
    public void launchJob(Properties jobProps) throws JobException {
        Preconditions.checkNotNull(jobProps);

        // Add all framework-specific configuration properties
        jobProps.putAll(this.properties);
        String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);

        // Get the job lock
        JobLock jobLock;
        try {
            jobLock = getJobLock(jobName, jobProps);
        } catch (IOException ioe) {
            throw new JobException("Failed to get job lock for job " + jobName, ioe);
        }

        // Try acquiring the job lock before proceeding
        if (!tryLockJob(jobName, jobLock)) {
            LOG.info(String.format(
                    "Previous instance of job %s is still running, skipping this scheduled run",
                    jobName));
            return;
        }

        String jobId = jobProps.getProperty(ConfigurationKeys.JOB_ID_KEY);
        // If no job ID is assigned (e.g., if the job is assigned through Azkaban),
        // assign a new job ID here.
        if (Strings.isNullOrEmpty(jobId)) {
            jobId = JobLauncherUtil.newJobId(jobName);
            jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, jobId);
        }

        JobState jobState = new JobState(jobName, jobId);
        // Add all job configuration properties of this job
        jobState.addAll(jobProps);

        LOG.info("Starting job " + jobId);

        // Initialize the source for the job
        SourceState sourceState;
        SourceWrapperBase source;
        try {
            sourceState = new SourceState(jobState, getPreviousWorkUnitStates(jobName,
                    jobProps.getProperty(ConfigurationKeys.JOB_PREVIOUS_RUN_ID_KEY)));
            source = initSource(jobProps, sourceState);
        } catch (Throwable t) {
            String errMsg = "Failed to initialize source for job " + jobId;
            LOG.error(errMsg, t);
            unlockJob(jobName, jobLock);
            throw new JobException(errMsg, t);
        }

        // Generate work units of the job from the source
        List<WorkUnit> workUnits = source.getWorkunits(sourceState);
        // If there is no real work to do
        if (workUnits == null || workUnits.isEmpty()) {
            LOG.warn("No work units to do for job " + jobId);
            source.shutdown(sourceState);
            unlockJob(jobName, jobLock);
            return;
        }

        jobState.setTasks(workUnits.size());
        jobState.setStartTime(System.currentTimeMillis());
        jobState.setState(JobState.RunningState.WORKING);

        // Populate job/task IDs
        int sequence = 0;
        for (WorkUnit workUnit : workUnits) {
            String taskId = JobLauncherUtil.newTaskId(jobId, sequence++);
            workUnit.setId(taskId);
            workUnit.setProp(ConfigurationKeys.JOB_ID_KEY, jobId);
            workUnit.setProp(ConfigurationKeys.TASK_ID_KEY, taskId);
        }

        // Actually launch the job to run
        try {
            runJob(jobName, jobProps, jobState, workUnits);
            jobState = getFinalJobState(jobState);
            commitJob(jobId, jobState);
        } catch (Throwable t) {
            String errMsg = "Failed to launch job " + jobId;
            LOG.error(errMsg, t);
            jobState.setState(JobState.RunningState.FAILED);
            throw new JobException(errMsg, t);
        } finally {
            source.shutdown(sourceState);
            persistJobState(jobState);
            cleanupStagingData(jobState);
            unlockJob(jobName, jobLock);
        }
    }

    /**
     * Run the given job.
     *
     * @param jobName Job name
     * @param jobProps Job configuration properties
     * @param jobState Job state
     * @param workUnits List of {@link WorkUnit}s of the job
     */
    protected abstract void runJob(String jobName, Properties jobProps,
                                   JobState jobState, List<WorkUnit> workUnits)
            throws Exception;

    /**
     * Get a {@link JobLock} to be used for the job.
     *
     * @param jobName Job name
     * @param jobProps Job configuration properties
     * @return {@link JobLock} to be used for the job
     */
    protected abstract JobLock getJobLock(String jobName, Properties jobProps)
            throws IOException;

    /**
     * Initialize the source for the given job.
     */
    private SourceWrapperBase initSource(Properties jobProps, SourceState sourceState)
            throws Exception {

        SourceWrapperBase source = this.sourceWrapperMap.get(jobProps.getProperty(
                ConfigurationKeys.SOURCE_WRAPPER_CLASS_KEY,
                ConfigurationKeys.DEFAULT_SOURCE_WRAPPER)
                .toLowerCase()).newInstance();
        source.init(sourceState);
        return source;
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
     * Get a list of work unit states of the most recent run of the given job.
     */
    @SuppressWarnings("unchecked")
    private List<WorkUnitState> getPreviousWorkUnitStates(String jobName,
                                                          String previousJobId)
            throws IOException {

        // This is the first run of the job
        if (Strings.isNullOrEmpty(previousJobId)) {
            return Lists.newArrayList();
        }

        LOG.info("Loading task states of the most recent run of job " + jobName);
        // Read the task states of the most recent run of the job
        return (List<WorkUnitState>) this.taskStateStore.getAll(
                jobName, previousJobId + TASK_STATE_STORE_TABLE_SUFFIX);
    }

    /**
     * Try acquring the job lock and return whether the lock is successfully locked.
     */
    private boolean tryLockJob(String jobName, JobLock jobLock) {
        try {
            return jobLock.tryLock();
        } catch (IOException ioe) {
            LOG.error("Failed to acquire the job lock for job " + jobName, ioe);
            return false;
        }
    }

    /**
     * Unlock a completed or failed job.
     */
    private void unlockJob(String jobName, JobLock jobLock) {
        try {
            // Unlock so the next run of the same job can proceed
            jobLock.unlock();
        } catch (IOException ioe) {
            LOG.error("Failed to unlock for job " + jobName, ioe);
        }
    }

    /**
     * Build a {@link JobState} object capturing the state of the given job.
     */
    private JobState getFinalJobState(JobState jobState) {
        jobState.setEndTime(System.currentTimeMillis());
        jobState.setDuration(jobState.getEndTime() - jobState.getStartTime());
        if (jobState.getState() == JobState.RunningState.WORKING) {
            jobState.setState(JobState.RunningState.SUCCESSFUL);
        }

        for (TaskState taskState : jobState.getTaskStates()) {
            // The job is considered failed if any task failed
            if (taskState.getWorkingState() == WorkUnitState.WorkingState.FAILED) {
                jobState.setState(JobState.RunningState.FAILED);
                break;
            }
        }

        return jobState;
    }

    /**
     * Commit a finished job.
     */
    @SuppressWarnings("unchecked")
    private void commitJob(String jobId, JobState jobState) throws Exception {
        JobCommitPolicy commitPolicy = JobCommitPolicy.forName(jobState.getProp(
                ConfigurationKeys.JOB_COMMIT_POLICY_KEY,
                ConfigurationKeys.DEFAULT_JOB_COMMIT_POLICY));

        // Do job publishing based on the job commit policy
        if (commitPolicy == JobCommitPolicy.COMMIT_ON_PARTIAL_SUCCESS ||
                (commitPolicy == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS &&
                        jobState.getState() == JobState.RunningState.SUCCESSFUL)) {

            LOG.info("Publishing job data of job " + jobId + " with commit policy " + commitPolicy);

            Class<? extends DataPublisher> dataPublisherClass = (Class<? extends DataPublisher>)
                    Class.forName(jobState.getProp(ConfigurationKeys.DATA_PUBLISHER_TYPE));
            Constructor<? extends DataPublisher> dataPublisherConstructor =
                    dataPublisherClass.getConstructor(com.linkedin.uif.configuration.State.class);
            DataPublisher publisher = dataPublisherConstructor.newInstance(jobState);

            publisher.initialize();
            if (publisher.publish(jobState.getTaskStates())) {
                jobState.setState(JobState.RunningState.COMMITTED);
            }
        } else {
            LOG.info("Job data will not be committed due to commit policy: " + commitPolicy);
        }
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
     * Cleanup the job's task staging/output directories.
     */
    private void cleanupStagingData(JobState jobState) {
        FileSystem fs;
        try {
            fs = FileSystem.get(
                    URI.create(jobState.getProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI)),
                    new Configuration());
        } catch (IOException ioe) {
            LOG.error("Failed to get a file system instance", ioe);
            return;
        }

        String relPath = jobState.getProp(
                ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY).replaceAll("\\.", "/");

        try {
            Path taskStagingPath = new Path(
                    jobState.getProp(ConfigurationKeys.WRITER_STAGING_DIR), relPath);
            if (fs.exists(taskStagingPath)) {
                fs.delete(taskStagingPath, true);
            }
        } catch (IOException ioe) {
            LOG.error("Failed to cleanup task staging directory of job " +
                    jobState.getJobId(), ioe);
        }

        try {
            Path taskOutputPath = new Path(
                    jobState.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR), relPath);
            if (fs.exists(taskOutputPath)) {
                fs.delete(taskOutputPath, true);
            }
        } catch (IOException ioe) {
            LOG.error("Failed to cleanup task output directory of job " +
                    jobState.getJobId(), ioe);
        }
    }
}
