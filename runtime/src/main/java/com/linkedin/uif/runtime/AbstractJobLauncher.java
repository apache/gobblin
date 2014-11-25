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

package com.linkedin.uif.runtime;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.State;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.metastore.FsStateStore;
import com.linkedin.uif.metastore.StateStore;
import com.linkedin.uif.metrics.JobMetrics;
import com.linkedin.uif.publisher.DataPublisher;
import com.linkedin.uif.source.extractor.JobCommitPolicy;
import com.linkedin.uif.source.Source;
import com.linkedin.uif.source.workunit.MultiWorkUnit;
import com.linkedin.uif.source.workunit.WorkUnit;
import com.linkedin.uif.util.ForkOperatorUtils;
import com.linkedin.uif.util.JobLauncherUtils;

/**
 * An abstract implementation of {@link JobLauncher} for execution-framework-specific
 * implementations.
 *
 * @author ynli
 */
public abstract class AbstractJobLauncher implements JobLauncher {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractJobLauncher.class);

    protected static final String TASK_STATE_STORE_TABLE_SUFFIX = ".tst";
    protected static final String JOB_STATE_STORE_TABLE_SUFFIX = ".jst";

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

    /**
     * Run the given list of {@link WorkUnit}s of the given job.
     *
     * @param jobId job ID
     * @param workUnits given list of {@link WorkUnit}s to run
     * @param stateTracker a {@link TaskStateTracker} for task state tracking
     * @param taskExecutor a {@link TaskExecutor} for task execution
     * @param countDownLatch a {@link java.util.concurrent.CountDownLatch} waited on for job completion
     * @return a list of {@link Task}s from the {@link WorkUnit}s
     * @throws InterruptedException
     */
    public static List<Task> runWorkUnits(String jobId, List<WorkUnit> workUnits,
                                          TaskStateTracker stateTracker,
                                          TaskExecutor taskExecutor,
                                          CountDownLatch countDownLatch)
            throws InterruptedException {

        List<Task> tasks = Lists.newArrayList();
        for (WorkUnit workUnit : workUnits) {
            String taskId = workUnit.getProp(ConfigurationKeys.TASK_ID_KEY);
            WorkUnitState workUnitState = new WorkUnitState(workUnit);
            workUnitState.setId(taskId);
            workUnitState.setProp(ConfigurationKeys.JOB_ID_KEY, jobId);
            workUnitState.setProp(ConfigurationKeys.TASK_ID_KEY, taskId);

            // Create a new task from the work unit and submit the task to run
            Task task = new Task(new TaskContext(workUnitState), stateTracker, Optional.of(countDownLatch));
            stateTracker.registerNewTask(task);
            tasks.add(task);
            LOG.info(String.format("Submitting task %s to run", taskId));
            taskExecutor.submit(task);
        }

        LOG.info(String.format("Waiting for submitted tasks of job %s to complete...", jobId));
        while (countDownLatch.getCount() > 0) {
            LOG.info(String.format("%d out of %d tasks of job %s are running",
                    countDownLatch.getCount(), workUnits.size(), jobId));
            countDownLatch.await(1, TimeUnit.MINUTES);
        }
        LOG.info(String.format("All tasks of job %s have completed", jobId));

        return tasks;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void launchJob(Properties jobProps, JobListener jobListener) throws JobException {
        Preconditions.checkNotNull(jobProps);

        String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
        if (Strings.isNullOrEmpty(jobName)) {
            throw new JobException("A job must have a job name specified by job.name");
        }

        String jobDisabled = jobProps.getProperty(ConfigurationKeys.JOB_DISABLED_KEY, "false");
        if (Boolean.valueOf(jobDisabled)) {
            LOG.info(String.format("Not launching job %s as it is disabled", jobName));
            return;
        }

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
            jobId = JobLauncherUtils.newJobId(jobName);
            jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, jobId);
        }

        JobState jobState = new JobState(jobName, jobId);
        // Add all job configuration properties of this job
        jobState.addAll(jobProps);
        // Remember the number of consecutive failures of this job in the past
        jobState.setProp(ConfigurationKeys.JOB_FAILURES_KEY, getFailureCount(jobName));
        jobState.setState(JobState.RunningState.PENDING);

        LOG.info("Starting job " + jobId);

        // Initialize the source for the job
        SourceState sourceState;
        Source<?, ?> source;
        try {
            sourceState = new SourceState(jobState, getPreviousWorkUnitStates(jobName));
            source = new SourceDecorator(initSource(jobProps, sourceState), jobId, LOG);
        } catch (Throwable t) {
            String errMsg = "Failed to initialize the source for job " + jobId;
            LOG.error(errMsg, t);
            unlockJob(jobName, jobLock);
            throw new JobException(errMsg, t);
        }

        // Generate work units of the job from the source
        Optional<List<WorkUnit>> workUnits = Optional.fromNullable(source.getWorkunits(sourceState));
        if (!workUnits.isPresent()) {
            // The absence means there is something wrong getting the work units
            source.shutdown(sourceState);
            unlockJob(jobName, jobLock);
            throw new JobException("Failed to get work units for job " + jobId);
        }

        if (workUnits.get().isEmpty()) {
            // No real work to do
            LOG.warn("No work units have been created for job " + jobId);
            source.shutdown(sourceState);
            unlockJob(jobName, jobLock);
            return;
        }

        long startTime = System.currentTimeMillis();
        jobState.setStartTime(startTime);
        jobState.setState(JobState.RunningState.RUNNING);

        // Populate job/task IDs
        int sequence = 0;
        for (WorkUnit workUnit : workUnits.get()) {
            if (workUnit instanceof MultiWorkUnit) {
                for (WorkUnit innerWorkUnit : ((MultiWorkUnit) workUnit).getWorkUnits()) {
                    addWorkUnit(innerWorkUnit, jobState, sequence++);
                }
            } else {
                addWorkUnit(workUnit, jobState, sequence++);
            }
        }

        // Actually launch the job to run
        try {
            runJob(jobName, jobProps, jobState, workUnits.get());
            if (jobState.getState() == JobState.RunningState.CANCELLED) {
                LOG.info(String.format("Job %s has been cancelled", jobId));
                return;
            }
            setFinalJobState(jobState);
            commitJob(jobId, jobState);
        } catch (Throwable t) {
            String errMsg = "Failed to launch and run job " + jobId;
            LOG.error(errMsg, t);
            throw new JobException(errMsg, t);
        } finally {
            source.shutdown(sourceState);

            long endTime = System.currentTimeMillis();
            jobState.setEndTime(endTime);
            jobState.setDuration(endTime - startTime);

            try {
                persistJobState(jobState);
                cleanupStagingData(jobState);
            } catch (Throwable t) {
                // Catch any possible errors so unlockJob is guaranteed to be called below
                LOG.error("Failed to persist job state and cleanup for job " + jobId, t);
            }

            // Release the job lock
            unlockJob(jobName, jobLock);

            if (JobMetrics.isEnabled(this.properties)) {
                // Remove all job-level metrics after the job is done
                jobState.removeMetrics();
            }

            if (Optional.fromNullable(jobListener).isPresent()) {
                jobListener.jobCompleted(jobState);
            }
        }

        // Throw an exception at the end if the job failed so the caller knows the job failure
        if (jobState.getState() == JobState.RunningState.FAILED) {
            throw new JobException(String.format("Job %s failed", jobId));
        }
    }

    /**
     * Run the given job.
     *
     * <p>
     *     The contract between {@link AbstractJobLauncher#launchJob(java.util.Properties, JobListener)}
     *     and this method is this method is responsible for for setting {@link JobState.RunningState}
     *     properly and upon returning from this method (either normally or due to exceptions) whatever
     *     {@link JobState.RunningState} is set in this method is used to determine if the job has finished.
     * </p>
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
     * Get the number of consecutive failures of the given job in the past.
     */
    private int getFailureCount(String jobName) {
        try {
            if (!this.jobStateStore.exists(jobName, "current" + JOB_STATE_STORE_TABLE_SUFFIX)) {
                return 0;
            }

            List<? extends State> jobStateList = this.jobStateStore.getAll(
                    jobName, "current" + JOB_STATE_STORE_TABLE_SUFFIX);
            if (jobStateList.isEmpty()) {
                return 0;
            }

            State prevJobState = jobStateList.get(0);
            // Read failure count from the persisted job state of its most recent run
            return prevJobState.getPropAsInt(ConfigurationKeys.JOB_FAILURES_KEY, 0);
        } catch (IOException ioe) {
            LOG.warn("Failed to read the previous job state for job " + jobName, ioe);
            return 0;
        }
    }

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
     * Add the given {@link WorkUnit} for execution.
     */
    private void addWorkUnit(WorkUnit workUnit, JobState jobState, int sequence) {
        workUnit.setProp(ConfigurationKeys.JOB_ID_KEY, jobState.getJobId());
        String taskId = JobLauncherUtils.newTaskId(jobState.getJobId(), sequence);
        workUnit.setId(taskId);
        workUnit.setProp(ConfigurationKeys.TASK_ID_KEY, taskId);
        jobState.addTask();
        // Pre-add a task state so if the task fails and no task state is written out,
        // there is still task state for the task when job/task states are persisted.
        jobState.addTaskState(new TaskState(new WorkUnitState(workUnit)));
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
    private List<WorkUnitState> getPreviousWorkUnitStates(String jobName) throws IOException {
        if (this.taskStateStore.exists(jobName, "current" + TASK_STATE_STORE_TABLE_SUFFIX)) {
            // Read the task states of the most recent run of the job
            return (List<WorkUnitState>) this.taskStateStore.getAll(
                    jobName, "current" + TASK_STATE_STORE_TABLE_SUFFIX);
        }

        return Lists.newArrayList();
    }

    /**
     * Try acquiring the job lock and return whether the lock is successfully locked.
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
     * Set final {@link JobState} of the given job.
     */
    private void setFinalJobState(JobState jobState) {
        jobState.setEndTime(System.currentTimeMillis());
        jobState.setDuration(jobState.getEndTime() - jobState.getStartTime());

        JobCommitPolicy commitPolicy = JobCommitPolicy.forName(jobState.getProp(
                ConfigurationKeys.JOB_COMMIT_POLICY_KEY,
                ConfigurationKeys.DEFAULT_JOB_COMMIT_POLICY));

        for (TaskState taskState : jobState.getTaskStates()) {
            // Set fork.branches explicitly here so the rest job flow can pick it up
            jobState.setProp(
                    ConfigurationKeys.FORK_BRANCHES_KEY,
                    taskState.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1));

            // Determine the final job state based on the task states and the job commit policy.
            // If COMMIT_ON_FULL_SUCCESS is used, the job is considered failed if any task failed.
            // On the other hand, if COMMIT_ON_PARTIAL_SUCCESS is used, the job is considered
            // successful even if some tasks failed.
            if (taskState.getWorkingState() != WorkUnitState.WorkingState.SUCCESSFUL &&
                    commitPolicy == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS) {
                jobState.setState(JobState.RunningState.FAILED);
                break;
            }
        }

        if (jobState.getState() == JobState.RunningState.SUCCESSFUL) {
            // Reset the failure count if the job successfully completed
            jobState.setProp(ConfigurationKeys.JOB_FAILURES_KEY, 0);
        }

        if (jobState.getState() == JobState.RunningState.FAILED) {
            // Increment the failure count by 1 if the job failed
            int failures = jobState.getPropAsInt(ConfigurationKeys.JOB_FAILURES_KEY, 0) + 1;
            jobState.setProp(ConfigurationKeys.JOB_FAILURES_KEY, failures);
        }
    }

    /**
     * Commit a finished job.
     */
    @SuppressWarnings("unchecked")
    private void commitJob(String jobId, JobState jobState) throws Exception {
        JobCommitPolicy commitPolicy = JobCommitPolicy.forName(jobState.getProp(
                ConfigurationKeys.JOB_COMMIT_POLICY_KEY,
                ConfigurationKeys.DEFAULT_JOB_COMMIT_POLICY));

        // Only commit job data if 1) COMMIT_ON_PARTIAL_SUCCESS is used,
        // or 2) COMMIT_ON_FULL_SUCCESS is used and the job is successful.
        if (commitPolicy == JobCommitPolicy.COMMIT_ON_PARTIAL_SUCCESS ||
                (commitPolicy == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS &&
                        jobState.getState() == JobState.RunningState.SUCCESSFUL)) {

            Class<? extends DataPublisher> dataPublisherClass = (Class<? extends DataPublisher>)
                    Class.forName(jobState.getProp(ConfigurationKeys.DATA_PUBLISHER_TYPE));
            Constructor<? extends DataPublisher> dataPublisherConstructor =
                    dataPublisherClass.getConstructor(State.class);

            LOG.info(String.format("Publishing job data of job %s with commit policy %s",
                    jobId, commitPolicy.name()));

            Closer closer = Closer.create();
            try {
                DataPublisher publisher = closer.register(dataPublisherConstructor.newInstance(jobState));
                publisher.initialize();
                publisher.publish(jobState.getTaskStates());
                jobState.setState(JobState.RunningState.COMMITTED);
            } finally {
                closer.close();
            }
        } else {
            LOG.info("Job data will not be committed due to commit policy: " + commitPolicy);
        }
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
            this.taskStateStore.putAll(
                    jobName, jobId + TASK_STATE_STORE_TABLE_SUFFIX, jobState.getTaskStates());
            this.jobStateStore.put(jobName, jobId + JOB_STATE_STORE_TABLE_SUFFIX,
                    jobState);
            this.taskStateStore.createAlias(
                    jobName,
                    jobId + TASK_STATE_STORE_TABLE_SUFFIX,
                    "current" + TASK_STATE_STORE_TABLE_SUFFIX);
            this.jobStateStore.createAlias(
                    jobName,
                    jobId + JOB_STATE_STORE_TABLE_SUFFIX,
                    "current" + JOB_STATE_STORE_TABLE_SUFFIX);
        } catch (IOException ioe) {
            LOG.error("Failed to persist job/task states of job " + jobId, ioe);
        }
    }

    /**
     * Cleanup the job's task staging data. This is not doing anything in case job succeeds
     * and data is successfully committed because the staging data has already been moved
     * to the job output directory. But in case the job fails and data is not committed,
     * we want the staging data to be cleaned up.
     */
    private void cleanupStagingData(JobState jobState) {
        for (TaskState taskState : jobState.getTaskStates()) {
            int branches = taskState.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1);
            for (int i = 0; i < branches; i++) {
                try {
                    String writerFsUri = taskState.getProp(
                            ForkOperatorUtils.getPropertyNameForBranch(
                                    ConfigurationKeys.WRITER_FILE_SYSTEM_URI, branches, i),
                            ConfigurationKeys.LOCAL_FS_URI);
                    FileSystem fs = FileSystem.get(URI.create(writerFsUri), new Configuration());

                    String writerFilePath = taskState.getProp(ForkOperatorUtils.getPropertyNameForBranch(
                            ConfigurationKeys.WRITER_FILE_PATH, branches, i));
                    if (Strings.isNullOrEmpty(writerFilePath)) {
                        // The job may be cancelled before the task starts, so this may not be set.
                        continue;
                    }

                    String stagingDirKey = ForkOperatorUtils.getPropertyNameForBranch(
                            ConfigurationKeys.WRITER_STAGING_DIR, branches > 1 ? i : -1);
                    if (taskState.contains(stagingDirKey)) {
                        Path stagingPath = new Path(taskState.getProp(stagingDirKey), writerFilePath);
                        if (fs.exists(stagingPath)) {
                            LOG.info("Cleaning up staging directory " + stagingPath.toUri().getPath());
                            fs.delete(stagingPath, true);
                        }
                    }

                    String outputDirKey = ForkOperatorUtils.getPropertyNameForBranch(
                            ConfigurationKeys.WRITER_OUTPUT_DIR, branches > 1 ? i : -1);
                    if (taskState.contains(outputDirKey)) {
                        Path outputPath = new Path(taskState.getProp(outputDirKey), writerFilePath);
                        if (fs.exists(outputPath)) {
                            LOG.info("Cleaning up output directory " + outputPath.toUri().getPath());
                            fs.delete(outputPath, true);
                        }
                    }
                } catch (IOException ioe) {
                    LOG.error(String.format("Failed to clean staging data for branch %d of task %s",
                            i, taskState.getTaskId()), ioe);
                }
            }
        }
    }
}
