package com.linkedin.uif.runtime.local;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ServiceManager;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.metrics.JobMetrics;
import com.linkedin.uif.runtime.AbstractJobLauncher;
import com.linkedin.uif.runtime.FileBasedJobLock;
import com.linkedin.uif.runtime.JobException;
import com.linkedin.uif.runtime.JobLauncher;
import com.linkedin.uif.runtime.JobLock;
import com.linkedin.uif.runtime.JobState;
import com.linkedin.uif.runtime.TaskExecutor;
import com.linkedin.uif.runtime.TaskState;
import com.linkedin.uif.runtime.TaskStateTracker;
import com.linkedin.uif.runtime.WorkUnitManager;
import com.linkedin.uif.source.workunit.WorkUnit;

/**
 * An implementation of {@link JobLauncher} for launching and running jobs
 * locally on a single node.
 *
 * @author ynli
 */
public class LocalJobLauncher extends AbstractJobLauncher {

    private static final Logger LOG = LoggerFactory.getLogger(LocalJobLauncher.class);

    private final WorkUnitManager workUnitManager;
    // Service manager to manage dependent services
    private final ServiceManager serviceManager;

    private volatile JobState jobState;
    private volatile CountDownLatch countDownLatch;
    private volatile boolean isCancelled = false;

    public LocalJobLauncher(Properties properties) throws Exception {
        super(properties);

        TaskExecutor taskExecutor = new TaskExecutor(properties);
        TaskStateTracker taskStateTracker = new LocalTaskStateTracker2(properties, taskExecutor);
        ((LocalTaskStateTracker2) taskStateTracker).setJobLauncher(this);
        this.workUnitManager = new WorkUnitManager(taskExecutor, taskStateTracker);

        this.serviceManager = new ServiceManager(Lists.newArrayList(
                // The order matters due to dependencies between services
                taskExecutor,
                taskStateTracker,
                this.workUnitManager
        ));
        // Start all dependent services
        this.serviceManager.startAsync().awaitHealthy(5, TimeUnit.SECONDS);
    }

    @Override
    public void cancelJob(Properties jobProps) throws JobException {
        if (isCancelled || !Optional.fromNullable(this.countDownLatch).isPresent()) {
            LOG.info(String.format(
                    "Job %s has already been cancelled or has not started yet",
                    jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY)));
            return;
        }

        // Unblock the thread that calls runJob below
        while (this.countDownLatch.getCount() > 0) {
            this.countDownLatch.countDown();
        }

        isCancelled = true;
    }

    @Override
    protected void runJob(String jobName, Properties jobProps, JobState jobState,
                          List<WorkUnit> workUnits) throws Exception {

        this.jobState = jobState;
        this.countDownLatch = new CountDownLatch(workUnits.size());

        String jobId = jobProps.getProperty(ConfigurationKeys.JOB_ID_KEY);

        // Add all generated work units
        for (WorkUnit workUnit : workUnits) {
            String taskId = workUnit.getProp(ConfigurationKeys.TASK_ID_KEY);
            WorkUnitState workUnitState = new WorkUnitState(workUnit);
            workUnitState.setId(taskId);
            workUnitState.setProp(ConfigurationKeys.JOB_ID_KEY, jobId);
            workUnitState.setProp(ConfigurationKeys.TASK_ID_KEY, taskId);
            this.workUnitManager.addWorkUnit(workUnitState);
        }

        LOG.info(String.format("Waiting for job %s to complete...", jobId));
        // Wait for all tasks to complete
        this.countDownLatch.await();

        // Set job state appropriately
        if (isCancelled) {
            jobState.setState(JobState.RunningState.CANCELLED);
        } else if (this.jobState.getState() == JobState.RunningState.RUNNING) {
            this.jobState.setState(JobState.RunningState.SUCCESSFUL);
        }

        // Stop all dependent services
        this.serviceManager.stopAsync().awaitStopped(5, TimeUnit.SECONDS);
    }

    @Override
    protected JobLock getJobLock(String jobName, Properties jobProps) throws IOException {
        URI fsUri = URI.create(jobProps.getProperty(
                ConfigurationKeys.FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI));
        return new FileBasedJobLock(
                FileSystem.get(fsUri, new Configuration()),
                jobProps.getProperty(ConfigurationKeys.JOB_LOCK_DIR_KEY),
                jobName);
    }

    /**
     * Callback method when a task is completed.
     *
     * @param taskState {@link TaskState}
     */
    public synchronized void onTaskCompletion(TaskState taskState) {
        if (JobMetrics.isEnabled(this.properties)) {
            // Remove all task-level metrics after the task is done
            taskState.removeMetrics();
        }

        LOG.info(String.format("Task %s completed with state %s", taskState.getTaskId(),
                taskState.getWorkingState().name()));
        if (taskState.getWorkingState() == WorkUnitState.WorkingState.FAILED) {
            // The job is considered being failed if any task failed
            this.jobState.setState(JobState.RunningState.FAILED);
        }
        this.jobState.addTaskState(taskState);
        this.countDownLatch.countDown();
    }
}
