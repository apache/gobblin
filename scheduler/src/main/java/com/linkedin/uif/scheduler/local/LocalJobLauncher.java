package com.linkedin.uif.scheduler.local;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ServiceManager;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.scheduler.AbstractJobLauncher;
import com.linkedin.uif.scheduler.JobLauncher;
import com.linkedin.uif.scheduler.JobLock;
import com.linkedin.uif.scheduler.JobState;
import com.linkedin.uif.scheduler.Metrics;
import com.linkedin.uif.scheduler.TaskExecutor;
import com.linkedin.uif.scheduler.TaskState;
import com.linkedin.uif.scheduler.TaskStateTracker;
import com.linkedin.uif.scheduler.WorkUnitManager;
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
    // Service manager to manage depedent services
    private final ServiceManager serviceManager;

    private JobState jobState;
    private CountDownLatch countDownLatch;

    public LocalJobLauncher(Properties properties) throws Exception {
        super(properties);

        TaskExecutor taskExecutor = new TaskExecutor(properties);
        TaskStateTracker taskStateTracker = new LocalTaskStateTracker(properties, taskExecutor);
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
        // Stop all dependent services
        this.serviceManager.stopAsync().awaitStopped(5, TimeUnit.SECONDS);
    }

    @Override
    protected JobLock getJobLock(String jobName, Properties jobProps) throws IOException {
        return new LocalJobLock();
    }

    /**
     * Callback method when a task is completed.
     *
     * @param taskState {@link TaskState}
     */
    public synchronized void onTaskCompletion(TaskState taskState) {
        if (Metrics.isEnabled(this.properties)) {
            // Remove all task-level metrics after the task is done
            taskState.removeMetrics();
        }

        LOG.info(String.format("Task %s completed with state %s", taskState.getTaskId(),
                taskState.getWorkingState().name()));
        this.jobState.addTaskState(taskState);
        this.countDownLatch.countDown();
    }
}
