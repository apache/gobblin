package com.linkedin.uif.runtime.local;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;

import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.metrics.JobMetrics;
import com.linkedin.uif.runtime.Task;
import com.linkedin.uif.runtime.TaskExecutor;
import com.linkedin.uif.runtime.TaskStateTracker;

/**
 * An implementation of {@link com.linkedin.uif.runtime.TaskStateTracker} for local mode.
 *
 * TODO: rename this to LocalTaskStateTracker once {@link LocalTaskStateTracker} is retired.
 *
 * @author ynli
 */
public class LocalTaskStateTracker2 extends AbstractIdleService implements TaskStateTracker {

    private static final Logger LOG = LoggerFactory.getLogger(LocalTaskStateTracker2.class);

    // This is used to retry failed tasks
    private final TaskExecutor taskExecutor;

    // This is used to schedule and run reporters for reporting state
    // and progress of running tasks
    private final ScheduledThreadPoolExecutor reporterExecutor;

    // Mapping between tasks and the task state reporters associated with them
    private final Map<String, ScheduledFuture<?>> scheduledReporters = Maps.newHashMap();

    // Maximum number of task retries allowed
    private final int maxTaskRetries;

    public LocalTaskStateTracker2(Properties properties, TaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
        this.reporterExecutor = new ScheduledThreadPoolExecutor(
                Integer.parseInt(properties.getProperty(
                        ConfigurationKeys.TASK_STATE_TRACKER_THREAD_POOL_CORE_SIZE_KEY,
                        ConfigurationKeys.DEFAULT_TASK_STATE_TRACKER_THREAD_POOL_CORE_SIZE)));
        this.reporterExecutor.setMaximumPoolSize(
                Integer.parseInt(properties.getProperty(
                        ConfigurationKeys.TASK_STATE_TRACKER_THREAD_POOL_MAX_SIZE_KEY,
                        ConfigurationKeys.DEFAULT_TASK_STATE_TRACKER_THREAD_POOL_MAX_SIZE)));
        this.maxTaskRetries = Integer.parseInt(properties.getProperty(
                ConfigurationKeys.MAX_TASK_RETRIES_KEY,
                ConfigurationKeys.DEFAULT_MAX_TASK_RETRIES));
    }

    @Override
    protected void startUp() {
        LOG.info("Starting the local task state tracker");
    }

    @Override
    protected void shutDown() {
        LOG.info("Stopping the local task state tracker");
        this.reporterExecutor.shutdown();
    }

    @Override
    public void registerNewTask(Task task) {
        try {
            // Schedule a reporter to periodically report state and progress
            // of the given task
            this.scheduledReporters.put(
                    task.getTaskId(),
                    this.reporterExecutor.scheduleAtFixedRate(
                            new TaskStateReporter(task),
                            task.getTaskContext().getStatusReportingInterval(),
                            task.getTaskContext().getStatusReportingInterval(),
                            TimeUnit.MILLISECONDS
                    )
            );
        } catch (RejectedExecutionException ree) {
            LOG.error(String.format(
                    "Scheduling of task state reporter for task %s was rejected", task.getTaskId()));
        }
    }

    @Override
    public void onTaskCompletion(Task task) {
        if (JobMetrics.isEnabled(task.getTaskState().getWorkunit())) {
            // Update record-level metrics after the task is done
            task.updateRecordMetrics();
            task.getTaskState().removeMetrics();
        }

        // Cancel the task state reporter associated with this task. The reporter might
        // not be found  for the given task because the task fails before the task is
        // registered. So we need to make sure the reporter exists before calling cancel.
        if (this.scheduledReporters.containsKey(task.getTaskId())) {
            this.scheduledReporters.remove(task.getTaskId()).cancel(false);
        }

        // Check the task state and handle task retry if task failed and
        // it has not reached the maximum number of retries
        WorkUnitState.WorkingState state = task.getTaskState().getWorkingState();
        if (state == WorkUnitState.WorkingState.FAILED && task.getRetryCount() < this.maxTaskRetries) {
            this.taskExecutor.retry(task);
            return;
        }

        // Mark the completion of this task
        task.markTaskCompletion();

        // At this point, the task is considered being completed.
        LOG.info(String.format("Task %s completed in %dms with state %s",
                task.getTaskId(), task.getTaskState().getTaskDuration(), state));
    }

    /**
     * A class for reporting the state of a task while the task is running.
     */
    private static class TaskStateReporter implements Runnable {

        public final Task task;

        public TaskStateReporter(Task task) {
            this.task = task;
        }

        @Override
        public void run() {
            if (JobMetrics.isEnabled(this.task.getTaskState().getWorkunit())) {
                // Update record-level metrics
                this.task.updateRecordMetrics();
            }
        }
    }
}
