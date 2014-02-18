package com.linkedin.uif.scheduler;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.Queues;
import com.google.common.util.concurrent.AbstractIdleService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.configuration.ConfigurationKeys;

/**
 * A class for managing {@link Task}s.
 *
 * <p>
 *     It's responsibilities include executing new {@link Task}s using a
 *     thread pool and handling failed {@link Task}s.
 * </p>
 *
 * @author ynli
 */
public class TaskManager extends AbstractIdleService {

    private static final Log LOG = LogFactory.getLog(TaskManager.class);

    // Thread pool for running tasks
    private final ExecutorService executor;

    // A queue for failed tasks
    private final BlockingQueue<Task> failedTaskQueue;

    private final TaskStateTracker taskStateTracker;

    // Maximum number of task retries allowed
    private final int maxTaskRetries;

    public TaskManager(TaskStateTracker taskStateTracker, Properties properties) {
        this.taskStateTracker = taskStateTracker;
        // Currently a fixed-size thread pool is used to execute tasks.
        // We probably need to revisist this later.
        this.executor = Executors.newFixedThreadPool(
                Integer.parseInt(properties.getProperty(
                        ConfigurationKeys.TASK_SCHEDULER_THREADPOOL_SIZE_KEY,
                        ConfigurationKeys.DEFAULT_TASK_SCHEDULER_THREADPOOL_SIZE)));
        this.failedTaskQueue = Queues.newLinkedBlockingQueue();
        this.maxTaskRetries = Integer.parseInt(properties.getProperty(
                ConfigurationKeys.MAX_TASK_RETRIES_KEY,
                ConfigurationKeys.DEFAULT_MAX_TASK_RETRIES));
    }

    @Override
    protected void startUp() throws Exception {
        LOG.info("Starting the task manager");
        if (this.executor.isShutdown() || this.executor.isTerminated()) {
            throw new IllegalStateException();
        }
    }

    @Override
    protected void shutDown() throws Exception {
        LOG.info("Stopping the task manager ");
        this.executor.shutdown();
    }

    /**
     * Execute a {@link Task}.
     *
     * @param task {@link Task} to execute
     */
    public void execute(Task task) {
        this.executor.execute(task);
        LOG.info(String.format("Scheduled task %s of job %s to run",
                task.getTaskId(), task.getJobId()));
    }

    /**
     * Callback method when the given task is completed.
     *
     * @param task given task that is completed
     * @throws IOException
     */
    public void onTaskCompletion(Task task) throws IOException {
        WorkUnitState.WorkingState state = task.getTaskState().getWorkingState();
        LOG.info(String.format("Task %s of job %s completed with state %s",
                task.getTaskId(), task.getJobId(), state));
        if (state == WorkUnitState.WorkingState.FAILED) {
            if (task.getRetryCount() < this.maxTaskRetries) {
                this.failedTaskQueue.add(task);
            } else {
                this.taskStateTracker.reportTaskState(task.getTaskState());
            }
        } else {
            this.taskStateTracker.reportTaskState(task.getTaskState());
        }
    }
}
