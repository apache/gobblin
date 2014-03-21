package com.linkedin.uif.scheduler;

import java.util.Properties;
import java.util.concurrent.*;

import com.google.common.util.concurrent.AbstractIdleService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.linkedin.uif.configuration.ConfigurationKeys;

/**
 * A class for executing new {@link Task}s and retrying failed ones.
 *
 * @author ynli
 */
public class TaskExecutor extends AbstractIdleService {

    private static final Log LOG = LogFactory.getLog(TaskExecutor.class);

    // Thread pool executor for running tasks
    private final ExecutorService executor;

    // Scheduled thread pool executor for scheduling task retries
    private final ScheduledThreadPoolExecutor retryExecutor;

    // Task retry interval
    private final long retryIntervalInSeconds;

    public TaskExecutor(Properties properties) {
        // Currently a fixed-size thread pool is used to execute tasks.
        // We probably need to revisist this later.
        this.executor = Executors.newFixedThreadPool(
                Integer.parseInt(properties.getProperty(
                        ConfigurationKeys.TASK_EXECUTOR_THREADPOOL_SIZE_KEY,
                        ConfigurationKeys.DEFAULT_TASK_SCHEDULER_THREADPOOL_SIZE)));

         // Using a separate thread pool for task retries to achieve isolation
         // between normal task execution and tasi retries
        this.retryExecutor = new ScheduledThreadPoolExecutor(
                Integer.parseInt(properties.getProperty(
                        ConfigurationKeys.TASK_RETRY_THREAD_POOL_CORE_SIZE_KEY,
                        ConfigurationKeys.DEFAULT_TASK_RETRY_THREAD_POOL_CORE_SIZE)));
        this.retryExecutor.setMaximumPoolSize(
                Integer.parseInt(properties.getProperty(
                        ConfigurationKeys.TASK_RETRY_THREAD_POOL_MAX_SIZE_KEY,
                        ConfigurationKeys.DEFAULT_TASK_RETRY_THREAD_POOL_MAX_SIZE)));

        this.retryIntervalInSeconds = Long.parseLong(properties.getProperty(
                ConfigurationKeys.TASK_RETRY_INTERVAL_IN_SEC_KEY,
                ConfigurationKeys.DEFAULT_TASK_RETRY_INTERVAL_IN_SEC));
    }

    @Override
    protected void startUp() throws Exception {
        LOG.info("Starting the task executor");
        if (this.executor.isShutdown() || this.executor.isTerminated()) {
            throw new IllegalStateException();
        }
    }

    @Override
    protected void shutDown() throws Exception {
        LOG.info("Stopping the task executor ");
        this.executor.shutdown();
    }

    /**
     * Execute a {@link Task}.
     *
     * @param task {@link Task} to be executed
     */
    public void execute(Task task) {
        this.executor.execute(task);
        LOG.info(String.format("Scheduled task %s to run", task.getTaskId()));
    }

    /**
     * Retry a failed {@link Task}.
     *
     * @param task failed {@link Task} to be retried
     */
    public void retry(Task task) {
        if (Metrics.isEnabled(task.getTaskState().getWorkunit())) {
            // Adjust metrics to clean up numbers from the failed task
            task.getTaskState().adjustJobMetricsOnRetry();
            // Remove task-level metrics associated with this task so
            // the retry will use fresh metrics
            task.getTaskState().removeMetrics();
        }

        // Task retry interval increases linearly with number of retries
        long interval = task.getRetryCount() * this.retryIntervalInSeconds;
        // Schedule the retry of the failed task
        this.retryExecutor.schedule(task, interval, TimeUnit.SECONDS);
        LOG.info(String.format(
                "Scheduled retry of failed task %s to run in %d seconds",
                task.getTaskId(), interval));
        task.incrementRetryCount();
    }
}
