package com.linkedin.uif.scheduler;

import com.google.common.collect.Queues;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A class for managing {@link Task}s.
 *
 * <p>
 *     It's responsibilities include executing new {@link Task}s using a
 *     thread pool and handling failed {@link Task}s.
 * </p>
 */
public class TaskManager extends AbstractIdleService {

    private static final Log LOG = LogFactory.getLog(TaskManager.class);

    private static final String TASK_SCHEDULER_THREADPOOL_SIZE =
            "uif.scheduler.threadpool.size";
    private static final String DEFAULT_TASK_SCHEDULER_THREADPOOL_SIZE = "8";

    // Thread pool for running tasks
    private final ExecutorService executor;

    // A queue for failed tasks
    private final BlockingQueue<Task<?, ?>> failedTaskQueue;

    public TaskManager(Properties properties) {
        // Currently a fixed-size thread pool is used to execute tasks.
        // We probably need to revisist this later.
        this.executor = Executors.newFixedThreadPool(
                Integer.parseInt(properties.getProperty(
                        TASK_SCHEDULER_THREADPOOL_SIZE,
                        DEFAULT_TASK_SCHEDULER_THREADPOOL_SIZE)));
        this.failedTaskQueue = Queues.newLinkedBlockingQueue();
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
    public void execute(Task<?, ?> task) {
        this.executor.execute(task);
        LOG.info(String.format("Scheduled task %s to run", task.toString()));
    }

    /**
     * Add a failed task.
     *
     * @param task failed task
     */
    public void addFailedTask(Task<?, ?> task) {
        this.failedTaskQueue.add(task);
    }
}
