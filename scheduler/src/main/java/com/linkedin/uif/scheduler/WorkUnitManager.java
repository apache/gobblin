package com.linkedin.uif.scheduler;

import com.google.common.collect.Queues;
import com.google.common.util.concurrent.AbstractIdleService;
import com.linkedin.uif.extractor.inputsource.WorkUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A class for managing {@link WorkUnit}s.
 *
 * <p>
 *     It's responsibilities include adding new {@link WorkUnit}s and running them locally.
 *     To run a {@link WorkUnit}, a {@link Task} is first created based on it and the
 *     {@link Task} is scheduled and executed through the {@link TaskManager}.
 * </p>
 */
public class WorkUnitManager extends AbstractIdleService {

    private static final Log LOG = LogFactory.getLog(WorkUnitManager.class);

    private final Properties properties;
    private final TaskManager taskManager;
    // This is used to store submitted work units
    private final BlockingQueue<WorkUnit> workUnitQueue;
    // This is used to run the handler
    private final ExecutorService executorService;
    // This handler that handles running work units locally
    private final WorkUnitHandler workUnitHandler;

    public WorkUnitManager(Properties properties, TaskManager taskManager) {
        this.properties = properties;
        this.taskManager = taskManager;
        // We need a blocking queue to support the producer-consumer model
        // for managing the submission and execution of work units, and we
        // need a priority queue to support priority-based execution of
        // work units.
        this.workUnitQueue = Queues.newPriorityBlockingQueue();
        this.executorService = Executors.newSingleThreadExecutor();
        this.workUnitHandler = new WorkUnitHandler(
                this.workUnitQueue, this.taskManager);
    }

    /**
     * Add a collection of {@link WorkUnit}s.
     *
     * @param workUnits the collection of {@link WorkUnit}s to add
     */
    public void addWorkUnits(Collection<WorkUnit> workUnits) {
        for (WorkUnit workUnit : workUnits) {
            this.workUnitQueue.add(workUnit);
        }
    }

    /**
     * Add a single {@link WorkUnit}.
     *
     * @param workUnit the {@link WorkUnit} to add
     */
    public void addWorkUnit(WorkUnit workUnit) {
        this.workUnitQueue.add(workUnit);
    }

    @Override
    protected void startUp() throws Exception {
        LOG.info("Starting the work unit manager");
        this.executorService.execute(this.workUnitHandler);
    }

    @Override
    protected void shutDown() throws Exception {
        LOG.info("Stopping the work unit manager");
        this.workUnitHandler.stop();
        this.executorService.shutdown();
    }

    /**
     * A handler that does the actual work of running {@link WorkUnit}s locally.
     */
    private static class WorkUnitHandler implements Runnable {

        private final BlockingQueue<WorkUnit> workUnitQueue;
        private final TaskManager taskManager;

        public WorkUnitHandler(BlockingQueue<WorkUnit> workUnitQueue,
                TaskManager taskManager) {

            this.workUnitQueue = workUnitQueue;
            this.taskManager = taskManager;
        }

        // Tells if the handler is asked to stop
        private volatile boolean stopped = false;

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            while (!this.stopped) {
                try {
                    // Take one work unit at a time from the queue
                    WorkUnit workUnit = this.workUnitQueue.take();
                    // Create a task based off the work unit
                    Task<?, ?> task = new Task(
                            new TaskContext(workUnit), this.taskManager);
                    // And then execute the task
                    this.taskManager.execute(task);
                } catch (InterruptedException ie) {
                    // Ignored
                } catch (IOException ioe) {
                    // Ignored
                }
            }
        }

        /**
         * Ask the handler to stop.
         */
        public void stop() {
            this.stopped = true;
        }
    }
}
