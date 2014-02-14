package com.linkedin.uif.scheduler;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.Queues;
import com.google.common.util.concurrent.AbstractIdleService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.workunit.WorkUnit;

/**
 * A class for managing {@link WorkUnit}s.
 *
 * <p>
 *     It's responsibilities include adding new {@link WorkUnit}s and running them locally.
 *     To run a {@link WorkUnit}, a {@link Task} is first created based on it and the
 *     {@link Task} is scheduled and executed through the {@link TaskManager}.
 * </p>
 *
 * @author ynli
 */
public class WorkUnitManager extends AbstractIdleService {

    private static final Log LOG = LogFactory.getLog(WorkUnitManager.class);

    // This is used to run tasks
    private final TaskManager taskManager;

    // This is used to store submitted work units
    private final BlockingQueue<WorkUnitState> workUnitQueue;

    // This is used to run the handler
    private final ExecutorService executorService;

    // This handler that handles running work units locally
    private final WorkUnitHandler workUnitHandler;

    public WorkUnitManager(TaskManager taskManager) {
        this.taskManager = taskManager;
        // We need a blocking queue to support the producer-consumer model
        // for managing the submission and execution of work units, and we
        // need a priority queue to support priority-based execution of
        // work units.
        this.workUnitQueue = Queues.newLinkedBlockingQueue();
        this.executorService = Executors.newSingleThreadExecutor();
        this.workUnitHandler = new WorkUnitHandler(
                this.workUnitQueue, this.taskManager);
    }

    /**
     * Add a collection of {@link WorkUnitState}s.
     *
     * @param workUnitStates the collection of {@link WorkUnitState}s to add
     */
    public void addWorkUnits(Collection<WorkUnitState> workUnitStates) {
        for (WorkUnitState workUnitState : workUnitStates) {
            this.workUnitQueue.add(workUnitState);
        }
    }

    /**
     * Add a single {@link WorkUnitState}.
     *
     * @param workUnitState the {@link WorkUnitState} to add
     */
    public void addWorkUnit(WorkUnitState workUnitState) {
        this.workUnitQueue.add(workUnitState);
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

        private final BlockingQueue<WorkUnitState> workUnitQueue;
        private final TaskManager taskManager;

        public WorkUnitHandler(BlockingQueue<WorkUnitState> workUnitQueue,
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
                    WorkUnitState workUnitState = this.workUnitQueue.take();
                    // Create a task based off the work unit
                    Task task = new Task(
                            new TaskContext(workUnitState), this.taskManager);
                    // And then execute the task
                    this.taskManager.execute(task);
                } catch (InterruptedException ie) {
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
