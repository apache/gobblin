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

package gobblin.runtime;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.AbstractIdleService;

import gobblin.configuration.WorkUnitState;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.ExecutorsUtils;


/**
 * A class for managing {@link WorkUnit}s.
 *
 * <p>
 *     It's responsibilities include adding new {@link WorkUnit}s and running
 *     them locally. To run a {@link WorkUnit}, a {@link Task} is first
 *     created based on it and the {@link Task} is scheduled and executed
 *     through the {@link TaskExecutor}.
 * </p>
 *
 * @author ynli
 */
@Deprecated
public class WorkUnitManager extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(WorkUnitManager.class);

  // This is used to store submitted work units
  private final BlockingQueue<WorkUnitState> workUnitQueue;

  // This is used to run the handler
  private final ExecutorService executorService;

  // This handler that handles running work units locally
  private final WorkUnitHandler workUnitHandler;

  public WorkUnitManager(TaskExecutor taskExecutor, TaskStateTracker taskStateTracker) {
    // We need a blocking queue to support the producer-consumer model
    // for managing the submission and execution of work units, and we
    // need a priority queue to support priority-based execution of
    // work units.
    this.workUnitQueue = Queues.newLinkedBlockingQueue();
    this.executorService = Executors.newSingleThreadExecutor(ExecutorsUtils.newThreadFactory(Optional.of(LOG)));
    this.workUnitHandler = new WorkUnitHandler(this.workUnitQueue, taskExecutor, taskStateTracker);
  }

  @Override
  protected void startUp()
      throws Exception {
    LOG.info("Starting the work unit manager");
    this.executorService.execute(this.workUnitHandler);
  }

  @Override
  protected void shutDown()
      throws Exception {
    LOG.info("Stopping the work unit manager");
    this.workUnitHandler.stop();
    this.executorService.shutdown();
  }

  /**
   * Add a collection of {@link WorkUnitState}s.
   *
   * @param workUnitStates the collection of {@link WorkUnitState}s to add
   */
  public void addWorkUnits(Collection<WorkUnitState> workUnitStates) {
    this.workUnitQueue.addAll(workUnitStates);
  }

  /**
   * Add a single {@link WorkUnitState}.
   *
   * @param workUnitState the {@link WorkUnitState} to add
   */
  public void addWorkUnit(WorkUnitState workUnitState) {
    this.workUnitQueue.add(workUnitState);
  }

  /**
   * A handler that does the actual work of running {@link WorkUnit}s locally.
   */
  private static class WorkUnitHandler implements Runnable {

    private final BlockingQueue<WorkUnitState> workUnitQueue;
    private final TaskExecutor taskExecutor;
    private final TaskStateTracker taskStateTracker;

    public WorkUnitHandler(BlockingQueue<WorkUnitState> workUnitQueue, TaskExecutor taskExecutor,
        TaskStateTracker taskStateTracker) {

      this.workUnitQueue = workUnitQueue;
      this.taskExecutor = taskExecutor;
      this.taskStateTracker = taskStateTracker;
    }

    // Tells if the handler is asked to stop
    private volatile boolean stopped = false;

    @Override
    @SuppressWarnings("unchecked")
    public void run() {
      while (!this.stopped) {
        try {
          // Take one work unit at a time from the queue
          WorkUnitState workUnitState = this.workUnitQueue.poll(5, TimeUnit.SECONDS);
          if (workUnitState == null) {
            continue;
          }

          // Create a task based off the work unit
          Task task = new Task(new TaskContext(workUnitState), this.taskStateTracker, taskExecutor,
              Optional.<CountDownLatch>absent());
          // And then execute the task
          this.taskExecutor.execute(task);
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
