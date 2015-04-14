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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.fork.CopyNotSupportedException;
import gobblin.fork.Copyable;
import gobblin.fork.ForkOperator;
import gobblin.qualitychecker.row.RowLevelPolicyCheckResults;
import gobblin.qualitychecker.row.RowLevelPolicyChecker;
import gobblin.source.extractor.Extractor;


/**
 * A physical unit of execution for a Gobblin {@link gobblin.source.workunit.WorkUnit}.
 *
 * <p>
 *     Each task is executed by a single thread in a thread pool managed by the {@link TaskExecutor}
 *     and each {@link Fork} of the task is executed in a separate thread pool also managed by the
 *     {@link TaskExecutor}.
 *
 *     Each {@link Task} consists of the following steps:
 *     <ul>
 *       <li>Extracting, converting, and forking the source schema.</li>
 *       <li>Extracting, converting, doing row-level quality checking, and forking each data record.</li>
 *       <li>Putting each forked record into the record queue managed by each {@link Fork}.</li>
 *       <li>Committing output data of each {@link Fork} once all {@link Fork}s finish.</li>
 *       <li>Cleaning up and exiting.</li>
 *     </ul>
 *
 *     Each {@link Fork} consists of the following steps:
 *     <ul>
 *       <li>Getting the next record off the record queue.</li>
 *       <li>Converting the record and doing row-level quality checking if applicable.</li>
 *       <li>Writing the record out if it passes the quality checking.</li>
 *       <li>Cleaning up and exiting once all the records have been processed.</li>
 *     </ul>
 * </p>
 *
 * @author ynli
 */
public class Task implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(Task.class);

  private final String jobId;
  private final String taskId;
  private final TaskContext taskContext;
  private final TaskState taskState;
  private final TaskStateTracker taskStateTracker;
  private final TaskExecutor taskExecutor;
  private final Optional<CountDownLatch> countDownLatch;

  private final List<Optional<Fork>> forks = Lists.newArrayList();

  // Number of task retries
  private volatile int retryCount = 0;

  /**
   * Instantiate a new {@link Task}.
   *
   * @param context a {@link TaskContext} containing all necessary information to construct and run a {@link Task}
   * @param taskStateTracker a {@link TaskStateTracker} for tracking task state
   * @param taskExecutor a {@link TaskExecutor} for executing the {@link Task} and its {@link Fork}s
   * @param countDownLatch an optional {@link java.util.concurrent.CountDownLatch} used to signal the task completion
   */
  @SuppressWarnings("unchecked")
  public Task(TaskContext context, TaskStateTracker taskStateTracker, TaskExecutor taskExecutor,
      Optional<CountDownLatch> countDownLatch) {
    this.taskContext = context;
    this.taskState = context.getTaskState();
    this.jobId = this.taskState.getJobId();
    this.taskId = this.taskState.getTaskId();
    this.taskStateTracker = taskStateTracker;
    this.taskExecutor = taskExecutor;
    this.countDownLatch = countDownLatch;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void run() {
    long startTime = System.currentTimeMillis();
    this.taskState.setStartTime(startTime);
    this.taskState.setWorkingState(WorkUnitState.WorkingState.RUNNING);

    // Clear the list so it starts with a fresh list of forks for each run/retry
    this.forks.clear();

    Closer closer = Closer.create();
    try {
      // Build the extractor for extracting source schema and data records
      Extractor extractor = closer.register(new ExtractorDecorator(
              new SourceDecorator(this.taskContext.getSource(), this.jobId, LOG).getExtractor(this.taskState),
              this.taskId, LOG));

      Converter converter = new MultiConverter(this.taskContext.getConverters());

      // Get the fork operator. By default IdentityForkOperator is used with a single branch.
      ForkOperator forkOperator = closer.register(this.taskContext.getForkOperator());
      forkOperator.init(this.taskState);
      int branches = forkOperator.getBranches(this.taskState);
      // Set fork.branches explicitly here so the rest task flow can pick it up
      this.taskState.setProp(ConfigurationKeys.FORK_BRANCHES_KEY, branches);

      // Extract, convert, and fork the source schema.
      Object schema = converter.convertSchema(extractor.getSchema(), this.taskState);
      List<Boolean> forkedSchemas = forkOperator.forkSchema(this.taskState, schema);
      if (forkedSchemas.size() != branches) {
        throw new ForkBranchMismatchException(String.format(
            "Number of forked schemas [%d] is not equal to number of branches [%d]", forkedSchemas.size(), branches));
      }

      if (inMultipleBranches(forkedSchemas) && !(schema instanceof Copyable)) {
        throw new CopyNotSupportedException(schema + " is not copyable");
      }

      // Used for the main branch to wait for all forks to finish
      CountDownLatch forkCountDownLatch = new CountDownLatch(branches);

      // Create one fork for each forked branch
      for (int i = 0; i < branches; i++) {
        if (forkedSchemas.get(i)) {
          Fork fork = closer.register(
              new Fork(this.taskContext, this.taskState, branches > 1 ? ((Copyable) schema).copy() : schema,
                  branches, i, forkCountDownLatch));
          // Schedule the fork to run
          this.taskExecutor.submit(fork);
          this.forks.add(Optional.of(fork));
        } else {
          this.forks.add(Optional.<Fork>absent());
        }
      }

      // Build the row-level quality checker
      RowLevelPolicyChecker rowChecker = closer.register(this.taskContext.getRowLevelPolicyChecker(this.taskState));
      RowLevelPolicyCheckResults rowResults = new RowLevelPolicyCheckResults();

      long pullLimit = this.taskState.getPropAsLong(ConfigurationKeys.EXTRACT_PULL_LIMIT, 0);
      long recordsPulled = 0;
      Object record = null;
      // Extract, convert, and fork one source record at a time.
      while ((pullLimit <= 0 || recordsPulled < pullLimit) && (record = extractor.readRecord(record)) != null) {
        recordsPulled++;
        for (Object convertedRecord : converter.convertRecord(schema, record, this.taskState)) {
          processRecord(convertedRecord, forkOperator, rowChecker, rowResults, branches);
        }
      }

      LOG.info("Extracted " + recordsPulled + " data records");
      LOG.info("Row quality checker finished with results: " + rowResults.getResults());

      if (pullLimit > 0) {
        // If pull limit is set, use the actual number of records pulled.
        this.taskState.setProp(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED, recordsPulled);
      } else {
        // Otherwise use the expected record count
        this.taskState.setProp(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED, extractor.getExpectedRecordCount());
      }

      for (Optional<Fork> fork : this.forks) {
        if (fork.isPresent()) {
          // Tell the fork that the main branch is done and no new incoming data records should be expected
          fork.get().markParentTaskDone();
        } else {
          forkCountDownLatch.countDown();
        }
      }

      // Wait for all forks to finish
      forkCountDownLatch.await();

      // Check if all forks succeeded
      boolean allForksSucceeded = true;
      for (Optional<Fork> fork : this.forks) {
        if (fork.isPresent()) {
          if (fork.get().isSucceeded()) {
            fork.get().commit();
          } else {
            allForksSucceeded = false;
          }
        }
      }

      if (allForksSucceeded) {
        // Set the task state to SUCCESSFUL. The state is not set to COMMITTED
        // as the data publisher will do that upon successful data publishing.
        this.taskState.setWorkingState(WorkUnitState.WorkingState.SUCCESSFUL);
      } else {
        LOG.error(String.format("Not all forks of task %s succeeded", this.taskId));
        this.taskState.setWorkingState(WorkUnitState.WorkingState.FAILED);
      }
    } catch (Throwable t) {
      LOG.error(String.format("Task %s failed", this.taskId), t);
      this.taskState.setWorkingState(WorkUnitState.WorkingState.FAILED);
      this.taskState.setProp(ConfigurationKeys.TASK_FAILURE_EXCEPTION_KEY, t.toString());
    } finally {
      try {
        closer.close();
      } catch (Throwable t) {
        LOG.error("Failed to close all open resources", t);
      }

      long endTime = System.currentTimeMillis();
      this.taskState.setEndTime(endTime);
      this.taskState.setTaskDuration(endTime - startTime);
      this.taskStateTracker.onTaskCompletion(this);
    }
  }

  /** Get the ID of the job this {@link Task} belongs to.
   *
   * @return ID of the job this {@link Task} belongs to.
   */
  public String getJobId() {
    return this.jobId;
  }

  /**
   * Get the ID of this task.
   *
   * @return ID of this task
   */
  public String getTaskId() {
    return this.taskId;
  }

  /**
   * Get the {@link TaskContext} associated with this task.
   *
   * @return {@link TaskContext} associated with this task
   */
  public TaskContext getTaskContext() {
    return this.taskContext;
  }

  /**
   * Get the state of this task.
   *
   * @return state of this task
   */
  public TaskState getTaskState() {
    return this.taskState;
  }

  /**
   * Get the list of {@link Fork}s created by this {@link Task}.
   *
   * @return the list of {@link Fork}s created by this {@link Task}
   */
  public List<Optional<Fork>> getForks() {
    return ImmutableList.copyOf(this.forks);
  }

  /**
   * Update record-level metrics.
   */
  public void updateRecordMetrics() {
    for (Optional<Fork> fork : this.forks) {
      if (fork.isPresent()) {
        fork.get().updateRecordMetrics();
      }
    }
  }

  /**
   * Update byte-level metrics.
   *
   * <p>
   *     This method is only supposed to be called after the writer commits.
   * </p>
   */
  public void updateByteMetrics() {
    try {
      for (Optional<Fork> fork : this.forks) {
        if (fork.isPresent()) {
          fork.get().updateByteMetrics();
        }
      }
    } catch (IOException ioe) {
      LOG.error("Failed to update byte-level metrics for task " + this.taskId);
    }
  }

  /**
   * Increment the retry count of this task.
   */
  public void incrementRetryCount() {
    this.retryCount++;
  }

  /**
   * Get the number of times this task has been retried.
   *
   * @return number of times this task has been retried
   */
  public int getRetryCount() {
    return this.retryCount;
  }

  /**
   * Mark the completion of this {@link Task}.
   */
  public void markTaskCompletion() {
    if (this.countDownLatch.isPresent()) {
      this.countDownLatch.get().countDown();
    }
    this.taskState.setProp(ConfigurationKeys.TASK_RETRIES_KEY, this.retryCount);
  }

  @Override
  public String toString() {
    return this.taskId;
  }

  /**
   * Process a (possibly converted) record.
   */
  @SuppressWarnings("unchecked")
  private void processRecord(Object convertedRecord, ForkOperator forkOperator, RowLevelPolicyChecker rowChecker,
      RowLevelPolicyCheckResults rowResults, int branches)
      throws Exception {
    // Skip the record if quality checking fails
    if (!rowChecker.executePolicies(convertedRecord, rowResults)) {
      return;
    }

    List<Boolean> forkedRecords = forkOperator.forkDataRecord(this.taskState, convertedRecord);
    if (forkedRecords.size() != branches) {
      throw new ForkBranchMismatchException(String
          .format("Number of forked data records [%d] is not equal to number of branches [%d]", forkedRecords.size(),
              branches));
    }

    boolean makesCopy = inMultipleBranches(forkedRecords);
    if (makesCopy && !(convertedRecord instanceof Copyable)) {
      throw new CopyNotSupportedException(convertedRecord + " is not copyable");
    }

    // If the record has been successfully put into the queues of every forks
    boolean allPutsSucceeded = false;
    // Use an array of primitive boolean type to avoid unnecessary boxing/unboxing
    boolean[] succeededPuts = new boolean[branches];
    // Put the record into the record queue of each fork. A put may timeout and return a false, in which
    // case the put needs to be retried in the next iteration along with other failed puts. This goes on
    // until all puts succeed, at which point the task moves to the next record.
    while (!allPutsSucceeded) {
      allPutsSucceeded = true;
      for (int i = 0; i < branches; i++) {
        if (succeededPuts[i]) {
          continue;
        }
        if (this.forks.get(i).isPresent() && forkedRecords.get(i)) {
          boolean succeeded =
              this.forks.get(i).get().putRecord(makesCopy ? ((Copyable) convertedRecord).copy() : convertedRecord);
          succeededPuts[i] = succeeded;
          if (!succeeded) {
            allPutsSucceeded = false;
          }
        } else {
          succeededPuts[i] = true;
        }
      }
    }
  }

  /**
   * Check if a schema or data record is being passed to more than one branches.
   */
  private boolean inMultipleBranches(List<Boolean> branches) {
    int inBranches = 0;
    for (Boolean bool : branches) {
      if (bool && ++inBranches > 1) {
        break;
      }
    }
    return inBranches > 1;
  }
}
