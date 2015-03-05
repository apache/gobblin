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
 *     Each task will be executed by a single thread within a thread pool managed by the
 *     {@link TaskExecutor} and it consists of the following steps:
 *
 * <ul>
 *     <li>Extracting, converting, and forking the source schema.</li>
 *     <li>Extracting, converting, doing row-level quality checking, and forking each data record.</li>
 *     <li>Processing the forked record in each forked branch in a {@link Fork} instance.</li>
 *     <li>Cleaning up and exiting.</li>
 * </ul>
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
  private final Optional<CountDownLatch> countDownLatch;

  private final List<Optional<Fork>> forks = Lists.newArrayList();

  // Number of task retries
  private volatile int retryCount = 0;

  /**
   * Instantiate a new {@link Task}.
   *
   * @param context a {@link TaskContext} containing all necessary information
   *                to construct and run a {@link Task}
   * @param taskStateTracker a {@link TaskStateTracker} for tracking task state
   */
  @SuppressWarnings("unchecked")
  public Task(TaskContext context, TaskStateTracker taskStateTracker, Optional<CountDownLatch> countDownLatch) {
    this.taskContext = context;
    this.taskState = context.getTaskState();
    this.jobId = this.taskState.getJobId();
    this.taskId = this.taskState.getTaskId();
    this.taskStateTracker = taskStateTracker;
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
        throw new ForkBranchMismatchException(String
            .format("Number of forked schemas [%d] is not equal to number of branches [%d]", forkedSchemas.size(),
                branches));
      }

      if (inMultipleBranches(forkedSchemas) && !(schema instanceof Copyable)) {
        throw new CopyNotSupportedException(schema + " is not copyable");
      }

      // Create one Fork for each forked branch
      for (int i = 0; i < branches; i++) {
        if (forkedSchemas.get(i)) {
          Fork fork = closer.register(
              new Fork(this.taskContext, this.taskState, branches > 1 ? ((Copyable) schema).copy() : schema,
                  branches, i));
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

      // Commit data of each forked branch
      for (Optional<Fork> fork : this.forks) {
        if (fork.isPresent()) {
          fork.get().commit(recordsPulled, extractor.getExpectedRecordCount(), pullLimit);
        }
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

    for (int i = 0; i < branches; i++) {
      if (this.forks.get(i).isPresent() && forkedRecords.get(i)) {
        this.forks.get(i).get().processRecord(makesCopy ? ((Copyable) convertedRecord).copy() : convertedRecord);
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
