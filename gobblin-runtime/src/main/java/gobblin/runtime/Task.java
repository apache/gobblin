/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
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
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;

import gobblin.Constructs;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.fork.CopyNotSupportedException;
import gobblin.fork.Copyable;
import gobblin.fork.ForkOperator;
import gobblin.instrumented.extractor.InstrumentedExtractorBase;
import gobblin.instrumented.extractor.InstrumentedExtractorDecorator;
import gobblin.publisher.DataPublisher;
import gobblin.publisher.SingleTaskDataPublisher;
import gobblin.qualitychecker.row.RowLevelPolicyCheckResults;
import gobblin.qualitychecker.row.RowLevelPolicyChecker;
import gobblin.source.extractor.JobCommitPolicy;
import gobblin.state.ConstructState;


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
 * @author Yinan Li
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
  private final Map<Optional<Fork>, Optional<Future<?>>> forks = Maps.newLinkedHashMap();

  // Number of task retries
  private final AtomicInteger retryCount = new AtomicInteger();

  /**
   * Instantiate a new {@link Task}.
   *
   * @param context a {@link TaskContext} containing all necessary information to construct and run a {@link Task}
   * @param taskStateTracker a {@link TaskStateTracker} for tracking task state
   * @param taskExecutor a {@link TaskExecutor} for executing the {@link Task} and its {@link Fork}s
   * @param countDownLatch an optional {@link java.util.concurrent.CountDownLatch} used to signal the task completion
   */
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

    // Clear the map so it starts with a fresh set of forks for each run/retry
    this.forks.clear();

    Closer closer = Closer.create();
    Converter converter = null;
    InstrumentedExtractorBase extractor = null;
    RowLevelPolicyChecker rowChecker = null;
    try {
      extractor =
          closer.register(new InstrumentedExtractorDecorator<>(this.taskState, this.taskContext.getExtractor()));

      converter = closer.register(new MultiConverter(this.taskContext.getConverters()));

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

      // Create one fork for each forked branch
      for (int i = 0; i < branches; i++) {
        if (forkedSchemas.get(i)) {
          Fork fork = closer.register(new Fork(this.taskContext,
              schema instanceof Copyable ? ((Copyable) schema).copy() : schema, branches, i));
          // Run the Fork
          this.forks.put(Optional.of(fork), Optional.<Future<?>> of(this.taskExecutor.submit(fork)));
        } else {
          this.forks.put(Optional.<Fork> absent(), Optional.<Future<?>> absent());
        }
      }

      // Build the row-level quality checker
      rowChecker = closer.register(this.taskContext.getRowLevelPolicyChecker());
      RowLevelPolicyCheckResults rowResults = new RowLevelPolicyCheckResults();

      long recordsPulled = 0;
      Object record;
      // Extract, convert, and fork one source record at a time.
      while ((record = extractor.readRecord(null)) != null) {
        recordsPulled++;
        for (Object convertedRecord : converter.convertRecord(schema, record, this.taskState)) {
          processRecord(convertedRecord, forkOperator, rowChecker, rowResults, branches);
        }
      }

      LOG.info("Extracted " + recordsPulled + " data records");
      LOG.info("Row quality checker finished with results: " + rowResults.getResults());

      this.taskState.setProp(ConfigurationKeys.EXTRACTOR_ROWS_EXTRACTED, recordsPulled);
      this.taskState.setProp(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED, extractor.getExpectedRecordCount());

      for (Optional<Fork> fork : this.forks.keySet()) {
        if (fork.isPresent()) {
          // Tell the fork that the main branch is completed and no new incoming data records should be expected
          fork.get().markParentTaskDone();
        }
      }

      for (Optional<Future<?>> forkFuture : this.forks.values()) {
        if (forkFuture.isPresent()) {
          try {
            forkFuture.get().get();
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
        }
      }

      // Check if all forks succeeded
      boolean allForksSucceeded = true;
      for (Optional<Fork> fork : this.forks.keySet()) {
        if (fork.isPresent()) {
          if (fork.get().isSucceeded()) {
            if (!fork.get().commit()) {
              allForksSucceeded = false;
            }
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
      failTask(t);
    } finally {

      addConstructsFinalStateToTaskState(extractor, converter, rowChecker);

      this.taskState.setProp(ConfigurationKeys.WRITER_RECORDS_WRITTEN, getRecordsWritten());
      this.taskState.setProp(ConfigurationKeys.WRITER_BYTES_WRITTEN, getBytesWritten());

      try {
        closer.close();
      } catch (Throwable t) {
        LOG.error("Failed to close all open resources", t);
      }

      for (Map.Entry<Optional<Fork>, Optional<Future<?>>> forkAndFuture : this.forks.entrySet()) {
        if (forkAndFuture.getKey().isPresent() && forkAndFuture.getValue().isPresent()) {
          try {
            forkAndFuture.getValue().get().cancel(true);
          } catch (Throwable t) {
            LOG.error(String.format("Failed to cancel Fork \"%s\"", forkAndFuture.getKey().get()), t);
          }
        }
      }

      try {
        if (shouldPublishDataInTask()) {
          // If data should be published by the task, publish the data and set the task state to COMMITTED.
          // Task data can only be published after all forks have been closed by closer.close().
          publishTaskData();
          this.taskState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
        }
      } catch (IOException ioe) {
        failTask(ioe);
      } finally {
        long endTime = System.currentTimeMillis();
        this.taskState.setEndTime(endTime);
        this.taskState.setTaskDuration(endTime - startTime);
        this.taskStateTracker.onTaskCompletion(this);
      }
    }
  }

  private void failTask(Throwable t) {
    LOG.error(String.format("Task %s failed", this.taskId), t);
    this.taskState.setWorkingState(WorkUnitState.WorkingState.FAILED);
    this.taskState.setProp(ConfigurationKeys.TASK_FAILURE_EXCEPTION_KEY, Throwables.getStackTraceAsString(t));
  }

  /**
   * Whether the task should directly publish its output data to the final publisher output directory.
   *
   * <p>
   *   The task should publish its output data directly if {@link ConfigurationKeys#PUBLISH_DATA_AT_JOB_LEVEL}
   *   is set to false AND any of the following conditions is satisfied:
   *
   *   <ul>
   *     <li>The {@link JobCommitPolicy#COMMIT_ON_PARTIAL_SUCCESS} policy is used.</li>
   *     <li>The {@link JobCommitPolicy#COMMIT_SUCCESSFUL_TASKS} policy is used. and all {@link Fork}s of this
   *     {@link Task} succeeded.</li>
   *   </ul>
   * </p>
   */
  private boolean shouldPublishDataInTask() {
    boolean publishDataAtJobLevel = this.taskState.getPropAsBoolean(ConfigurationKeys.PUBLISH_DATA_AT_JOB_LEVEL,
        ConfigurationKeys.DEFAULT_PUBLISH_DATA_AT_JOB_LEVEL);
    if (publishDataAtJobLevel) {
      LOG.info(String.format("%s is true. Will publish data at the job level.",
          ConfigurationKeys.PUBLISH_DATA_AT_JOB_LEVEL));
      return false;
    }

    JobCommitPolicy jobCommitPolicy = JobCommitPolicy.getCommitPolicy(this.taskState);

    if (jobCommitPolicy == JobCommitPolicy.COMMIT_SUCCESSFUL_TASKS) {
      return this.taskState.getWorkingState() == WorkUnitState.WorkingState.SUCCESSFUL;
    }

    if (jobCommitPolicy == JobCommitPolicy.COMMIT_ON_PARTIAL_SUCCESS) {
      return true;
    }

    LOG.info("Will publish data at the job level with job commit policy: " + jobCommitPolicy);
    return false;
  }

  private void publishTaskData() throws IOException {
    Closer closer = Closer.create();
    try {
      Class<? extends DataPublisher> dataPublisherClass = getTaskPublisherClass();
      SingleTaskDataPublisher publisher =
          closer.register(SingleTaskDataPublisher.getInstance(dataPublisherClass, this.taskState));

      LOG.info("Publishing data from task " + this.taskId);
      publisher.publish(this.taskState);
    } catch (ClassCastException e) {
      LOG.error(String.format("To publish data in task, the publisher class must extend %s",
          SingleTaskDataPublisher.class.getSimpleName()), e);
      this.taskState.setTaskFailureException(e);
      throw closer.rethrow(e);
    } catch (Throwable t) {
      this.taskState.setTaskFailureException(t);
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  @SuppressWarnings("unchecked")
  private Class<? extends DataPublisher> getTaskPublisherClass() throws ReflectiveOperationException {
    if (this.taskState.contains(ConfigurationKeys.TASK_DATA_PUBLISHER_TYPE)) {
      return (Class<? extends DataPublisher>) Class
          .forName(this.taskState.getProp(ConfigurationKeys.TASK_DATA_PUBLISHER_TYPE));
    }
    return (Class<? extends DataPublisher>) Class.forName(
        this.taskState.getProp(ConfigurationKeys.DATA_PUBLISHER_TYPE, ConfigurationKeys.DEFAULT_DATA_PUBLISHER_TYPE));
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
    return ImmutableList.copyOf(this.forks.keySet());
  }

  /**
   * Update record-level metrics.
   */
  public void updateRecordMetrics() {
    for (Optional<Fork> fork : this.forks.keySet()) {
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
      for (Optional<Fork> fork : this.forks.keySet()) {
        if (fork.isPresent()) {
          fork.get().updateByteMetrics();
        }
      }
    } catch (IOException ioe) {
      LOG.error("Failed to update byte-level metrics for task " + this.taskId, ioe);
    }
  }

  /**
   * Increment the retry count of this task.
   */
  public void incrementRetryCount() {
    this.retryCount.incrementAndGet();
  }

  /**
   * Get the number of times this task has been retried.
   *
   * @return number of times this task has been retried
   */
  public int getRetryCount() {
    return this.retryCount.get();
  }

  /**
   * Mark the completion of this {@link Task}.
   */
  public void markTaskCompletion() {
    if (this.countDownLatch.isPresent()) {
      this.countDownLatch.get().countDown();
    }

    this.taskState.setProp(ConfigurationKeys.TASK_RETRIES_KEY, this.retryCount.get());
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
      RowLevelPolicyCheckResults rowResults, int branches) throws Exception {
    // Skip the record if quality checking fails
    if (!rowChecker.executePolicies(convertedRecord, rowResults)) {
      return;
    }

    List<Boolean> forkedRecords = forkOperator.forkDataRecord(this.taskState, convertedRecord);
    if (forkedRecords.size() != branches) {
      throw new ForkBranchMismatchException(
          String.format("Number of forked data records [%d] is not equal to number of branches [%d]",
              forkedRecords.size(), branches));
    }

    if (inMultipleBranches(forkedRecords) && !(convertedRecord instanceof Copyable)) {
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

      int branch = 0;
      for (Optional<Fork> fork : this.forks.keySet()) {
        if (succeededPuts[branch]) {
          branch++;
          continue;
        }
        if (fork.isPresent() && forkedRecords.get(branch)) {
          boolean succeeded = fork.get()
              .putRecord(convertedRecord instanceof Copyable ? ((Copyable<?>) convertedRecord).copy() : convertedRecord);
          succeededPuts[branch] = succeeded;
          if (!succeeded) {
            allPutsSucceeded = false;
          }
        } else {
          succeededPuts[branch] = true;
        }
        branch++;
      }
    }
  }

  /**
   * Check if a schema or data record is being passed to more than one branches.
   */
  private static boolean inMultipleBranches(List<Boolean> branches) {
    int inBranches = 0;
    for (Boolean bool : branches) {
      if (bool && ++inBranches > 1) {
        break;
      }
    }
    return inBranches > 1;
  }

  /**
   * Get the total number of records written by every {@link Fork}s of this {@link Task}.
   *
   * @return the number of records written by every {@link Fork}s of this {@link Task}
   */
  private long getRecordsWritten() {
    long recordsWritten = 0;
    for (Optional<Fork> fork : this.forks.keySet()) {
      recordsWritten += fork.get().getRecordsWritten();
    }
    return recordsWritten;
  }

  /**
   * Get the total number of bytes written by every {@link Fork}s of this {@link Task}.
   *
   * @return the number of bytes written by every {@link Fork}s of this {@link Task}
   */
  private long getBytesWritten() {
    long bytesWritten = 0;
    for (Optional<Fork> fork : this.forks.keySet()) {
      bytesWritten += fork.get().getBytesWritten();
    }
    return bytesWritten;
  }

  /**
   * Get the final state of each construct used by this task and add it to the {@link gobblin.runtime.TaskState}.
   * @param extractor the {@link gobblin.instrumented.extractor.InstrumentedExtractorBase} used by this task.
   * @param converter the {@link gobblin.converter.Converter} used by this task.
   * @param rowChecker the {@link RowLevelPolicyChecker} used by this task.
   */
  private void addConstructsFinalStateToTaskState(InstrumentedExtractorBase<?, ?> extractor,
      Converter<?, ?, ?, ?> converter, RowLevelPolicyChecker rowChecker) {
    ConstructState constructState = new ConstructState();
    if (extractor != null) {
      constructState.addConstructState(Constructs.EXTRACTOR, new ConstructState(extractor.getFinalState()));
    }
    if (converter != null) {
      constructState.addConstructState(Constructs.CONVERTER, new ConstructState(converter.getFinalState()));
    }
    if (rowChecker != null) {
      constructState.addConstructState(Constructs.ROW_QUALITY_CHECKER, new ConstructState(rowChecker.getFinalState()));
    }
    int forkIdx = 0;
    for (Optional<Fork> fork : this.forks.keySet()) {
      constructState.addConstructState(Constructs.FORK_OPERATOR, new ConstructState(fork.get().getFinalState()),
          Integer.toString(forkIdx));
      forkIdx++;
    }

    constructState.mergeIntoWorkUnitState(this.taskState);
  }
}
