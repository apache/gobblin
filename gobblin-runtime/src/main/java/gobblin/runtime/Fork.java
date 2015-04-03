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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.metrics.JobMetrics;
import gobblin.publisher.TaskPublisher;
import gobblin.qualitychecker.row.RowLevelPolicyCheckResults;
import gobblin.qualitychecker.row.RowLevelPolicyChecker;
import gobblin.qualitychecker.task.TaskLevelPolicyCheckResults;
import gobblin.util.ForkOperatorUtils;
import gobblin.writer.DataWriter;
import gobblin.writer.Destination;


/**
 * A class representing a forked branch of operations of a {@link Task} flow. The {@link Fork}s of a
 * {@link Task} are executed in a thread pool managed by the {@link TaskExecutor}, which is different
 * from the thread pool used to execute {@link Task}s.
 *
 * <p>
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
@SuppressWarnings("unchecked")
public class Fork implements Closeable, Runnable {

  // Possible state of a fork
  enum ForkState {
    PENDING, RUNNING, SUCCEEDED, FAILED, COMMITTED
  }

  private final Logger logger;

  private final TaskContext taskContext;
  private final TaskState taskState;
  private final String taskId;

  private final int branches;
  private final int index;

  private final Converter converter;
  private final Object convertedSchema;
  private final RowLevelPolicyChecker rowLevelPolicyChecker;
  private final RowLevelPolicyCheckResults rowLevelPolicyCheckingResult;
  private final DataWriter<Object> writer;

  // This is used to signal the parent task of the completion of this fork
  private final CountDownLatch countDownLatch;

  // A bounded blocking queue in between the parent task and this fork
  private final BoundedBlockingRecordQueue<Object> recordQueue;

  private final Closer closer = Closer.create();

  // This is used by the parent task to signal that it has done pulling records and this fork
  // should not expect any new incoming data records. This is written by the parent task and
  // read by this fork. Since this flag will be updated by only a single thread and updates to
  // a boolean are atomic, volatile is sufficient here.
  private volatile boolean parentTaskDone = false;

  // Writes to and reads of references are always atomic according to the Java language specs.
  // An AtomicReference is still used here for the compareAntSet operation.
  private final AtomicReference<ForkState> forkState;

  public Fork(TaskContext taskContext, TaskState taskState, Object schema, int branches, int index,
      CountDownLatch countDownLatch)
      throws Exception {

    this.logger = LoggerFactory.getLogger(Fork.class.getName() + "-" + index);

    this.taskContext = taskContext;
    this.taskState = taskState;
    this.taskId = taskState.getTaskId();

    this.branches = branches;
    this.index = index;

    this.converter = new MultiConverter(this.taskContext.getConverters(this.index));
    this.convertedSchema = this.converter.convertSchema(schema, this.taskState);
    this.rowLevelPolicyChecker = this.taskContext.getRowLevelPolicyChecker(this.taskState, this.index);
    this.rowLevelPolicyCheckingResult = new RowLevelPolicyCheckResults();
    this.writer = buildWriter();

    this.recordQueue = BoundedBlockingRecordQueue.newBuilder()
        .hasCapacity(taskState.getPropAsInt(
            ConfigurationKeys.FORK_BRANCH_RECORD_QUEUE_CAPACITY_KEY,
            ConfigurationKeys.DEFAULT_FORK_BRANCH_RECORD_QUEUE_CAPACITY))
        .useTimeout(taskState.getPropAsLong(
            ConfigurationKeys.FORK_BRANCH_RECORD_QUEUE_TIMEOUT_KEY,
            ConfigurationKeys.DEFAULT_FORK_BRANCH_RECORD_QUEUE_TIMEOUT))
        .useTimeoutTimeUnit(TimeUnit.valueOf(taskState.getProp(
            ConfigurationKeys.FORK_BRANCH_RECORD_QUEUE_TIMEOUT_UNIT_KEY,
            ConfigurationKeys.DEFAULT_FORK_BRANCH_RECORD_QUEUE_TIMEOUT_UNIT)))
        .collectStats()
        .build();

    this.countDownLatch = countDownLatch;

    this.closer.register(this.rowLevelPolicyChecker);
    this.closer.register(this.writer);

    this.forkState = new AtomicReference<ForkState>(ForkState.PENDING);
  }

  @Override
  public void run() {
    compareAndSetForkState(ForkState.PENDING, ForkState.RUNNING);
    try {
      processRecords();
      compareAndSetForkState(ForkState.RUNNING, ForkState.SUCCEEDED);
    } catch (IOException ioe) {
      this.forkState.set(ForkState.FAILED);
      this.logger.error(
          String.format("Fork %d of task %s failed to process data records", this.index, this.taskId), ioe);
      Throwables.propagate(ioe);
    } catch (DataConversionException dce) {
      this.forkState.set(ForkState.FAILED);
      this.logger.error(
          String.format("Fork %d of task %s failed to convert data records", this.index, this.taskId), dce);
      Throwables.propagate(dce);
    } catch (Throwable t) {
      this.forkState.set(ForkState.FAILED);
      this.logger.error(String.format("Fork %d of task %s failed to process data records", this.index, this.taskId), t);
      Throwables.propagate(t);
    } finally {
      // Clear the queue and count down so the parent task knows this fork is done (succeeded or failed)
      this.recordQueue.clear();
      this.countDownLatch.countDown();
    }
  }

  /**
   * Put a new record into the record queue for this {@link Fork} to process.
   *
   * <p>
   *   This method is used by the {@link Task} that creates this {@link Fork}.
   * </p>
   *
   * @param record the new record
   * @return whether the record has been successfully put into the queue
   * @throws InterruptedException
   */
  public boolean putRecord(Object record) throws InterruptedException {
    if (this.forkState.compareAndSet(ForkState.FAILED, ForkState.FAILED)) {
      throw new IllegalStateException(
          String.format("Fork %d of task %s has failed and is no longer running", this.index, this.taskId));
    }
    return this.recordQueue.put(record);
  }

  /**
   * Tell this {@link Fork} that the parent task is already done pulling records and
   * it should not expect more incoming data records.
   *
   * <p>
   *   This method is used by the {@link Task} that creates this {@link Fork}.
   * </p>
   */
  public void markParentTaskDone() {
    this.parentTaskDone = true;
  }

  /**
   * Update record-level metrics.
   */
  public void updateRecordMetrics() {
    this.taskState.updateRecordMetrics(this.writer.recordsWritten(), this.index);
  }

  /**
   * Update byte-level metrics.
   *
   * <p>
   *     This method is only supposed to be called after the writer commits.
   * </p>
   */
  public void updateByteMetrics()
      throws IOException {
    this.taskState.updateByteMetrics(this.writer.bytesWritten(), this.index);
  }

  /**
   * Commit data of this {@link Fork}.
   *
   * @throws Exception if there is anything wrong committing the data
   */
  public void commit()
      throws Exception {
    try {
      if (checkDataQuality(this.convertedSchema)) {
        // Commit data if all quality checkers pass. Again, not to catch the exception
        // it may throw so the exception gets propagated to the caller of this method.
        this.logger.info(String.format("Committing data for fork %d of task %s", this.index, this.taskId));
        commitData();
        compareAndSetForkState(ForkState.SUCCEEDED, ForkState.COMMITTED);
      } else {
        this.logger.error(String.format("Fork %d of task %s failed to pass quality checking", this.index, this.taskId));
        compareAndSetForkState(ForkState.SUCCEEDED, ForkState.FAILED);
      }
    } catch (Throwable t) {
      this.logger.error(String.format("Fork %d of task %s failed to commit data", this.index, this.taskId), t);
      this.forkState.set(ForkState.FAILED);
      Throwables.propagate(t);
    }
  }

  /**
   * Get the ID of the task that creates this {@link Fork}.
   *
   * @return the ID of the task that creates this {@link Fork}
   */
  public String getTaskId() {
    return this.taskId;
  }

  /**
   * Get the branch index of this {@link Fork}.
   *
   * @return branch index of this {@link Fork}
   */
  public int getIndex() {
    return this.index;
  }

  /**
   * Get a {@link BoundedBlockingRecordQueue.QueueStats} object representing the record queue
   * statistics of this {@link Fork}.
   *
   * @return a {@link BoundedBlockingRecordQueue.QueueStats} object representing the record queue
   *         statistics of this {@link Fork} wrapped in an {@link com.google.common.base.Optional},
   *         which means it may be absent if collecting of queue statistics is not enabled.
   */
  public Optional<BoundedBlockingRecordQueue<Object>.QueueStats> queueStats() {
    return this.recordQueue.stats();
  }

  /**
   * Return if this {@link Fork} has succeeded processing all records.
   *
   * @return if this {@link Fork} has succeeded processing all records
   */
  public boolean isSucceeded() {
    return this.forkState.compareAndSet(ForkState.SUCCEEDED, ForkState.SUCCEEDED);
  }

  @Override
  public void close()
      throws IOException {
    // Tell this fork that the parent task is done. This is a second chance call if the parent
    // task failed and didn't do so through the normal way of calling markParentTaskDone().
    this.parentTaskDone = true;

    // Record the fork state into the task state that will be persisted into the state store
    this.taskState.setProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.FORK_STATE_KEY, this.index),
        this.forkState.get().name());
    try {
      this.closer.close();
    } finally {
      this.writer.cleanup();
    }
  }

  /**
   * Build a {@link gobblin.writer.DataWriter} for writing fetched data records.
   */
  @SuppressWarnings("unchecked")
  private DataWriter<Object> buildWriter()
      throws IOException, SchemaConversionException {
    String branchName = ForkOperatorUtils
        .getBranchName(this.taskState, this.index, ConfigurationKeys.DEFAULT_FORK_BRANCH_NAME + this.index);
    String writerFilePathKey =
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_PATH, this.branches, this.index);
    if (!this.taskState.contains(writerFilePathKey)) {
      this.taskState.setProp(writerFilePathKey, ForkOperatorUtils
          .getPathForBranch(this.taskState.getExtract().getOutputFilePath(), branchName, this.branches));
    }

    return this.taskContext.getDataWriterBuilder(this.branches, this.index)
        .writeTo(Destination.of(this.taskContext.getDestinationType(this.branches, this.index), this.taskState))
        .writeInFormat(this.taskContext.getWriterOutputFormat(this.branches, this.index)).withWriterId(this.taskId)
        .withSchema(this.convertedSchema).forBranch(this.branches > 1 ? this.index : -1).build();
  }

  /**
   * Get new records off the record queue and process them.
   */
  private void processRecords() throws IOException, DataConversionException {
    while (true) {
      try {
        Object record = this.recordQueue.get();
        if (record == null) {
          // The parent task has already done pulling records so no new record means this fork is done
          if (this.parentTaskDone) {
            return;
          }
        } else {
          // Convert the record, check its data quality, and finally write it out if quality checking passes.
          for (Object convertedRecord : this.converter.convertRecord(this.convertedSchema, record, this.taskState)) {
            if (this.rowLevelPolicyChecker.executePolicies(convertedRecord, this.rowLevelPolicyCheckingResult)) {
              this.writer.write(convertedRecord);
            }
          }
        }
      } catch (InterruptedException ie) {
        this.logger.warn("Interrupted while trying to get a record off the queue", ie);
        Throwables.propagate(ie);
      }
    }
  }

  /**
   * Check data quality.
   *
   * @return whether data publishing is successful and data should be committed
   */
  private boolean checkDataQuality(Object schema)
      throws Exception {
    TaskState taskStateForFork = this.taskState;
    if (this.branches > 1) {
      // Make a copy if there are more than one fork
      taskStateForFork = new TaskState(this.taskState);
    }

    taskStateForFork.setProp(ConfigurationKeys.WRITER_ROWS_WRITTEN, this.writer.recordsWritten());
    taskStateForFork.setProp(ConfigurationKeys.EXTRACT_SCHEMA, schema.toString());

    try {
      // Do task-level quality checking
      TaskLevelPolicyCheckResults taskResults =
          this.taskContext.getTaskLevelPolicyChecker(taskStateForFork, this.branches > 1 ? this.index : -1)
              .executePolicies();
      TaskPublisher publisher =
          this.taskContext.getTaskPublisher(taskStateForFork, taskResults, this.branches > 1 ? this.index : -1);
      switch (publisher.canPublish()) {
        case SUCCESS:
          return true;
        case CLEANUP_FAIL:
          this.logger.error("Cleanup failed for task " + this.taskId);
          break;
        case POLICY_TESTS_FAIL:
          this.logger.error("Not all quality checking passed for task " + this.taskId);
          break;
        case COMPONENTS_NOT_FINISHED:
          this.logger.error("Not all components completed for task " + this.taskId);
          break;
        default:
          break;
      }

      return false;
    } catch (Throwable t) {
      this.logger.error("Failed to check task-level data quality", t);
      return false;
    }
  }

  /**
   * Commit task data.
   */
  private void commitData()
      throws IOException {
    this.logger.info(String.format("Committing data of fork %d of task %s", this.index, this.taskId));
    // Not to catch the exception this may throw so it gets propagated
    this.writer.commit();

    try {
      if (JobMetrics.isEnabled(this.taskState.getWorkunit())) {
        // Update byte-level metrics after the writer commits
        updateByteMetrics();
      }
    } catch (IOException ioe) {
      // Trap the exception as failing to update metrics should not cause the task to fail
      this.logger.error("Failed to update byte-level metrics of task " + this.taskId);
    }
  }

  /**
   * Compare and set the state of this {@link Fork} to a new state if and only if the current state
   * is equal to the expected state.
   */
  private void compareAndSetForkState(ForkState expectedState, ForkState newState) {
    if (!this.forkState.compareAndSet(expectedState, newState)) {
      throw new IllegalStateException(String
          .format("Expected fork state %s; actual fork state %s", expectedState.name(), this.forkState.get().name()));
    }
  }
}
