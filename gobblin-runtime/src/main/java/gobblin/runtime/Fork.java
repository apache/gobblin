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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
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
 * A class representing a forked branch of operations in a task flow.
 *
 * <p>
 *     A {@link Fork} consists of the following steps:
 *
 *     <ul>
 *         <li>Converting the input forked schema if converters are specified.</li>
 *         <li>Converting each input forked record if converters are specified.</li>
 *         <li>Performing row-level quality checking on the converted record.</li>
 *         <li>Writing out the converted data record if it passes the quality checking.</li>
 *         <li>Performing task-level quality checking after all data records are processed.</li>
 *         <li>Committing data if the task-level quality checking passes.</li>
 *         <li>Cleaning up.</li>
 *     </ul>
 * </p>
 *
 * @author ynli
 */
@SuppressWarnings("unchecked")
public class Fork implements Closeable {

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

  private final Closer closer = Closer.create();

  public Fork(TaskContext taskContext, TaskState taskState, Object schema, int branches, int index)
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

    this.closer.register(this.rowLevelPolicyChecker);
    this.closer.register(this.writer);
  }

  public void processRecord(Object record)
      throws Exception {
    for (Object convertedRecord : this.converter.convertRecord(this.convertedSchema, record, this.taskState)) {
      if (this.rowLevelPolicyChecker.executePolicies(convertedRecord, this.rowLevelPolicyCheckingResult)) {
        this.writer.write(convertedRecord);
      }
    }
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
   * @param recordsPulled number of records pulled
   * @param expectedCount expected record count
   * @param pullLimit pull limit
   * @throws Exception
   */
  public void commit(long recordsPulled, long expectedCount, long pullLimit)
      throws Exception {
    if (checkDataQuality(recordsPulled, expectedCount, pullLimit, this.convertedSchema)) {
      // Commit data if all quality checkers pass. Again, not to catch the exception
      // it may throw so the exception gets propagated to the caller of this method.
      commitData();
    }
  }

  @Override
  public void close()
      throws IOException {
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
   * Check data quality.
   *
   * @return whether data publishing is successful and data should be committed
   */
  private boolean checkDataQuality(long recordsPulled, long expectedCount, long pullLimit, Object schema)
      throws Exception {

    if (pullLimit > 0) {
      // If pull limit is set, use the actual number of records pulled.
      this.taskState.setProp(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED, recordsPulled);
    } else {
      // Otherwise use the expected record count
      this.taskState.setProp(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED, expectedCount);
    }
    this.taskState.setProp(ConfigurationKeys.WRITER_ROWS_WRITTEN, this.writer.recordsWritten());
    this.taskState.setProp(ConfigurationKeys.EXTRACT_SCHEMA, schema.toString());

    try {
      // Do task-level quality checking
      TaskLevelPolicyCheckResults taskResults =
          this.taskContext.getTaskLevelPolicyChecker(this.taskState, this.branches > 1 ? this.index : -1)
              .executePolicies();
      TaskPublisher publisher =
          this.taskContext.getTaskPublisher(this.taskState, taskResults, this.branches > 1 ? this.index : -1);
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

      this.taskState.setWorkingState(WorkUnitState.WorkingState.FAILED);
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
    this.logger.info(String.format("Committing data of branch %d of task %s", this.index, this.taskId));
    // Not to catch the exception this may throw so it gets propagated
    this.writer.commit();
    // Change the state to SUCCESSFUL upon successful commit. The state is not changed
    // to COMMITTED as the data publisher will do that upon successful data publishing.
    this.taskState.setWorkingState(WorkUnitState.WorkingState.SUCCESSFUL);

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
}
