package com.linkedin.uif.runtime;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.converter.Converter;
import com.linkedin.uif.metrics.JobMetrics;
import com.linkedin.uif.publisher.TaskPublisher;
import com.linkedin.uif.publisher.TaskPublisherBuilder;
import com.linkedin.uif.publisher.TaskPublisherBuilderFactory;
import com.linkedin.uif.qualitychecker.row.RowLevelPolicyCheckResults;
import com.linkedin.uif.qualitychecker.row.RowLevelPolicyChecker;
import com.linkedin.uif.qualitychecker.row.RowLevelPolicyCheckerBuilder;
import com.linkedin.uif.qualitychecker.row.RowLevelPolicyCheckerBuilderFactory;
import com.linkedin.uif.qualitychecker.task.TaskLevelPolicyCheckResults;
import com.linkedin.uif.qualitychecker.task.TaskLevelPolicyChecker;
import com.linkedin.uif.qualitychecker.task.TaskLevelPolicyCheckerBuilder;
import com.linkedin.uif.qualitychecker.task.TaskLevelPolicyCheckerBuilderFactory;
import com.linkedin.uif.source.extractor.Extractor;
import com.linkedin.uif.writer.DataWriter;
import com.linkedin.uif.writer.DataWriterBuilder;
import com.linkedin.uif.writer.DataWriterBuilderFactory;
import com.linkedin.uif.writer.Destination;

/**
 * A physical unit of execution for a UIF work unit.
 *
 * <p>
 *     Each task will be executed by a single thread within a thread pool
 *     managed by the {@link TaskExecutor} and it consists of the following
 *     steps:
 *
 * <ul>
 *     <li>Extracting data records from the data source.</li>
 *     <li>Performing row-level quality checking.</li>
 *     <li>Writing extracted data records to the specified sink.</li>
 *     <li>
 *         Performing quality checking when all extracted data records
 *         have been written.
 *     </li>
 *     <li>Cleaning up when the task is completed or failed.</li>
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
    private final TaskStateTracker taskStateTracker;
    private final TaskState taskState;

    private volatile DataWriter writer;

    // Number of retries
    private int retryCount = 0;

    /**
     * Instantiate a new {@link Task}.
     *
     * @param context Task context containing all necessary information
     *                to construct and run a {@link Task}
     */
    @SuppressWarnings("unchecked")
    public Task(TaskContext context, TaskStateTracker taskStateTracker) {
        this.taskContext = context;
        // Task manager is used to register failed tasks
        this.taskStateTracker = taskStateTracker;
        this.taskState = context.getTaskState();
        this.jobId = this.taskState.getJobId();
        this.taskId = this.taskState.getTaskId();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() {
        long startTime = System.currentTimeMillis();
        this.taskState.setStartTime(startTime);
        this.taskStateTracker.registerNewTask(this);
        this.taskState.setWorkingState(WorkUnitState.WorkingState.RUNNING);

        Closer closer = Closer.create();
        try {
            // Build the extractor for pulling source schema and data records
            Extractor extractor = closer.register(new ExtractorDecorator(
                    new SourceDecorator(this.taskContext.getSource(), this.jobId, LOG)
                            .getExtractor(this.taskState),
                    this.taskId, LOG));

            // Original source schema
            Object sourceSchema = extractor.getSchema();

            List<Converter> converterList = this.taskContext.getConverters();
            // If conversion is needed on the source schema and data records
            // before they are passed to the writer
            boolean doConversion = !converterList.isEmpty();
            Converter converter = null;
            // (Possibly converted) source schema ready for the writer
            Object schemaForWriter = sourceSchema;
            if (doConversion) {
                converter = new MultiConverter(converterList);
                // Convert the source schema to a schema ready for the writer
                schemaForWriter = converter.convertSchema(sourceSchema, this.taskState);
            }

            // Construct the row level policy checker
            RowLevelPolicyChecker rowChecker = closer.register(buildRowLevelPolicyChecker(this.taskState));
            RowLevelPolicyCheckResults rowResults = new RowLevelPolicyCheckResults();

            // Build the writer for writing the output of the extractor
            buildWriter(this.taskContext, schemaForWriter);
            closer.register(this.writer);

            long pullLimit = this.taskState.getPropAsLong(ConfigurationKeys.EXTRACT_PULL_LIMIT, 0);
            long recordsPulled = 0;
            Object record = null;
            // Read one source record at a time
            while ((record = extractor.readRecord(record)) != null) {
                // Apply the converters first if applicable
                Object convertedRecord = record;
                if (doConversion) {
                  convertedRecord = converter.convertRecord(sourceSchema, record, this.taskState);
                }

                // Do quality checks
                if (convertedRecord != null && rowChecker.executePolicies(convertedRecord, rowResults)) {
                    // Finally write the record
                    this.writer.write(convertedRecord);
                }

                recordsPulled++;
                // Check if the pull limit is reached
                if (pullLimit > 0 && recordsPulled >= pullLimit) {
                    break;
                }
            }

            LOG.info("Row quality checker finished with results: " + rowResults.getResults());

            // Runs task level quality checking policies and checks their output
            boolean shouldCommit = checkDataQuality(recordsPulled,
                    extractor.getExpectedRecordCount(), pullLimit, schemaForWriter);
            if (shouldCommit) {
                // Commit the data if all quality checkers pass
                commitData();
            }
        } catch (Throwable t) {
            LOG.error(String.format("Task %s failed", this.taskId), t);
            this.taskState.setWorkingState(WorkUnitState.WorkingState.FAILED);
            this.taskState.setProp(ConfigurationKeys.TASK_FAILURE_EXCEPTION_KEY, t.toString());
        } finally {
            try {
                closer.close();
            } catch (IOException ioe) {
                LOG.error("Failed to close all open resources", ioe);
            }

            try {
                this.writer.cleanup();
            } catch (IOException ioe) {
                LOG.error("The writer failed to cleanup for task " + taskId, ioe);
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
        if (this.writer == null) {
            LOG.warn(String.format(
                    "Could not update record metrics for task %s: writer was not built",
                    this.taskId));
            return;
        }

        this.taskState.updateRecordMetrics(this.writer.recordsWritten());
    }

    /**
     * Update byte-level metrics.
     *
     * <p>
     *     This method is only supposed to be called after the writer commits.
     * </p>
     */
    public void updateByteMetrics() throws IOException {
        if (this.writer == null) {
            LOG.error(String.format(
                    "Could not update byte metrics for task %s: writer was not built",
                    this.taskId));
            return;
        }

        this.taskState.updateByteMetrics(this.writer.bytesWritten());
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

    @Override
    public String toString() {
        return this.taskId;
    }

    /**
     * Build a {@link DataWriter} for writing fetched data records.
     */
    @SuppressWarnings("unchecked")
    private void buildWriter(TaskContext context, Object schema) throws IOException {
        // First create the right writer builder using the factory
        DataWriterBuilder builder = new DataWriterBuilderFactory().newDataWriterBuilder(this.taskState);

        this.taskState.setProp(
                ConfigurationKeys.WRITER_FILE_PATH, this.taskState.getExtract().getOutputFilePath());

        // Then build the right writer using the builder
        this.writer = builder
                .writeTo(Destination.of(context.getDestinationType(), this.taskState))
                .writeInFormat(context.getWriterOutputFormat())
                .withWriterId(this.taskId)
                .withSchema(schema)
                .build();
    }

    /**
     * Build a {@link TaskLevelPolicyChecker} to execute all defined
     * {@link com.linkedin.uif.qualitychecker.task.TaskLevelPolicy}.
     *
     * @return a {@link TaskLevelPolicyChecker}
     */
    private TaskLevelPolicyChecker buildTaskLevelPolicyChecker(TaskState taskState) throws Exception {
        TaskLevelPolicyCheckerBuilder builder = new TaskLevelPolicyCheckerBuilderFactory()
                .newPolicyCheckerBuilder(taskState);
        return builder.build();
    }

    /**
     * Build a {@link RowLevelPolicyChecker} to execute all defined
     * {@link com.linkedin.uif.qualitychecker.row.RowLevelPolicy}.
     *
     * @return a {@link RowLevelPolicyChecker}
     */
    private RowLevelPolicyChecker buildRowLevelPolicyChecker(TaskState taskState) throws Exception {
        RowLevelPolicyCheckerBuilder builder = new RowLevelPolicyCheckerBuilderFactory()
                .newPolicyCheckerBuilder(taskState);
        return builder.build();
    }

    /**
     * Build a {@link TaskPublisher} to publish this {@link Task}'s data.
     *
     * @return a {@link TaskPublisher}
     */
    private TaskPublisher buildTaskPublisher(TaskState taskState, TaskLevelPolicyCheckResults results)
            throws Exception {

        TaskPublisherBuilder builder = new TaskPublisherBuilderFactory()
                .newTaskPublisherBuilder(taskState, results);
        return builder.build();
    }

    /**
     * Check data quality.
     *
     * @return whether data publishing is successful and data should be committed
     */
    private boolean checkDataQuality(long recordsPulled, long expectedCount,
                                     long pullLimit, Object schemaForWriter)
        throws Exception {

        // Do overall quality checking and publish task data
        if (pullLimit > 0) {
            // If pull limit is set, use the actual number of records pulled
            this.taskState.setProp(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED, recordsPulled);
        } else {
            // Otherwise use the expected record count
            this.taskState.setProp(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED, expectedCount);
        }
        this.taskState.setProp(ConfigurationKeys.WRITER_ROWS_WRITTEN, this.writer.recordsWritten());
        this.taskState.setProp(ConfigurationKeys.EXTRACT_SCHEMA, schemaForWriter.toString());

        TaskLevelPolicyChecker policyChecker = buildTaskLevelPolicyChecker(this.taskState);
        TaskLevelPolicyCheckResults taskResults = policyChecker.executePolicies();
        TaskPublisher publisher = buildTaskPublisher(this.taskState, taskResults);

        switch (publisher.canPublish()) {
            case SUCCESS:
                return true;
            case CLEANUP_FAIL:
                LOG.error("Cleanup failed for task " + this.taskId);
                break;
            case POLICY_TESTS_FAIL:
                LOG.error("Not all quality checking passed for task " + this.taskId);
                break;
            case COMPONENTS_NOT_FINISHED:
                LOG.error("Not all components completed for task " + this.taskId);
                break;
            default:
                break;
        }

        this.taskState.setWorkingState(WorkUnitState.WorkingState.FAILED);
        return false;
    }

    /**
     * Commit task data.
     */
    private void commitData() {
        LOG.info("Committing data of task " + this.taskId);
        try {
            this.writer.commit();
            // Change the state to SUCCESSFUL after successful commit. The state is not changed
            // to COMMITTED as the data publisher will do that upon successful data publishing.
            this.taskState.setWorkingState(WorkUnitState.WorkingState.SUCCESSFUL);
            if (JobMetrics.isEnabled(this.taskState.getWorkunit())) {
                // Update byte-level metrics after the writer commits
                updateByteMetrics();
            }
        } catch (IOException ioe) {
            if (this.taskState.getWorkingState() != WorkUnitState.WorkingState.SUCCESSFUL) {
                LOG.error("Failed to commit data of task " + this.taskId, ioe);
            }
        }
    }
}
