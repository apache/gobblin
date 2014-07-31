package com.linkedin.uif.runtime;

import java.io.IOException;

import com.linkedin.uif.metrics.JobMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.converter.Converter;
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

    private Extractor extractor;
    private DataWriter writer;
    private TaskPublisher publisher;
    private boolean shouldCommit = false;

    // Number of retries
    private int retryCount = 0;

    /**
     * Instantitate a new {@link Task}.
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

        RowLevelPolicyChecker rowChecker = null;
        boolean writerClosed = false;
        try {
            // Build the extractor for pulling source schema and data records
            this.extractor = new ExtractorDecorator(
                    new SourceDecorator(this.taskContext.getSource(), this.jobId, LOG)
                            .getExtractor(this.taskState),
                    this.taskId, LOG);

            // Original source schema
            Object sourceSchema = this.extractor.getSchema();

            // If conversion is needed on the source schema and data records
            // before they are passed to the writer
            boolean doConversion = !this.taskContext.getConverters().isEmpty();
            Converter converter = null;
            // (Possibly converted) source schema ready for the writer
            Object schemaForWriter = sourceSchema;
            if (doConversion) {
                converter = new MultiConverter(this.taskContext.getConverters());
                // Convert the source schema to a schema ready for the writer
                schemaForWriter = converter.convertSchema(sourceSchema, this.taskState);
            }
                        
            // Construct the row level policy checker
            rowChecker = buildRowLevelPolicyChecker(this.taskState);
            RowLevelPolicyCheckResults rowResults = new RowLevelPolicyCheckResults();
            
            // Build the writer for writing the output of the extractor
            this.writer = buildWriter(this.taskContext, schemaForWriter);

            this.taskState.setWorkingState(WorkUnitState.WorkingState.WORKING);
            this.taskStateTracker.registerNewTask(this);

            long recordsPulled = 0;
            long pullLimit = this.taskState.getPropAsLong(ConfigurationKeys.EXTRACT_PULL_LIMIT, 0);
            Object record;
            // Read one source record at a time
            while ((record = this.extractor.readRecord()) != null) {
                // Apply the converters first if applicable
                if (doConversion) {
                    record = converter.convertRecord(sourceSchema, record, this.taskState);
                }

                // Do quality checks
                if (rowChecker.executePolicies(record, rowResults)) {
                    // Finally write the record
                    this.writer.write(record);
                }

                recordsPulled++;
                // Check if the pull limit is reached
                if (pullLimit > 0 && recordsPulled >= pullLimit) {
                    break;
                }
            }

            LOG.info("Row quality checker finished with results: " + rowResults.getResults());

            // Attempt to publish and commit the task data
            publishData(recordsPulled, pullLimit, schemaForWriter);
            // We need to close the writer before task data can be committed
            this.writer.close();
            writerClosed = true;
            commitData();
        } catch (Throwable t) {
            LOG.error(String.format("Task %s failed", this.taskId), t);
            this.taskState.setWorkingState(WorkUnitState.WorkingState.FAILED);
            this.taskState.setProp(ConfigurationKeys.TASK_FAILURE_EXCEPTION_KEY, t.toString());
        } finally {
            // Properly close all components and cleanup
            if (this.extractor != null) {
                this.extractor.close();
            }

            if (rowChecker != null) {
                try {
                    rowChecker.close();
                } catch (IOException ioe) {
                    LOG.error("Failed to close the row quality checker for task " + taskId, ioe);
                }
            }

            if (this.publisher != null) {
                try {
                    this.publisher.cleanup();
                } catch (Exception e) {
                    LOG.error("Failed to close the task publisher for task " + taskId, e);
                }
            }

            if (this.writer != null) {
                if (!writerClosed) {
                    // Close the writer if it has not been closed yet
                    try {
                        this.writer.close();
                    } catch (IOException ioe) {
                        LOG.error("Failed to close the writer for task " + taskId, ioe);
                    }
                }

                try {
                    this.writer.cleanup();
                } catch (IOException ioe) {
                    LOG.error("The writer failed to cleanup for task " + taskId, ioe);
                }
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
            LOG.error(String.format(
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
     *
     * @return newly built {@link DataWriter}
     */
    @SuppressWarnings("unchecked")
    private DataWriter buildWriter(TaskContext context, Object schema)
            throws IOException {

        // First create the right writer builder using the factory
        DataWriterBuilder builder = new DataWriterBuilderFactory()
                .newDataWriterBuilder(context.getWriterOutputFormat());

        // Then build the right writer using the builder
        return builder
                .writeTo(Destination.of(
                        context.getDestinationType(),
                        context.getDestinationProperties()))
                .writeInFormat(context.getWriterOutputFormat())
                .withWriterId(this.taskId)
                .useSchemaConverter(context.getSchemaConverter())
                .useDataConverter(context.getDataConverter(schema))
                .withSourceSchema(schema)
                .withFilePath(this.taskState.getExtract().getOutputFilePath())
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
     * Publish task data.
     */
    private void publishData(long recordsPulled, long pullLimit, Object schemaForWriter)
            throws Exception {

        // Do overall quality checking and publish task data
        if (pullLimit > 0) {
            // If pull limit is set, use the actual number of records pulled
            this.taskState.setProp(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED,
                    recordsPulled);
        } else {
            // Otherwise use the expected record count
            this.taskState.setProp(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED,
                    this.extractor.getExpectedRecordCount());
        }
        this.taskState.setProp(ConfigurationKeys.WRITER_ROWS_WRITTEN,
                this.writer.recordsWritten());
        this.taskState.setProp(ConfigurationKeys.EXTRACT_SCHEMA, schemaForWriter.toString());

        TaskLevelPolicyChecker policyChecker = buildTaskLevelPolicyChecker(this.taskState);
        TaskLevelPolicyCheckResults taskResults = policyChecker.executePolicies();

        this.publisher = buildTaskPublisher(this.taskState, taskResults);
        switch (this.publisher.canPublish()) {
            case SUCCESS:
                this.shouldCommit = true;
                this.taskState.setWorkingState(WorkUnitState.WorkingState.SUCCESSFUL);
                break;
            case CLEANUP_FAIL:
                LOG.error("Cleanup failed for task " + this.taskId);
                this.taskState.setWorkingState(WorkUnitState.WorkingState.FAILED);
                break;
            case POLICY_TESTS_FAIL:
                LOG.error("Not all quality checking passed for task " + this.taskId);
                this.taskState.setWorkingState(WorkUnitState.WorkingState.FAILED);
                break;
            case COMPONENTS_NOT_FINISHED:
                LOG.error("Not all components completed for task " + this.taskId);
                this.taskState.setWorkingState(WorkUnitState.WorkingState.FAILED);
                break;
            default:
                this.taskState.setWorkingState(WorkUnitState.WorkingState.FAILED);
                break;
        }
    }

    /**
     * Commit task data.
     */
    private void commitData() {
        if (this.shouldCommit) {
            LOG.info("Committing data of task " + this.taskId);
            try {
                this.writer.commit();
                // Change the state to COMMITTED after successful commit
                this.taskState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
                if (JobMetrics.isEnabled(this.taskState.getWorkunit())) {
                    // Update byte-level metrics after the writer commits
                    updateByteMetrics();
                }
            } catch (IOException ioe) {
                if (this.taskState.getWorkingState() != WorkUnitState.WorkingState.COMMITTED) {
                    LOG.error("Failed to commit data of task " + this.taskId, ioe);
                }
            }
        }
    }
}
