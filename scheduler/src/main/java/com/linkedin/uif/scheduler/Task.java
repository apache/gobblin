package com.linkedin.uif.scheduler;

import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.converter.Converter;
import com.linkedin.uif.publisher.TaskPublisher;
import com.linkedin.uif.publisher.TaskPublisherBuilder;
import com.linkedin.uif.publisher.TaskPublisherBuilderFactory;
import com.linkedin.uif.qualitychecker.PolicyCheckResults;
import com.linkedin.uif.qualitychecker.PolicyChecker;
import com.linkedin.uif.qualitychecker.PolicyCheckerBuilder;
import com.linkedin.uif.qualitychecker.PolicyCheckerBuilderFactory;
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
public class Task implements Runnable, Serializable {

    private static final Log LOG = LogFactory.getLog(Task.class);

    private final String jobId;
    private final String taskId;
    private final TaskContext taskContext;
    private final TaskStateTracker taskStateTracker;
    private final TaskState taskState;

    private DataWriter writer;

    // Number of retries
    private int retryCount = 0;

    /**
     * Instantitate a new {@link Task}.
     *
     * @param context Task context containing all necessary information
     *                to construct and run a {@link Task}
     * @throws IOException if there is anything wrong constructing the task.
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
        Extractor extractor = null;
        TaskPublisher publisher = null;

        long startTime = System.currentTimeMillis();
        this.taskState.setStartTime(startTime);
        // Should the writer commit its output
        boolean shouldCommit = false;

        try {
            // Build the extractor for pulling source schema and data records
            extractor = this.taskContext.getSource().getExtractor(this.taskState);
            // If conversion is needed on the source schema and data records
            // before they are passed to the writer
            boolean doConversion = !this.taskContext.getConverters().isEmpty();
            // Original source schema
            Object sourceSchema = extractor.getSchema();
            Converter converter = null;
            // (Possibly converted) source schema ready for the writer
            Object schemaForWriter = sourceSchema;
            if (doConversion) {
                converter = new MultiConverter(this.taskContext.getConverters());
                // Convert the source schema to a schema ready for the writer
                schemaForWriter = converter.convertSchema(sourceSchema, this.taskState);
            }
            
            // Build the writer for writing the output of the extractor
            this.writer = buildWriter(this.taskContext, schemaForWriter);

            this.taskState.setWorkingState(WorkUnitState.WorkingState.WORKING);
            this.taskStateTracker.registerNewTask(this);

            // Extract and write data records
            Object record;
            // Read one source record at a time
            while ((record = extractor.readRecord()) != null) {
                // Apply the converters first if applicable
                if (doConversion) {
                    record = converter.convertRecord(sourceSchema, record, this.taskState);
                }
                // Finally write the record
                this.writer.write(record);
            }

            // Do overall quality checking and publish task data
            this.taskState.setProp(ConfigurationKeys.EXTRACTOR_ROWS_READ,
                    extractor.getExpectedRecordCount());
            this.taskState.setProp(ConfigurationKeys.WRITER_ROWS_WRITTEN,
                    this.writer.recordsWritten());
            this.taskState.setProp(ConfigurationKeys.EXTRACT_SCHEMA, schemaForWriter.toString());

            PolicyChecker policyChecker = buildPolicyChecker(this.taskState);
            PolicyCheckResults results = policyChecker.executePolicies();
            
            publisher = buildTaskPublisher(this.taskState, results);
            switch (publisher.canPublish()) {
            case SUCCESS:
                LOG.info("Committing data of task " + this.taskId);
                shouldCommit = true;
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
        } catch (Exception e) {
            LOG.error(String.format("Task %s failed", this.taskId), e);
            this.taskState.setWorkingState(WorkUnitState.WorkingState.FAILED);
        } finally {
            // Cleanup when the task completes or fails
            if (extractor != null) {
                try {
                    extractor.close();
                } catch (Exception ioe) {
                    // Ignored
                }
            }

            if (this.writer != null) {
                try {
                    // We need to close the writer before it can commit
                    this.writer.close();
                    if (shouldCommit) {
                        this.writer.commit();
                        // Change the state to COMMITTED after successful commit
                        this.taskState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
                        if (Metrics.isEnabled(this.taskState)) {
                            // Collect byte-level metrics after the writer commits
                            collectBytesMetrics();
                        }
                    }
                } catch (IOException ioe) {
                    // Ignored
                } finally {
                    try {
                        this.writer.cleanup();
                    } catch (IOException ioe) {
                        // Ignored
                    }
                }
            }

            if (publisher != null) {
                try {
                    publisher.cleanup();
                } catch (Exception e) {
                    // Ignored
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
     * Collect record-level metrics.
     */
    public void collectRecordMetrics() {
        Counter taskRecordCounter = Metrics.getCounter(Metrics.metricName(
                "task", this.taskId, "records"));
        Counter jobRecordCounter = Metrics.getCounter(Metrics.metricName(
                "job", this.jobId, "records"));
        Meter taskRecordMeter = Metrics.getMeter(Metrics.metricName(
                "task", this.taskId, "recordsPerSec"));
        Meter jobRecordMeter = Metrics.getMeter(Metrics.metricName(
                "job", this.jobId, "recordsPerSec"));

        long recordsWritten = this.writer.recordsWritten();
        // Update metrics at the record level
        long inc = recordsWritten - taskRecordCounter.getCount();
        taskRecordCounter.inc(inc);
        taskRecordMeter.mark(recordsWritten);
        synchronized (Metrics.class) {
            jobRecordCounter.inc(inc);
            jobRecordMeter.mark(jobRecordMeter.getCount() + inc);
        }
    }

    /**
     * Collect byte-level metrics.
     *
     * <p>
     *     This method is only supposed to be called after the writer commits.
     * </p>
     */
    public void collectBytesMetrics() throws IOException {
        Counter taskByteCounter = Metrics.getCounter(Metrics.metricName(
                "task", this.taskId, "bytes"));
        Counter jobByteCounter = Metrics.getCounter(Metrics.metricName(
                "job", this.jobId, "bytes"));
        Meter taskByteMeter = Metrics.getMeter(Metrics.metricName(
                "task", this.taskId, "bytesPerSec"));
        Meter jobByteMeter = Metrics.getMeter(Metrics.metricName(
                "job", this.jobId, "bytesPerSec"));

        long bytes = this.writer.bytesWritten();
        // Update metrics at the byte level ONLY after commit is done
        taskByteCounter.inc(bytes);
        jobByteCounter.inc(bytes);
        taskByteMeter.mark(bytes);
        jobByteMeter.mark(bytes);
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
     * Build a {@link PolicyChecker} to execute all defined
     * {@link com.linkedin.uif.qualitychecker.Policy}.
     *
     * @return a {@link PolicyChecker}
     */
    private PolicyChecker buildPolicyChecker(TaskState taskState) throws Exception {
        PolicyCheckerBuilder builder = new PolicyCheckerBuilderFactory()
                .newPolicyCheckerBuilder(taskState);
        return builder.build();
    }

    /**
     * Build a {@link TaskPublisher} to publish this {@link Task}'s data.
     *
     * @return a {@link TaskPublisher}
     */
    private TaskPublisher buildTaskPublisher(TaskState taskState, PolicyCheckResults results)
            throws Exception {

        TaskPublisherBuilder builder = new TaskPublisherBuilderFactory()
                .newTaskPublisherBuilder(taskState, results);
        return builder.build();
    }
}
