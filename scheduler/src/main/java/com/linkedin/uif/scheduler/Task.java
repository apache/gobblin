package com.linkedin.uif.scheduler;

import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.converter.Converter;
import com.linkedin.uif.converter.MultiConverter;
import com.linkedin.uif.source.extractor.Extractor;
import com.linkedin.uif.writer.*;

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
        DataWriter writer = null;

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
                schemaForWriter = converter.convertSchema(
                        sourceSchema, this.taskState.getWorkunit());
            }
            // Build the writer for writing the output of the extractor
            writer = buildWriter(this.taskContext, schemaForWriter);

            this.taskState.setWorkingState(WorkUnitState.WorkingState.WORKING);
            this.taskStateTracker.registerNewTask(this);

            // Extract and write data records
            Object record;
            // Read one source record at a time
            while ((record = extractor.readRecord()) != null) {
                // Apply the converters first if applicable
                if (doConversion) {
                    record = converter.convertRecord(
                            sourceSchema, record, this.taskState.getWorkunit());
                }
                // Finally write the record
                writer.write(record);
            }

            // Do overall quality checking

            this.taskState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
        } catch (IOException ioe) {
            LOG.error(String.format("Task %s failed", this.taskId), ioe);
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

            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException ioe) {
                    // Ignored
                }
            }

            this.taskStateTracker.onTaskCompletion(this);
        }
    }

    /**
     * Get the ID of the job this {@link Task} belongs to.
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
                .withWriterId(context.getTaskState().getTaskId())
                .useSchemaConverter(context.getSchemaConverter())
                .useDataConverter(context.getDataConverter(schema))
                .withSourceSchema(schema, context.getSchemaType())
                .build();
    }
}
