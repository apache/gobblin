package com.linkedin.uif.scheduler;

import java.io.IOException;
import java.io.Serializable;
import java.util.Timer;
import java.util.TimerTask;

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
 *     managed by the {@link TaskManager} and it consists of the following
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
    private final TaskManager taskManager;
    private final TaskState taskState;
    private final Timer statusReportingTimer;

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
    public Task(TaskContext context, TaskManager taskManager) {
        this.taskContext = context;
        // Task manager is used to register failed tasks
        this.taskManager = taskManager;
        this.taskState = context.getTaskState();
        this.jobId = this.taskState.getJobId();
        this.taskId = this.taskState.getTaskId();
        this.statusReportingTimer = new Timer();
        // Schedule the timer task to periodically report status/progress
        this.statusReportingTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                reportStatusAndProgress();
            }
        }, 0, context.getStatusReportingInterval());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() {
        boolean aborted = false;

        // Build all the necessary components before actually executing the task
        Extractor extractor = this.taskContext.getSource().getExtractor(this.taskState);
        Converter converter = new MultiConverter(this.taskContext.getConverters());
        Object schemaForWriter = converter.convertSchema(
                extractor.getSchema(), this.taskState.getWorkunit());
        DataWriter writer;
        try {
            writer = buildWriter(this.taskContext, schemaForWriter);
        } catch (IOException ioe) {
            LOG.error("Failed to build the writer", ioe);
            this.taskState.setWorkingState(WorkUnitState.WorkingState.FAILED);
            return;
        } finally {
            extractor.close();
        }

        this.taskState.setWorkingState(WorkUnitState.WorkingState.WORKING);

        try {
            // Extract and write data records
            Object record;
            // Read one source record at a time
            while ((record = extractor.readRecord()) != null) {
                // Apply the converters first
                Object convertedRecord = converter.convertRecord(
                        schemaForWriter, record, this.taskState.getWorkunit());
                // Finally write the record
                writer.write(convertedRecord);
            }

            // Do overall quality checking

            this.taskState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
            this.taskManager.onTaskSuccess(this);
        } catch (IOException ioe) {
            LOG.error(String.format("Task %s failed", this.toString()), ioe);
            this.taskState.setWorkingState(WorkUnitState.WorkingState.FAILED);
            try {
                this.taskManager.onTaskFailure(this);
            } catch (IOException ioe1) {
                aborted = true;
            }
        } finally {
            // Cleanup when the task completes or fails
            try {
                extractor.close();
            } catch (Exception ioe) {
                // Ignored
            }

            try {
                writer.close();
            } catch (IOException ioe) {
                // Ignored
            }

            if (aborted) {
                this.taskState.setWorkingState(WorkUnitState.WorkingState.ABORTED);
                try {
                    this.taskManager.onTaskAbortion(this);
                } catch (IOException ioe) {
                    // Ignored
                }
            }

            this.statusReportingTimer.cancel();
        }
    }

    /**
     * Abort this task.
     */
    public void abort() {
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
                .useSchemaConverter(context.getSchemaConverter())
                .useDataConverter(context.getDataConverter(schema))
                .withSourceSchema(schema, context.getSchemaType())
                .build();
    }

    /**
     * Report task status and progress.
     */
    private void reportStatusAndProgress() {

    }
}
