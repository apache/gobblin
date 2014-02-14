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
 * A physical unit of execution for a UIF {@link com.linkedin.uif.source.workunit.WorkUnit}.
 *
 * <p>
 *     Each task will be executed by a single thread within a thread pool managed by the
 *     {@link TaskManager} and it consists of the following
 *     steps:
 *
 *     1. Extracting data records from the data source.
 *     2. Writing extracted data records to the specified sink.
 *     3. Performing quality checking when all extracted data records have been written.
 *     4. Cleaning up when the task is completed or failed.
 * </p>
 *
 * @author ynli
 */
public class Task implements Runnable, Serializable {

    private static final Log LOG = LogFactory.getLog(Task.class);

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

        }

        this.taskState.setWorkingState(WorkUnitState.WorkingState.WORKING);

        // Schema verification

        try {
            // Extract and write data records
            Object record;
            // Read one source record at a time
            while ((record = extractor.readRecord()) != null) {
                // Apply the converters first
                Object convertedRecord = converter.convertRecord(
                        schemaForWriter, record, this.taskState.getWorkunit());
                // Do quality checking on the converted record
                // Finally write the record
                writer.write(convertedRecord);
            }

            // Do overall quality checking

            this.taskState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
        } catch (IOException ioe) {
            LOG.error(String.format("Task %s failed", this.toString()), ioe);
            this.taskManager.addFailedTask(this);
            this.taskState.setWorkingState(WorkUnitState.WorkingState.FAILED);
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

            this.statusReportingTimer.cancel();
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
     * Build a {@link DataWriter} for writing fetched data records.
     *
     * @return newly built {@link DataWriter}
     */
    @SuppressWarnings("unchecked")
    private DataWriter buildWriter(TaskContext context, Object schema)
            throws IOException {

        DataWriterBuilder builder = new DataWriterBuilderFactory()
                .newDataWriterBuilder(context.getWriterOutputFormat());
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
