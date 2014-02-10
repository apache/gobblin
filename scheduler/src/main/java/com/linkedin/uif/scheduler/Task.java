package com.linkedin.uif.scheduler;

import com.linkedin.uif.writer.DataWriterBuilder;
import com.linkedin.uif.writer.Destination;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.linkedin.uif.extractor.model.Extractor;
import com.linkedin.uif.writer.DataWriter;

import java.io.IOException;
import java.io.Serializable;
import java.util.Timer;
import java.util.TimerTask;

/**
 * A physical unit of execution for a UIF
 * {@link com.linkedin.uif.extractor.inputsource.WorkUnit}.
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
 * @param <S> type of source schema representation
 * @param <D> type of source data record representation
 */
public class Task<S, D> implements Runnable, Serializable {

    private static final Log LOG = LogFactory.getLog(Task.class);

    private final TaskManager taskManager;

    private final Extractor<D, S> extractor;
    private final DataWriter<D>  writer;
    private final Timer statusReportingTimer;

    // Initial status is SUBMITTED
    private TaskStatus status = TaskStatus.SUBMITTED;

    // Number of retries
    private int retryCount = 0;

    /**
     * Instantitate a new {@link Task}.
     *
     * @param context Task context containing all necessary information
     *                to construct and run a {@link Task}
     * @throws IOException if there is anything wrong constructing the task.
     */
    public Task(TaskContext<S, D> context, TaskManager taskManager)
            throws IOException {

        this.taskManager = taskManager;
        this.extractor = buildExtractor(context);
        this.writer = buildWriter(context);
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
    public void run() {
        this.status = TaskStatus.RUNNING;

        try {
            // Extract and write data records
            D record;
            while ((record = this.extractor.read()) != null) {
                this.writer.write(record);
            }

            // Do quality checking

            this.status = TaskStatus.COMPLETED;
        } catch (IOException ioe) {
            LOG.error(String.format("Task %s failed", this.toString()), ioe);
            this.taskManager.addFailedTask(this);
        } finally {
            // Cleanup when the task completes or fails

            //this.extractor.
            try {
                this.writer.close();
            } catch (IOException ioe) {
                // Ignored
            }

            // Report final status

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
     * Build an {@link Extractor} used to extract data records from the source.
     *
     * @return newly built {@link Extractor}
     */
    private Extractor<D, S> buildExtractor(TaskContext<S, D> context) {
        return null;
    }

    /**
     * Build a {@link DataWriter} for writing fetched data records.
     *
     * @return newly built {@link DataWriter}
     */
    private DataWriter<D> buildWriter(TaskContext<S, D> context)
            throws IOException {

        return DataWriterBuilder.<S, D>newBuilder()
                .writeTo(Destination.of(
                        context.getDestinationType(),
                        context.getDestinationProperties()))
                .useDataConverter(context.getDataConverter())
                .useSchemaConverter(context.getSchemaConverter())
                .dataSchema(context.getSourceSchema(), context.getSchemaType())
                .build();
    }

    /**
     * Report task status and progress.
     */
    private void reportStatusAndProgress() {

    }
}
