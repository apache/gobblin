package com.linkedin.uif.scheduler;

import java.io.IOException;
import java.io.Serializable;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.MetaStoreClient;
import com.linkedin.uif.configuration.MetaStoreClientBuilder;
import com.linkedin.uif.configuration.MetaStoreClientBuilderFactory;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.converter.Converter;
import com.linkedin.uif.converter.MultiConverter;
import com.linkedin.uif.publisher.TaskPublisher;
import com.linkedin.uif.publisher.TaskPublisherBuilder;
import com.linkedin.uif.publisher.TaskPublisherBuilderFactory;
import com.linkedin.uif.qualitychecker.PolicyCheckResults;
import com.linkedin.uif.qualitychecker.PolicyChecker;
import com.linkedin.uif.qualitychecker.PolicyCheckerBuilder;
import com.linkedin.uif.qualitychecker.PolicyCheckerBuilderFactory;
import com.linkedin.uif.source.extractor.Extractor;
import com.linkedin.uif.writer.*;

/**
 * A physical unit of execution for a UIF {@link WorkUnit}.
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
    public Task(String jobId, String taskId, TaskContext context,
                TaskManager taskManager) {

        this.jobId = jobId;
        this.taskId = taskId;
        this.taskContext = context;
        // Task manager is used to register failed tasks
        this.taskManager = taskManager;
        this.taskState = context.getTaskState();
        this.taskState.setTaskId(taskId);
        this.statusReportingTimer = new Timer();
        // Schedule the timer task to periodically report status/progress
        this.statusReportingTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                reportStatusAndProgress();
            }
        }, 0, context.getStatusReportingInterval());
    }

    /**
     * Get the ID of this task.
     *
     * @return ID of this task
     */
    public String getTaskId() {
        return this.taskId;
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

            // Do overall quality checking and publish task data
            this.taskState.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.EXTRACTOR_ROWS_READ, extractor.getExpectedRecordCount());
            this.taskState.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.WRITER_ROWS_WRITTEN, writer.recordsWritten());
            
            MetaStoreClient collector = buildMetaStoreClient(this.taskState);
            
            PolicyChecker policyChecker = buildPolicyChecker(this.taskState, collector);
            PolicyCheckResults results = policyChecker.executePolicies();

            TaskPublisher publisher = buildTaskPublisher(this.taskState, results, collector);
            
            // TODO Need a way to capture status of Publisher properly
            switch ( publisher.publish() ) {
            case SUCCESS:
                this.taskState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
                break;
            default:
                this.taskState.setWorkingState(WorkUnitState.WorkingState.FAILED);
                break;
            }
            
        } catch (Exception e) {
            LOG.error(String.format("Task %s failed", this.toString()), e);
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
            try {
                this.taskManager.getTaskTracker().reportTaskState(this.taskState);
            } catch (IOException ioe) {
                LOG.error("Failed to report task state for task " + this.taskId);
            }
        }
    }

    /**
     * Builds a {@link MetaStoreClient} to communicate with an external MetaStore
     * @return a {@link MetaStoreClient}
     */
    private MetaStoreClient buildMetaStoreClient(TaskState taskState) throws Exception {
        MetaStoreClientBuilder builder = new MetaStoreClientBuilderFactory().newMetaStoreClientBuilder(taskState);
        return builder.build();
    }
    
    /**
     * Builds a {@link PolicyChecker} to execute all the Policies
     * @return a {@link PolicyChecker}
     */
    private PolicyChecker buildPolicyChecker(TaskState taskState, MetaStoreClient collector) throws Exception {
        PolicyCheckerBuilder builder = new PolicyCheckerBuilderFactory().newPolicyCheckerBuilder(taskState, collector);
        return builder.build();
    }
    
    /**
     * Builds a {@link TaskPublisher} to publish this Task's data
     * @return a {@link TaskPublisher}
     */
    private TaskPublisher buildTaskPublisher(TaskState taskState, PolicyCheckResults results, MetaStoreClient collector) throws Exception {
        TaskPublisherBuilder builder = new TaskPublisherBuilderFactory().newTaskPublisherBuilder(taskState, results, collector);
        return builder.build();
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
