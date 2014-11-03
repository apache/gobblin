package com.linkedin.uif.runtime;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.converter.Converter;
import com.linkedin.uif.converter.ForkOperator;
import com.linkedin.uif.metrics.JobMetrics;
import com.linkedin.uif.publisher.TaskPublisher;
import com.linkedin.uif.qualitychecker.row.RowLevelPolicyCheckResults;
import com.linkedin.uif.qualitychecker.row.RowLevelPolicyChecker;
import com.linkedin.uif.qualitychecker.task.TaskLevelPolicyCheckResults;
import com.linkedin.uif.qualitychecker.task.TaskLevelPolicyChecker;
import com.linkedin.uif.source.extractor.Extractor;
import com.linkedin.uif.util.ForkOperatorUtils;
import com.linkedin.uif.writer.DataWriter;
import com.linkedin.uif.writer.DataWriterBuilder;
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
    private final TaskState taskState;
    private final TaskStateTracker taskStateTracker;
    private final Optional<CountDownLatch> countDownLatch;

    // Data writers, one for each forked stream.
    private final List<DataWriter> writers = Lists.newArrayList();

    // Number of retries
    private volatile int retryCount = 0;

    /**
     * Instantiate a new {@link Task}.
     *
     * @param context Task context containing all necessary information
     *                to construct and run a {@link Task}
     */
    @SuppressWarnings("unchecked")
    public Task(TaskContext context, TaskStateTracker taskStateTracker,
                Optional<CountDownLatch> countDownLatch) {

        this.taskContext = context;
        this.taskState = context.getTaskState();
        this.jobId = this.taskState.getJobId();
        this.taskId = this.taskState.getTaskId();
        this.taskStateTracker = taskStateTracker;
        this.countDownLatch = countDownLatch;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() {
        long startTime = System.currentTimeMillis();
        this.taskState.setStartTime(startTime);
        this.taskStateTracker.registerNewTask(this);
        this.taskState.setWorkingState(WorkUnitState.WorkingState.RUNNING);

        // Number of forked branches. Default is 1, indicating there is no fork/branching.
        int branches = 1;

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
            if (doConversion) {
                converter = new MultiConverter(converterList);
                // Convert the source schema to a schema ready for the writer
                sourceSchema = converter.convertSchema(sourceSchema, this.taskState);
            }

            // Get the fork operator. By default the IdentityForkOperator is
            // used with a single branch to make the logic below simpler.
            ForkOperator forkOperator = closer.register(this.taskContext.getForkOperator());
            forkOperator.init(this.taskState);
            branches = forkOperator.getBranches(this.taskState);
            // Set fork.branches explicitly here so the rest task flow can pick it up
            this.taskState.setProp(ConfigurationKeys.FORK_BRANCHES_KEY, branches);
            List<Optional<Object>> forkedSchemas = forkOperator.forkSchema(this.taskState, sourceSchema);
            if (forkedSchemas.size() != branches) {
                throw new ForkBranchMismatchException(String.format(
                        "Number of forked schemas [%d] is not equal to number of branches [%d]",
                        forkedSchemas.size(), branches));
            }

            // Construct the row level policy checker
            RowLevelPolicyChecker rowChecker = closer.register(
                    this.taskContext.getRowLevelPolicyChecker(this.taskState));
            RowLevelPolicyCheckResults rowResults = new RowLevelPolicyCheckResults();

            // Build the writers (one per forked stream) for writing the output data records
            buildWriters(this.taskContext, branches, forkedSchemas);
            for (DataWriter writer : this.writers) {
                closer.register(writer);
            }

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

                // Perform quality checks and write out data if all quality checks pass
                if (convertedRecord != null && rowChecker.executePolicies(convertedRecord, rowResults)) {
                    // Fork the converted record
                    List<Optional<Object>> forkedRecords = forkOperator.forkDataRecord(
                            this.taskState, convertedRecord);
                    if (forkedRecords.size() != branches) {
                        throw new ForkBranchMismatchException(String.format(
                                "Number of forked data records [%d] is not equal to number of branches [%d]",
                                forkedSchemas.size(), branches));
                    }

                    // Write out the forked records
                    for (int i = 0; i < branches; i++) {
                        if (forkedRecords.get(i).isPresent()) {
                            this.writers.get(i).write(forkedRecords.get(i).get());
                        }
                    }
                }

                recordsPulled++;
                // Check if the pull limit is reached
                if (pullLimit > 0 && recordsPulled >= pullLimit) {
                    break;
                }
            }

            LOG.info("Row quality checker finished with results: " + rowResults.getResults());

            // Commit data of each forked stream
            for (int i = 0; i < branches; i++) {
                // Runs task level quality checking policies and checks their output
                boolean shouldCommit = checkDataQuality(recordsPulled, extractor.getExpectedRecordCount(),
                        pullLimit, i, forkedSchemas.get(i).get());
                if (shouldCommit) {
                    // Commit the data if all quality checkers pass
                    commitData(i);
                }
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

            for (int i = 0; i < branches; i++) {
                try {
                    this.writers.get(i).cleanup();
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
        for (DataWriter writer : this.writers) {
            this.taskState.updateRecordMetrics(writer.recordsWritten());
        }
    }

    /**
     * Update byte-level metrics.
     *
     * <p>
     *     This method is only supposed to be called after the writer commits.
     * </p>
     */
    public void updateByteMetrics() throws IOException {
        for (DataWriter writer : this.writers) {
            this.taskState.updateByteMetrics(writer.bytesWritten());
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
     * Mark the completion of this {@link Task}.
     */
    public void markTaskCompletion() {
        if (this.countDownLatch.isPresent()) {
            this.countDownLatch.get().countDown();
        }
    }

    @Override
    public String toString() {
        return this.taskId;
    }

    /**
     * Build a {@link DataWriter} for writing fetched data records.
     */
    @SuppressWarnings("unchecked")
    private void buildWriters(TaskContext context, int branches, List<Optional<Object>> schemas)
            throws IOException {

        // Clear the writer list so it starts with a fresh list of writers for each run/retry
        this.writers.clear();

        for (int i = 0; i < branches; i++) {
            // First create the right writer builder using the factory
            DataWriterBuilder builder = this.taskContext.getDataWriterBuilder(branches, i);

            String branchName = ForkOperatorUtils.getBranchName(
                    this.taskState, i, ConfigurationKeys.DEFAULT_FORK_BRANCH_NAME + i);
            this.taskState.setProp(
                    ForkOperatorUtils.getPropertyNameForBranch(
                            ConfigurationKeys.WRITER_FILE_PATH, branches, i),
                    ForkOperatorUtils.getPathForBranch(
                            this.taskState.getExtract().getOutputFilePath(), branchName, branches));

            // Then build the right writer using the builder
            DataWriter writer = builder
                    .writeTo(Destination.of(context.getDestinationType(branches, i), this.taskState))
                    .writeInFormat(context.getWriterOutputFormat(branches, i))
                    .withWriterId(this.taskId)
                    .withSchema(schemas.get(i).get())
                    .forBranch(branches > 1 ? i : -1)
                    .build();

            this.writers.add(writer);
        }
    }

    /**
     * Check data quality.
     *
     * @return whether data publishing is successful and data should be committed
     */
    private boolean checkDataQuality(long recordsPulled, long expectedCount, long pullLimit,
                                     int index, Object schema) throws Exception {

        // Do overall quality checking and publish task data
        if (pullLimit > 0) {
            // If pull limit is set, use the actual number of records pulled
            this.taskState.setProp(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED, recordsPulled);
        } else {
            // Otherwise use the expected record count
            this.taskState.setProp(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED, expectedCount);
        }
        this.taskState.setProp(ConfigurationKeys.WRITER_ROWS_WRITTEN, this.writers.get(index).recordsWritten());
        this.taskState.setProp(ConfigurationKeys.EXTRACT_SCHEMA, schema.toString());

        TaskLevelPolicyChecker policyChecker = this.taskContext.getTaskLevelPolicyChecker(this.taskState);
        TaskLevelPolicyCheckResults taskResults = policyChecker.executePolicies();
        TaskPublisher publisher = this.taskContext.geTaskPublisher(this.taskState, taskResults);

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
    private void commitData(int index) {
        LOG.info(String.format("Committing data of branch %d of task %s", index, this.taskId));
        try {
            this.writers.get(index).commit();
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
