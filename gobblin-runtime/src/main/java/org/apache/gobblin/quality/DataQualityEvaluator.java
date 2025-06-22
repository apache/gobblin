package org.apache.gobblin.quality;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.OpenTelemetryMetrics;
import org.apache.gobblin.metrics.OpenTelemetryMetricsBase;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.qualitychecker.DataQualityStatus;
import org.apache.gobblin.qualitychecker.task.TaskLevelPolicyChecker;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.util.PropertiesUtils;

import io.opentelemetry.api.common.Attributes;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Evaluates data quality for a set of task states and emits relevant metrics.
 * This is a stateless utility class.
 */
@Slf4j
public class DataQualityEvaluator {

    private static final String GAAS_OBSERVABILITY_METRICS_GROUPNAME = "gobblin.gaas.observability";

    // Private constructor to prevent instantiation
    private DataQualityEvaluator() {}

    /**
     * Result of a data quality evaluation containing the overall status and metrics.
     */
    @Getter
    public static class DataQualityEvaluationResult {
        private final DataQualityStatus qualityStatus;
        private final int totalFiles;
        private final int passedFiles;
        private final int failedFiles;
        // Number of files that were not evaluated for data quality for example files not found or not processed
        private final int nonEvaluatedFiles;

        public DataQualityEvaluationResult(DataQualityStatus qualityStatus, int totalFiles, int passedFiles, int failedFiles, int nonEvaluatedFiles) {
            this.qualityStatus = qualityStatus;
            this.totalFiles = totalFiles;
            this.passedFiles = passedFiles;
            this.failedFiles = failedFiles;
            this.nonEvaluatedFiles = nonEvaluatedFiles;
        }

        public DataQualityEvaluationResult(DataQualityStatus qualityStatus, int totalFiles, int passedFiles, int failedFiles) {
            this(qualityStatus, totalFiles, passedFiles, failedFiles, 0);
        }
    }

    /**
     * Evaluates the data quality of a dataset state and stores the result.
     * This method is specifically designed for dataset-level quality evaluation.
     *
     * @param datasetState The dataset state to evaluate and update
     * @param jobState The job state containing additional context
     * @return DataQualityEvaluationResult containing the evaluation results
     */
    public static DataQualityEvaluationResult evaluateAndReportDatasetQuality(JobState.DatasetState datasetState, JobState jobState) {
        List<TaskState> taskStates = datasetState.getTaskStates();
        DataQualityEvaluationResult result = evaluateDataQuality(taskStates, jobState);

        // Store the result in the dataset state
        jobState.setProp(ConfigurationKeys.DATASET_QUALITY_STATUS_KEY, result.getQualityStatus().name());
        // Emit dataset-specific metrics
        emitMetrics(jobState, result.getQualityStatus(), result.getTotalFiles(),
            result.getPassedFiles(), result.getFailedFiles(), result.nonEvaluatedFiles, datasetState.getDatasetUrn());

        return result;
    }

    /**
     * Evaluates the data quality of a set of task states and emits relevant metrics.
     *
     * @param taskStates List of task states to evaluate
     * @param jobState The job state containing additional context
     * @return DataQualityEvaluationResult containing the evaluation results
     */
    public static DataQualityEvaluationResult evaluateDataQuality(List<TaskState> taskStates, JobState jobState) {
        DataQualityStatus jobDataQuality = DataQualityStatus.PASSED;
        int totalFiles = 0;
        int failedFilesSize = 0;
        int passedFilesSize = 0;
        int nonEvaluatedFilesSize = 0;

        for (TaskState taskState : taskStates) {
            totalFiles++;
            DataQualityStatus taskDataQuality = null;
            String result = taskState.getProp(ConfigurationKeys.TASK_LEVEL_POLICY_RESULT_KEY);
            if (result != null) {
                try {
                    taskDataQuality = DataQualityStatus.valueOf(result);
                } catch (IllegalArgumentException e) {
                    log.warn("Unknown data quality status encountered " + result);
                    // since we are setting known enums to task state, this should never happen
                    taskDataQuality = DataQualityStatus.UNKNOWN;
                }

                log.info("Data quality status of this task is: " + taskDataQuality);
                if (DataQualityStatus.PASSED == taskDataQuality) {
                    passedFilesSize++;
                } else if (DataQualityStatus.FAILED == taskDataQuality){
                    failedFilesSize++;
                    jobDataQuality = DataQualityStatus.FAILED;
                }
            } else {
                // Handle files without data quality evaluation (result is null)
                nonEvaluatedFilesSize++;
                log.warn("No data quality evaluation available for task: " + taskState.getTaskId());
            }
        }

        // Log summary of evaluation
        log.info("Data quality evaluation summary - Total: {}, Passed: {}, Failed: {}, Not Evaluated: {}",
            totalFiles, passedFilesSize, failedFilesSize, nonEvaluatedFilesSize);
        return new DataQualityEvaluationResult(jobDataQuality, totalFiles, passedFilesSize, failedFilesSize, nonEvaluatedFilesSize);
    }

    private static void emitMetrics(JobState jobState, DataQualityStatus jobDataQuality, int totalFiles,
            int passedFilesSize, int failedFilesSize, int nonEvaluatedFilesSize) {
        emitMetrics(jobState, jobDataQuality, totalFiles, passedFilesSize, failedFilesSize, nonEvaluatedFilesSize,
            jobState.getProp(ConfigurationKeys.DATASET_URN_KEY));
    }

    private static void emitMetrics(JobState jobState, DataQualityStatus jobDataQuality, int totalFiles,
            int passedFilesSize, int failedFilesSize, String datasetUrn) {
        emitMetrics(jobState, jobDataQuality, totalFiles, passedFilesSize, failedFilesSize, 0, datasetUrn);
    }

    private static void emitMetrics(JobState jobState, DataQualityStatus jobDataQuality, int totalFiles,
            int passedFilesSize, int failedFilesSize, int nonEvaluatedFilesSize, String datasetUrn) {
        OpenTelemetryMetricsBase otelMetrics = OpenTelemetryMetrics.getInstance(jobState);
        if (otelMetrics != null) {
            Attributes tags = getTagsForDataQualityMetrics(jobState, datasetUrn);
            // Emit data quality status (1 for PASSED, 0 for FAILED)
            DataQualityStatus finalJobDataQuality = jobDataQuality;
            log.info("Data quality status for this job is " + finalJobDataQuality);
            otelMetrics.getMeter(GAAS_OBSERVABILITY_METRICS_GROUPNAME)
                .gaugeBuilder(ServiceMetricNames.DATA_QUALITY_STATUS_METRIC_NAME)
                .ofLongs()
                .buildWithCallback(measurement -> {
                    log.info("Emitting metric for data quality");
                    measurement.record(DataQualityStatus.PASSED.equals(finalJobDataQuality) ? 1 : 0, tags);
                });

            otelMetrics.getMeter(GAAS_OBSERVABILITY_METRICS_GROUPNAME)
                .counterBuilder(ServiceMetricNames.DATA_QUALITY_OVERALL_FILE_COUNT)
                .build()
                .add(totalFiles, tags);
            // Emit passed files count
            otelMetrics.getMeter(GAAS_OBSERVABILITY_METRICS_GROUPNAME)
                .counterBuilder(ServiceMetricNames.DATA_QUALITY_SUCCESS_FILE_COUNT)
                .build().add(passedFilesSize, tags);
            // Emit failed files count
            otelMetrics.getMeter(GAAS_OBSERVABILITY_METRICS_GROUPNAME)
                .counterBuilder(ServiceMetricNames.DATA_QUALITY_FAILURE_FILE_COUNT)
                .build().add(failedFilesSize, tags);
            // Emit non-evaluated files count
            otelMetrics.getMeter(GAAS_OBSERVABILITY_METRICS_GROUPNAME)
                .counterBuilder(ServiceMetricNames.DATA_QUALITY_NON_EVALUATED_FILE_COUNT)
                .build().add(nonEvaluatedFilesSize, tags);
        }
    }

    private static Attributes getTagsForDataQualityMetrics(JobState jobState) {
        return getTagsForDataQualityMetrics(jobState, jobState.getProp(ConfigurationKeys.DATASET_URN_KEY));
    }

    private static Attributes getTagsForDataQualityMetrics(JobState jobState, String datasetUrn) {
        Properties jobProperties = new Properties();
        try {
            jobProperties = PropertiesUtils.deserialize(jobState.getProp("job.props", ""));
            log.info("Job properties loaded: " + jobProperties);
        } catch (IOException e) {
            log.error("Could not deserialize job properties", e);
        }

        return Attributes.builder()
            .put(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, jobState.getJobName())
            .put(TimingEvent.DATASET_URN, datasetUrn)
            .put(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, jobState.getProp(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD))
            .put(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, jobState.getProp(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD))
            .put(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, jobState.getProp(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD))
            .put(TimingEvent.FlowEventConstants.FLOW_EDGE_FIELD, jobState.getProp(TimingEvent.FlowEventConstants.FLOW_EDGE_FIELD))
            .put(TimingEvent.FlowEventConstants.FLOW_FABRIC, jobState.getProp(ServiceConfigKeys.GOBBLIN_SERVICE_INSTANCE_NAME, null))
            .put(TimingEvent.FlowEventConstants.FLOW_SOURCE, jobProperties.getProperty(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY, ""))
            .put(TimingEvent.FlowEventConstants.FLOW_DESTINATION, jobProperties.getProperty(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, ""))
            .put(TimingEvent.FlowEventConstants.SPEC_EXECUTOR_FIELD, jobState.getProp(TimingEvent.FlowEventConstants.SPEC_EXECUTOR_FIELD, ""))
            .build();
    }
}
    }
}