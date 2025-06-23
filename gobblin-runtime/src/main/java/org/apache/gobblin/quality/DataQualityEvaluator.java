/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.quality;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.OpenTelemetryMetrics;
import org.apache.gobblin.metrics.OpenTelemetryMetricsBase;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.qualitychecker.DataQualityStatus;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.service.ServiceConfigKeys;

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
        emitMetrics(jobState, result.getQualityStatus() == DataQualityStatus.PASSED? 1 : 0, result.getTotalFiles(),
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

            // Handle null task states gracefully
            if (taskState == null) {
                log.warn("Encountered null task state, skipping data quality evaluation for this task");
                nonEvaluatedFilesSize++;
                continue;
            }

            DataQualityStatus taskDataQuality = null;
            String result = taskState.getProp(ConfigurationKeys.TASK_LEVEL_POLICY_RESULT_KEY);
            taskDataQuality = DataQualityStatus.fromString(result);
            if (taskDataQuality != DataQualityStatus.NOT_EVALUATED) {
                log.debug("Data quality status of this task is: " + taskDataQuality);
                if (DataQualityStatus.PASSED == taskDataQuality) {
                    passedFilesSize++;
                } else if (DataQualityStatus.FAILED == taskDataQuality){
                    failedFilesSize++;
                    jobDataQuality = DataQualityStatus.FAILED;
                }
            } else {
                // Handle files without data quality evaluation
                nonEvaluatedFilesSize++;
                log.warn("No data quality evaluation for task: " + taskState.getTaskId());
            }
        }

        // Log summary of evaluation
        log.info("Data quality evaluation summary - Total: {}, Passed: {}, Failed: {}, Not Evaluated: {}",
            totalFiles, passedFilesSize, failedFilesSize, nonEvaluatedFilesSize);
        return new DataQualityEvaluationResult(jobDataQuality, totalFiles, passedFilesSize, failedFilesSize, nonEvaluatedFilesSize);
    }

    private static void emitMetrics(JobState jobState, int jobDataQuality, int totalFiles,
            int passedFilesSize, int failedFilesSize, int nonEvaluatedFilesSize, String datasetUrn) {
        OpenTelemetryMetricsBase otelMetrics = OpenTelemetryMetrics.getInstance(jobState);
        if(otelMetrics != null) {
            Meter meter = otelMetrics.getMeter(GAAS_OBSERVABILITY_METRICS_GROUPNAME);
            AtomicLong jobDataQualityRef = new AtomicLong(jobDataQuality);
            AtomicLong totalFilesRef = new AtomicLong(totalFiles);
            AtomicLong passedFilesRef = new AtomicLong(passedFilesSize);
            AtomicLong failedFilesRef = new AtomicLong(failedFilesSize);
            AtomicLong nonEvaluatedFilesRef = new AtomicLong(nonEvaluatedFilesSize);
// Define the observable counters
            ObservableLongMeasurement dataQualityStatusCounter =
                meter.counterBuilder(ServiceMetricNames.DATA_QUALITY_STATUS_METRIC_NAME).buildObserver();
            ObservableLongMeasurement totalCounter =
                meter.counterBuilder(ServiceMetricNames.DATA_QUALITY_OVERALL_FILE_COUNT).buildObserver();
            ObservableLongMeasurement successCounter =
                meter.counterBuilder(ServiceMetricNames.DATA_QUALITY_SUCCESS_FILE_COUNT).buildObserver();
            ObservableLongMeasurement failureCounter =
                meter.counterBuilder(ServiceMetricNames.DATA_QUALITY_FAILURE_FILE_COUNT).buildObserver();
            ObservableLongMeasurement nonEvaluatedFilesCounter =
                meter.counterBuilder(ServiceMetricNames.DATA_QUALITY_NON_EVALUATED_FILE_COUNT).buildObserver();
            Attributes tags = getTagsForDataQualityMetrics(jobState, datasetUrn);
            log.info("About to make batch callback for all data quality metrics with tags: " + tags.toString());
            meter.batchCallback(() -> {
                dataQualityStatusCounter.record(jobDataQualityRef.get(), tags);
                totalCounter.record(totalFilesRef.get(), tags);
                successCounter.record(passedFilesRef.get(), tags);
                failureCounter.record(failedFilesRef.get(), tags);
                nonEvaluatedFilesCounter.record(nonEvaluatedFilesRef.get(), tags);
            }, dataQualityStatusCounter, totalCounter, successCounter, failureCounter, nonEvaluatedFilesCounter);
        }
    }

    private static Attributes getTagsForDataQualityMetrics(JobState jobState, String datasetUrn) {
        Properties jobProperties = jobState.getProperties();
        log.info("Job properties loaded: " + jobProperties);

        return Attributes.builder()
            .put(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, jobState.getJobName())
            .put(TimingEvent.DATASET_URN, datasetUrn)
            .put(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, jobProperties.getProperty(ConfigurationKeys.FLOW_NAME_KEY))
            .put(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, jobProperties.getProperty(ConfigurationKeys.FLOW_GROUP_KEY))
            .put(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, jobProperties.getProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY))
            .put(TimingEvent.FlowEventConstants.FLOW_EDGE_FIELD, jobProperties.getProperty(ConfigurationKeys.FLOW_EDGE_ID_KEY))
            .put(TimingEvent.FlowEventConstants.FLOW_FABRIC, jobProperties.getProperty(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_FABRIC, null))
            .put(TimingEvent.FlowEventConstants.FLOW_SOURCE, jobProperties.getProperty(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY, ""))
            .put(TimingEvent.FlowEventConstants.FLOW_DESTINATION, jobProperties.getProperty(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, ""))
            .put(TimingEvent.FlowEventConstants.SPEC_EXECUTOR_FIELD, jobProperties.getProperty(ConfigurationKeys.FLOW_SPEC_EXECUTOR, ""))
            .build();
    }
}

