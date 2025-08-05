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

import java.util.List;
import java.util.Properties;

import lombok.AllArgsConstructor;
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
  private DataQualityEvaluator() {
  }

  /**
   * Result of a data quality evaluation containing the overall status and metrics.
   */
  @Getter
  @AllArgsConstructor
  public static class DataQualityEvaluationResult {
    private final DataQualityStatus qualityStatus;
    // Total number of files evaluated for data quality
    private final int totalFiles;
    // Number of files that passed data quality checks
    private final int passedFiles;
    // Number of files that failed data quality checks
    private final int failedFiles;
    // Number of files that were not evaluated for data quality for example files not found or not processed
    private final int nonEvaluatedFiles;
  }

  /**
   * Evaluates the data quality of a dataset state and stores the result.
   * This method is specifically designed for dataset-level quality evaluation.
   *
   * @param datasetState The dataset state to evaluate and update
   * @param jobState The job state containing additional context
   * @return DataQualityEvaluationResult containing the evaluation results
   */
  public static DataQualityEvaluationResult evaluateAndReportDatasetQuality(JobState.DatasetState datasetState,
      JobState jobState) {
    List<TaskState> taskStates = datasetState.getTaskStates();
    DataQualityEvaluationResult result = evaluateDataQuality(taskStates, jobState);

    // Store the result in the dataset state
    jobState.setProp(ConfigurationKeys.DATASET_QUALITY_STATUS_KEY, result.getQualityStatus().name());
    // Emit dataset-specific metrics
    emitMetrics(jobState, result.getQualityStatus(), result.getTotalFiles(), result.getPassedFiles(),
        result.getFailedFiles(), result.getNonEvaluatedFiles(), datasetState.getDatasetUrn());

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
    DataQualityStatus jobDataQualityStatus = DataQualityStatus.PASSED;
    int totalFiles = 0;
    int failedFilesCount = 0;
    int passedFilesCount = 0;
    int nonEvaluatedFilesCount = 0;

    for (TaskState taskState : taskStates) {
      totalFiles++;

      // Handle null task states gracefully
      if (taskState == null) {
        log.warn("Encountered null task state, skipping data quality evaluation for this task");
        nonEvaluatedFilesCount++;
        continue;
      }

      DataQualityStatus taskDataQuality = null;
      String result = taskState.getProp(ConfigurationKeys.TASK_LEVEL_POLICY_RESULT_KEY);
      taskDataQuality = DataQualityStatus.fromString(result);
      if (taskDataQuality != DataQualityStatus.NOT_EVALUATED) {
        log.debug("Data quality status of this task is: " + taskDataQuality);
        if (DataQualityStatus.PASSED == taskDataQuality) {
          passedFilesCount++;
        } else if (DataQualityStatus.FAILED == taskDataQuality) {
          failedFilesCount++;
          jobDataQualityStatus = DataQualityStatus.FAILED;
        } else {
          log.warn("Unexpected data quality status: " + taskDataQuality + " for task: " + taskState.getTaskId());
        }
      } else {
        // Handle files without data quality evaluation
        nonEvaluatedFilesCount++;
        log.warn("No data quality evaluation for task: " + taskState.getTaskId());
      }
    }

    // Log summary of evaluation
    log.info("Data quality evaluation summary - Total: {}, Passed: {}, Failed: {}, Not Evaluated: {}", totalFiles,
        passedFilesCount, failedFilesCount, nonEvaluatedFilesCount);
    return new DataQualityEvaluationResult(jobDataQualityStatus, totalFiles, passedFilesCount, failedFilesCount,
        nonEvaluatedFilesCount);
  }

  private static void emitMetrics(JobState jobState, final DataQualityStatus jobDataQuality, final int totalFiles,
      final int passedFilesCount, final int failedFilesCount, final int nonEvaluatedFilesCount,
      final String datasetUrn) {
    try {
      // Check if OpenTelemetry is enabled
      boolean otelEnabled = jobState.getPropAsBoolean(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_ENABLED,
          ConfigurationKeys.DEFAULT_METRICS_REPORTING_OPENTELEMETRY_ENABLED);

      if (!otelEnabled) {
        log.info("OpenTelemetry metrics disabled, skipping metrics emission");
        return;
      }

      OpenTelemetryMetricsBase otelMetrics = OpenTelemetryMetrics.getInstance(jobState);
      if (otelMetrics == null) {
        log.warn("OpenTelemetry metrics instance is null, skipping metrics emission");
        return;
      }
      log.info("OpenTelemetry instance obtained");

      Meter meter = otelMetrics.getMeter(GAAS_OBSERVABILITY_METRICS_GROUPNAME);
      Attributes tags = getTagsForDataQualityMetrics(jobState, datasetUrn);
      log.info("Emitting DQ metrics for job={}, status={}, tags={}", jobState.getJobName(), jobDataQuality, tags);
      String jobMetricDescription = "Number of Jobs with data quality status: " + jobDataQuality;
      String jobMetricName =
          (jobDataQuality == DataQualityStatus.PASSED) ? ServiceMetricNames.DATA_QUALITY_JOB_SUCCESS_COUNT
              : ServiceMetricNames.DATA_QUALITY_JOB_FAILURE_COUNT;
      log.info("Data quality status for job: {} is {}", jobState.getJobName(), jobDataQuality);
      meter.counterBuilder(jobMetricName).setDescription(jobMetricDescription).build().add(1, tags);

      // Emit overall files count
      meter.counterBuilder(ServiceMetricNames.DATA_QUALITY_OVERALL_FILE_COUNT)
          .setDescription("Number of files evaluated for data quality").build().add(totalFiles, tags);

      // Emit passed files count
      meter.counterBuilder(ServiceMetricNames.DATA_QUALITY_SUCCESS_FILE_COUNT)
          .setDescription("Number of files that passed data quality").build().add(passedFilesCount, tags);

      // Emit failed files count
      meter.counterBuilder(ServiceMetricNames.DATA_QUALITY_FAILURE_FILE_COUNT)
          .setDescription("Number of files that failed data quality").build().add(failedFilesCount, tags);

      // Emit non-evaluated files count
      meter.counterBuilder(ServiceMetricNames.DATA_QUALITY_NON_EVALUATED_FILE_COUNT)
          .setDescription("Number of files that did not have data quality evaluation").build()
          .add(nonEvaluatedFilesCount, tags);
    } catch (Exception e) {
      log.error("Error in emitMetrics for job: {}", jobState.getJobName(), e);
    }
    log.info("Completed emitMetrics for job: {}", jobState.getJobName());
  }

  private static Attributes getTagsForDataQualityMetrics(JobState jobState, String datasetUrn) {
    Properties jobProperties = jobState.getProperties();
    log.info("Job properties loaded: " + jobProperties);

    return Attributes.builder().put(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, jobState.getJobName())
        .put(TimingEvent.DATASET_URN, datasetUrn)
        .put(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, jobProperties.getProperty(ConfigurationKeys.FLOW_NAME_KEY))
        .put(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD,
            jobProperties.getProperty(ConfigurationKeys.FLOW_GROUP_KEY))
        .put(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD,
            jobProperties.getProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY))
        .put(TimingEvent.FlowEventConstants.FLOW_EDGE_FIELD,
            jobProperties.getProperty(ConfigurationKeys.FLOW_EDGE_ID_KEY))
        .put(TimingEvent.FlowEventConstants.FLOW_FABRIC,
            jobProperties.getProperty(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_FABRIC, null))
        .put(TimingEvent.FlowEventConstants.FLOW_SOURCE,
            jobProperties.getProperty(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY, ""))
        .put(TimingEvent.FlowEventConstants.FLOW_DESTINATION,
            jobProperties.getProperty(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, ""))
        .put(TimingEvent.FlowEventConstants.SPEC_EXECUTOR_FIELD,
            jobProperties.getProperty(ConfigurationKeys.FLOW_SPEC_EXECUTOR, "")).build();
  }
}
