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
package org.apache.gobblin.cli;

import com.google.common.base.Optional;
import com.linkedin.data.template.StringMap;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.MetricNames;
import org.apache.gobblin.rest.*;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.joda.time.format.PeriodFormat;
import org.joda.time.format.PeriodFormatter;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Utility methods to print out various pieces of info about jobs
 */
public class JobInfoPrintUtils {
    private static NumberFormat decimalFormatter = new DecimalFormat("#0.00");
    private static DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.dateHourMinuteSecond();
    private static PeriodFormatter periodFormatter = PeriodFormat.getDefault();

    /**
     * Extracts the schedule from a job execution.
     * <p/>
     * If the job was in run once mode, it will return that, otherwise it will return the schedule.
     *
     * @param jobInfo A job execution info to extract from
     * @return "RUN_ONCE", the Quartz schedule string, or "UNKNOWN" if there were no job properties
     */
    public static String extractJobSchedule(JobExecutionInfo jobInfo) {
        if (jobInfo.hasJobProperties() && jobInfo.getJobProperties().size() > 0) {
            StringMap props = jobInfo.getJobProperties();

            if (props.containsKey(ConfigurationKeys.JOB_RUN_ONCE_KEY) ||
                    !props.containsKey(ConfigurationKeys.JOB_SCHEDULE_KEY)) {
                return "RUN_ONCE";
            } else if (props.containsKey(ConfigurationKeys.JOB_SCHEDULE_KEY)) {
                return props.get(ConfigurationKeys.JOB_SCHEDULE_KEY);
            }
        }
        return "UNKNOWN";
    }

    /**
     * Print a table describing a bunch of individual job executions.
     * @param jobExecutionInfos Job execution status to print
     */
    public static void printJobRuns(List<JobExecutionInfo> jobExecutionInfos) {
        if (jobExecutionInfos == null) {
            System.err.println("No job executions found.");
            System.exit(1);
        }

        List<String> labels = Arrays.asList("Job Id", "State", "Schedule", "Completed Tasks", "Launched Tasks",
                "Start Time", "End Time", "Duration (s)");
        List<String> flags = Arrays.asList("-", "-", "-", "", "", "-", "-", "-");
        List<List<String>> data = new ArrayList<>();
        for (JobExecutionInfo jobInfo : jobExecutionInfos) {
            List<String> entry = new ArrayList<>();
            entry.add(jobInfo.getJobId());
            entry.add(jobInfo.getState().toString());
            entry.add(extractJobSchedule(jobInfo));
            entry.add(jobInfo.getCompletedTasks().toString());
            entry.add(jobInfo.getLaunchedTasks().toString());
            entry.add(dateTimeFormatter.print(jobInfo.getStartTime()));
            entry.add(dateTimeFormatter.print(jobInfo.getEndTime()));
            entry.add(jobInfo.getState() == JobStateEnum.COMMITTED ?
                    decimalFormatter.format(jobInfo.getDuration() / 1000.0) : "-");
            data.add(entry);
        }
        new CliTablePrinter.Builder()
                .labels(labels)
                .data(data)
                .flags(flags)
                .delimiterWidth(2)
                .build()
                .printTable();
    }

    /**
     * Print summary information about a bunch of jobs in the system
     * @param jobExecutionInfos List of jobs
     * @param resultsLimit original result limit
     */
    public static void printAllJobs(List<JobExecutionInfo> jobExecutionInfos, int resultsLimit) {
        if (jobExecutionInfos == null) {
            System.err.println("No jobs found.");
            System.exit(1);
        }

        List<String> labels = Arrays.asList("Job Name", "State", "Last Run Started", "Last Run Completed",
                "Schedule", "Last Run Records Processed", "Last Run Records Failed");
        List<String> flags = Arrays.asList("-", "-", "-", "-", "-", "", "");
        List<List<String>> data = new ArrayList<>();
        for (JobExecutionInfo jobInfo : jobExecutionInfos) {
            List<String> entry = new ArrayList<>();
            entry.add(jobInfo.getJobName());
            entry.add(jobInfo.getState().toString());
            entry.add(dateTimeFormatter.print(jobInfo.getStartTime()));
            entry.add(dateTimeFormatter.print(jobInfo.getEndTime()));

            entry.add(extractJobSchedule(jobInfo));

            // Add metrics
            MetricArray metrics = jobInfo.getMetrics();
            Double recordsProcessed = null;
            Double recordsFailed = null;
            try {
                for (Metric metric : metrics) {
                    if (metric.getName().equals(MetricNames.ExtractorMetrics.RECORDS_READ_METER)) {
                        recordsProcessed = Double.parseDouble(metric.getValue());
                    } else if (metric.getName().equals(MetricNames.ExtractorMetrics.RECORDS_FAILED_METER)) {
                        recordsFailed = Double.parseDouble(metric.getValue());
                    }
                }

                if (recordsProcessed != null && recordsFailed != null) {
                    entry.add(recordsProcessed.toString());
                    entry.add(recordsFailed.toString());
                }
            } catch (NumberFormatException ex) {
                System.err.println("Failed to process metrics");
            }
            if (recordsProcessed == null || recordsFailed == null) {
                entry.add("-");
                entry.add("-");
            }

            data.add(entry);
        }
        new CliTablePrinter.Builder()
                .labels(labels)
                .data(data)
                .flags(flags)
                .delimiterWidth(2)
                .build()
                .printTable();

        if (jobExecutionInfos.size() == resultsLimit) {
            System.out.println("\nWARNING: There may be more jobs (# of results is equal to the limit)");
        }
    }

    /**
     * Print information about one specific job.
     * @param jobExecutionInfoOptional Job info to print
     */
    public static void printJob(Optional<JobExecutionInfo> jobExecutionInfoOptional) {
        if (!jobExecutionInfoOptional.isPresent()) {
            System.err.println("Job id not found.");
            return;
        }

        JobExecutionInfo jobExecutionInfo = jobExecutionInfoOptional.get();
        List<List<String>> data = new ArrayList<>();
        List<String> flags = Arrays.asList("", "-");

        data.add(Arrays.asList("Job Name", jobExecutionInfo.getJobName()));
        data.add(Arrays.asList("Job Id", jobExecutionInfo.getJobId()));
        data.add(Arrays.asList("State", jobExecutionInfo.getState().toString()));
        data.add(Arrays.asList("Completed/Launched Tasks",
                String.format("%d/%d", jobExecutionInfo.getCompletedTasks(), jobExecutionInfo.getLaunchedTasks())));
        data.add(Arrays.asList("Start Time", dateTimeFormatter.print(jobExecutionInfo.getStartTime())));
        data.add(Arrays.asList("End Time", dateTimeFormatter.print(jobExecutionInfo.getEndTime())));
        data.add(Arrays.asList("Duration", jobExecutionInfo.getState() == JobStateEnum.COMMITTED ? periodFormatter
                .print(new Period(jobExecutionInfo.getDuration().longValue())) : "-"));
        data.add(Arrays.asList("Tracking URL", jobExecutionInfo.getTrackingUrl()));
        data.add(Arrays.asList("Launcher Type", jobExecutionInfo.getLauncherType().name()));

        new CliTablePrinter.Builder()
                .data(data)
                .flags(flags)
                .delimiterWidth(2)
                .build()
                .printTable();

        JobInfoPrintUtils.printMetrics(jobExecutionInfo.getMetrics());
    }

    /**
     * Print properties of a specific job
     * @param jobExecutionInfoOptional
     */
    public static void printJobProperties(Optional<JobExecutionInfo> jobExecutionInfoOptional) {
        if (!jobExecutionInfoOptional.isPresent()) {
            System.err.println("Job not found.");
            return;
        }
        List<List<String>> data = new ArrayList<>();
        List<String> flags = Arrays.asList("", "-");
        List<String> labels = Arrays.asList("Property Key", "Property Value");

        for (Map.Entry<String, String> entry : jobExecutionInfoOptional.get().getJobProperties().entrySet()) {
            data.add(Arrays.asList(entry.getKey(), entry.getValue()));
        }

        new CliTablePrinter.Builder()
                .labels(labels)
                .data(data)
                .flags(flags)
                .delimiterWidth(2)
                .build()
                .printTable();
    }

    /**
     * Print out various metrics
     * @param metrics Metrics to print
     */
    private static void printMetrics(MetricArray metrics) {
        System.out.println();

        if (metrics.size() == 0) {
            System.out.println("No metrics found.");
            return;
        }

        List<List<String>> data = new ArrayList<>();
        List<String> flags = Arrays.asList("", "-");

        for (Metric metric : metrics) {
            data.add(Arrays.asList(metric.getName(), metric.getValue()));
        }

        new CliTablePrinter.Builder()
                .data(data)
                .flags(flags)
                .delimiterWidth(2)
                .build()
                .printTable();
    }

}
