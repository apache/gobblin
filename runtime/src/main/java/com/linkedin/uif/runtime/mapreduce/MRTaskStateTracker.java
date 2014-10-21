package com.linkedin.uif.runtime.mapreduce;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractIdleService;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.metrics.JobMetrics;
import com.linkedin.uif.runtime.Task;
import com.linkedin.uif.runtime.TaskStateTracker;

/**
 * An implementation of {@link TaskStateTracker} for Hadoop MapReduce based runtime.
 *
 * @author ynli
 */
public class MRTaskStateTracker extends AbstractIdleService implements TaskStateTracker {

    private static final Logger LOG = LoggerFactory.getLogger(MRTaskStateTracker.class);

    // Mapper context used to signal progress and update counters
    private final Mapper<LongWritable, Text, NullWritable, NullWritable>.Context context;

    // This is used to schedule and run reporters for reporting state
    // and progress of running tasks
    private final ScheduledThreadPoolExecutor reporterExecutor;

    public MRTaskStateTracker(
            Mapper<LongWritable, Text, NullWritable, NullWritable>.Context context) {

        this.context = context;

        // Use a thread pool of size 1 since this is only used by a single task
        this.reporterExecutor = new ScheduledThreadPoolExecutor(1);
        this.reporterExecutor.setMaximumPoolSize(1);
    }

    @Override
    protected void startUp() {
        LOG.info("Starting the MR task state tracker");
    }

    @Override
    protected void shutDown() {
        LOG.info("Stopping the MR task state tracker");
        this.reporterExecutor.shutdown();
    }

    @Override
    public void registerNewTask(Task task) {
        try {
            this.reporterExecutor.scheduleAtFixedRate(
                    new TaskStateUpdater(this.context, task),
                    task.getTaskContext().getStatusReportingInterval(),
                    task.getTaskContext().getStatusReportingInterval(),
                    TimeUnit.MILLISECONDS
            );
        } catch (RejectedExecutionException ree) {
            LOG.error(String.format(
                    "Scheduling of task state reporter for task %s was rejected", task.getTaskId()));
        }
    }

    @Override
    public void onTaskCompletion(Task task) {
        JobMetrics metrics = JobMetrics.get(
                task.getTaskState().getProp(ConfigurationKeys.JOB_NAME_KEY),
                task.getJobId());

        /*
         * Update record-level and byte-level metrics and Hadoop MR counters
         * if enabled at both the task level and job level (aggregated).
         */
        if (JobMetrics.isEnabled(task.getTaskState().getWorkunit())) {
            task.updateRecordMetrics();
            task.updateByteMetrics();

            // Task-level record counter
            String taskRecordMetric = JobMetrics.metricName(
                    JobMetrics.MetricGroup.TASK, task.getTaskId(), "records");
            this.context.getCounter(JobMetrics.MetricGroup.TASK.name(), taskRecordMetric).setValue(
                    metrics.getCounter(taskRecordMetric).getCount());

            // Job-level record counter
            String jobRecordMetric = JobMetrics.metricName(
                    JobMetrics.MetricGroup.JOB, task.getJobId(), "records");
            this.context.getCounter(JobMetrics.MetricGroup.JOB.name(), jobRecordMetric).increment(
                    metrics.getCounter(taskRecordMetric).getCount());

            // Task-level byte counter
            String taskByteMetric = JobMetrics.metricName(
                    JobMetrics.MetricGroup.TASK, task.getTaskId(), "bytes");
            this.context.getCounter(JobMetrics.MetricGroup.TASK.name(), taskByteMetric).setValue(
                    metrics.getCounter(taskByteMetric).getCount());

            // Job-level byte counter
            String jobByteMetric = JobMetrics.metricName(
                    JobMetrics.MetricGroup.JOB, task.getJobId(), "bytes");
            this.context.getCounter(JobMetrics.MetricGroup.JOB.name(), jobByteMetric).increment(
                    metrics.getCounter(taskByteMetric).getCount());

            task.getTaskState().removeMetrics();
        }
    }

    /**
     * A class for updating task states including task metrics/counters.
     */
    private static class TaskStateUpdater implements Runnable {

        private final Mapper<LongWritable, Text, NullWritable, NullWritable>.Context context;
        private final Task task;

        public TaskStateUpdater(
                Mapper<LongWritable, Text, NullWritable, NullWritable>.Context context,
                Task task) {

            this.context = context;
            this.task = task;
        }

        @Override
        public void run() {
            // Tell the TaskTracker it's making progress
            this.context.progress();

            /*
             * Update record-level metrics and Hadoop MR counters if enabled at the
             * task level ONLY. Job-level metrics are updated only after the job
             * completes so metrics can be properly aggregated at the job level.
             */
            if (JobMetrics.isEnabled(this.task.getTaskState().getWorkunit())) {
                this.task.updateRecordMetrics();

                // Task-level record counter
                String taskRecordMetric = JobMetrics.metricName(
                        JobMetrics.MetricGroup.TASK, task.getTaskId(), "records");
                this.context.getCounter(JobMetrics.MetricGroup.TASK.name(), taskRecordMetric).setValue(
                        JobMetrics.get(
                                this.task.getTaskState().getProp(ConfigurationKeys.JOB_NAME_KEY),
                                this.task.getJobId())
                                .getCounter(taskRecordMetric).getCount());
            }
        }
    }
}
