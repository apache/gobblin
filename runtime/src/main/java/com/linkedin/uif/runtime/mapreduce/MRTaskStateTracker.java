package com.linkedin.uif.runtime.mapreduce;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractIdleService;

import com.linkedin.uif.runtime.JobState;
import com.linkedin.uif.runtime.Metrics;
import com.linkedin.uif.runtime.Task;
import com.linkedin.uif.runtime.TaskState;
import com.linkedin.uif.runtime.TaskStateTracker;

/**
 * An implementation of {@link TaskStateTracker} for Hadoop MapReduce based runtime.
 *
 * @author ynli
 */
public class MRTaskStateTracker extends AbstractIdleService implements TaskStateTracker {

    private static final Logger LOG = LoggerFactory.getLogger(MRTaskStateTracker.class);

    // Mapper context used to signal progress and update counters
    private final Mapper<LongWritable, Text, Text, TaskState>.Context context;

    // This is used to schedule and run reporters for reporting state
    // and progress of running tasks
    private final ScheduledThreadPoolExecutor reporterExecutor;

    // This is used to signal the mapper that the taks has completed
    private final CountDownLatch countDownLatch;

    public MRTaskStateTracker(
            Mapper<LongWritable, Text, Text, TaskState>.Context context,
            CountDownLatch countDownLatch) {

        this.context = context;
        this.countDownLatch = countDownLatch;

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
        this.reporterExecutor.scheduleAtFixedRate(
                new TaskStateUpdater(this.context, task),
                0,
                task.getTaskContext().getStatusReportingInterval(),
                TimeUnit.MILLISECONDS
        );
    }

    @Override
    public void onTaskCompletion(Task task) {
        /*
         * Update record-level and byte-level metrics and Hadoop MR counters if enabled
         */
        if (Metrics.isEnabled(task.getTaskState().getWorkunit())) {
            task.updateRecordMetrics();

            // Job-level record counter
            String jobRecordMetric = Metrics.metricName(
                    JobState.JOB_METRICS_PREFIX, task.getJobId(), "records");
            this.context.getCounter("JOB", jobRecordMetric).setValue(
                    Metrics.getCounter(jobRecordMetric).getCount());

            // Task-level record counter
            String taskRecordMetric = Metrics.metricName(
                    TaskState.TASK_METRICS_PREFIX, task.getTaskId(), "records");
            this.context.getCounter("TASK", taskRecordMetric).setValue(
                    Metrics.getCounter(taskRecordMetric).getCount());

            // Job-level byte counter
            String jobByteMetric = Metrics.metricName(
                    JobState.JOB_METRICS_PREFIX, task.getJobId(), "bytes");
            this.context.getCounter("JOB", jobByteMetric).setValue(
                    Metrics.getCounter(jobByteMetric).getCount());

            // Task-level byte counter
            String taskByteMetric = Metrics.metricName(
                    TaskState.TASK_METRICS_PREFIX, task.getTaskId(), "bytes");
            this.context.getCounter("TASK", taskByteMetric).setValue(
                    Metrics.getCounter(taskByteMetric).getCount());
        }

        // Count down so signal the mapper the task is completed
        this.countDownLatch.countDown();
    }

    /**
     * A class for updating task states including task metrics/counters.
     */
    private static class TaskStateUpdater implements Runnable {

        private final Mapper<LongWritable, Text, Text, TaskState>.Context context;
        private final Task task;

        public TaskStateUpdater(
                Mapper<LongWritable, Text, Text, TaskState>.Context context,
                Task task) {

            this.context = context;
            this.task = task;
        }

        @Override
        public void run() {
            // Tell the TaskTracker it's making progress
            this.context.progress();

            /*
             * Update record-level metrics and Hadoop MR counters if enabled
             */
            if (Metrics.isEnabled(this.task.getTaskState().getWorkunit())) {
                this.task.updateRecordMetrics();

                // Job-level record counter
                String jobRecordMetric = Metrics.metricName(
                        JobState.JOB_METRICS_PREFIX, task.getJobId(), "records");
                this.context.getCounter("JOB", jobRecordMetric).setValue(
                        Metrics.getCounter(jobRecordMetric).getCount());

                // Task-level record counter
                String taskRecordMetric = Metrics.metricName(
                        TaskState.TASK_METRICS_PREFIX, task.getTaskId(), "records");
                this.context.getCounter("TASK", taskRecordMetric).setValue(
                        Metrics.getCounter(taskRecordMetric).getCount());
            }
        }
    }
}
