/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.uif.runtime.mapreduce;

import java.util.concurrent.RejectedExecutionException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.metrics.JobMetrics;
import com.linkedin.uif.runtime.AbstractTaskStateTracker;
import com.linkedin.uif.runtime.Task;


/**
 * A concrete extension to {@link AbstractTaskStateTracker} for Hadoop MapReduce based runtime.
 *
 * @author ynli
 */
public class MRTaskStateTracker extends AbstractTaskStateTracker {

  private static final Logger LOG = LoggerFactory.getLogger(MRTaskStateTracker.class);

  // Mapper context used to signal progress and update counters
  private final Mapper<LongWritable, Text, NullWritable, NullWritable>.Context context;

  public MRTaskStateTracker(Mapper<LongWritable, Text, NullWritable, NullWritable>.Context context) {
    super(context.getConfiguration(), LOG);
    this.context = context;
  }

  @Override
  public void registerNewTask(Task task) {
    try {
      scheduleTaskMetricsUpdater(new MRTaskMetricsUpdater(task, this.context), task);
    } catch (RejectedExecutionException ree) {
      LOG.error(String.format("Scheduling of task state reporter for task %s was rejected", task.getTaskId()));
    }
  }

  @Override
  public void onTaskCompletion(Task task) {
    JobMetrics metrics = JobMetrics.get(task.getTaskState().getProp(ConfigurationKeys.JOB_NAME_KEY), task.getJobId());

    /*
     * Update record-level and byte-level metrics and Hadoop MR counters
     * if enabled at both the task level and job level (aggregated).
     */
    if (JobMetrics.isEnabled(task.getTaskState().getWorkunit())) {
      task.updateRecordMetrics();
      task.updateByteMetrics();

      // Task-level record counter
      String taskRecordMetric = JobMetrics.metricName(JobMetrics.MetricGroup.TASK, task.getTaskId(), "records");
      this.context.getCounter(JobMetrics.MetricGroup.TASK.name(), taskRecordMetric)
          .setValue(metrics.getCounter(taskRecordMetric).getCount());

      // Job-level record counter
      String jobRecordMetric = JobMetrics.metricName(JobMetrics.MetricGroup.JOB, task.getJobId(), "records");
      this.context.getCounter(JobMetrics.MetricGroup.JOB.name(), jobRecordMetric)
          .increment(metrics.getCounter(taskRecordMetric).getCount());

      // Task-level byte counter
      String taskByteMetric = JobMetrics.metricName(JobMetrics.MetricGroup.TASK, task.getTaskId(), "bytes");
      this.context.getCounter(JobMetrics.MetricGroup.TASK.name(), taskByteMetric)
          .setValue(metrics.getCounter(taskByteMetric).getCount());

      // Job-level byte counter
      String jobByteMetric = JobMetrics.metricName(JobMetrics.MetricGroup.JOB, task.getJobId(), "bytes");
      this.context.getCounter(JobMetrics.MetricGroup.JOB.name(), jobByteMetric)
          .increment(metrics.getCounter(taskByteMetric).getCount());
    }

    // Mark the completion of this task
    task.markTaskCompletion();

    LOG.info(String
        .format("Task %s completed in %dms with state %s", task.getTaskId(), task.getTaskState().getTaskDuration(),
            task.getTaskState().getWorkingState()));
  }

  /**
   * An extension to {@link AbstractTaskStateTracker.TaskMetricsUpdater} for updating task metrics
   * in the Hadoop MapReduce setting.
   */
  private static class MRTaskMetricsUpdater extends AbstractTaskStateTracker.TaskMetricsUpdater {

    private final Mapper<LongWritable, Text, NullWritable, NullWritable>.Context context;

    MRTaskMetricsUpdater(Task task, Mapper<LongWritable, Text, NullWritable, NullWritable>.Context context) {
      super(task);
      this.context = context;
    }

    @Override
    protected void updateTaskMetrics() {
      super.updateTaskMetrics();

      /*
       * Update record-level metrics and Hadoop MR counters if enabled at the
       * task level ONLY. Job-level metrics are updated only after the job
       * completes so metrics can be properly aggregated at the job level.
       */
      if (JobMetrics.isEnabled(this.task.getTaskState().getWorkunit())) {
        // Task-level record counter
        String taskRecordMetric = JobMetrics.metricName(JobMetrics.MetricGroup.TASK, task.getTaskId(), "records");
        this.context.getCounter(JobMetrics.MetricGroup.TASK.name(), taskRecordMetric).setValue(
            JobMetrics.get(this.task.getTaskState().getProp(ConfigurationKeys.JOB_NAME_KEY), this.task.getJobId())
                .getCounter(taskRecordMetric).getCount());
      }

      // Tell the TaskTracker it's making progress
      this.context.progress();
    }
  }
}
