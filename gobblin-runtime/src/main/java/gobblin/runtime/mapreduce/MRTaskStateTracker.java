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

package gobblin.runtime.mapreduce;

import java.util.Map;
import java.util.concurrent.RejectedExecutionException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.JobMetrics;
import gobblin.runtime.AbstractTaskStateTracker;
import gobblin.runtime.Task;
import gobblin.source.workunit.WorkUnit;

/**
 * A concrete extension to {@link gobblin.runtime.AbstractTaskStateTracker} for Hadoop MapReduce based runtime.
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
    WorkUnit workUnit = task.getTaskState().getWorkunit();

    /*
     * Update record-level and byte-level metrics and Hadoop MR counters
     * if enabled at both the task level and job level (aggregated).
     */
    if (JobMetrics.isEnabled(workUnit)) {
      task.updateRecordMetrics();
      task.updateByteMetrics();

      JobMetrics metrics = JobMetrics.get(task.getTaskState().getProp(ConfigurationKeys.JOB_NAME_KEY), task.getJobId());

      if (workUnit.getPropAsBoolean(ConfigurationKeys.MR_INCLUDE_TASK_COUNTERS_KEY,
          ConfigurationKeys.DEFAULT_MR_INCLUDE_TASK_COUNTERS)) {
        // Task-level counters
        Map<String, ? extends Metric> taskLevelCounters =
            metrics.getMetricsOfType(JobMetrics.MetricType.COUNTER, JobMetrics.MetricGroup.TASK, task.getTaskId());
        for (Map.Entry<String, ? extends Metric> entry : taskLevelCounters.entrySet()) {
          this.context.getCounter(JobMetrics.MetricGroup.TASK.name(), entry.getKey())
              .setValue(((Counter) entry.getValue()).getCount());
        }
      }

      // Job-level counters
      Map<String, ? extends Metric> jobLevelCounters =
          metrics.getMetricsOfType(JobMetrics.MetricType.COUNTER, JobMetrics.MetricGroup.JOB, task.getJobId());
      for (Map.Entry<String, ? extends Metric> entry : jobLevelCounters.entrySet()) {
        this.context.getCounter(JobMetrics.MetricGroup.JOB.name(), entry.getKey())
            .increment(((Counter) entry.getValue()).getCount());
      }
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
  private class MRTaskMetricsUpdater extends AbstractTaskStateTracker.TaskMetricsUpdater {

    private final Mapper<LongWritable, Text, NullWritable, NullWritable>.Context context;

    MRTaskMetricsUpdater(Task task, Mapper<LongWritable, Text, NullWritable, NullWritable>.Context context) {
      super(task);
      this.context = context;
    }

    @Override
    protected void updateTaskMetrics() {
      super.updateTaskMetrics();

      WorkUnit workUnit = this.task.getTaskState().getWorkunit();

      /*
       * Update metrics and Hadoop MR counters if enabled at the task level ONLY. Job-level metrics are
       * updated only after the job completes so metrics can be properly aggregated at the job level.
       */
      if (JobMetrics.isEnabled(workUnit)) {
        JobMetrics metrics =
            JobMetrics.get(this.task.getTaskState().getProp(ConfigurationKeys.JOB_NAME_KEY), this.task.getJobId());
        if (workUnit.getPropAsBoolean(ConfigurationKeys.MR_INCLUDE_TASK_COUNTERS_KEY,
            ConfigurationKeys.DEFAULT_MR_INCLUDE_TASK_COUNTERS)) {
          // Task-level counters
          Map<String, ? extends Metric> taskLevelCounters = metrics
              .getMetricsOfType(JobMetrics.MetricType.COUNTER, JobMetrics.MetricGroup.TASK, this.task.getTaskId());
          for (Map.Entry<String, ? extends Metric> entry : taskLevelCounters.entrySet()) {
            this.context.getCounter(JobMetrics.MetricGroup.TASK.name(), entry.getKey())
                .setValue(((Counter) entry.getValue()).getCount());
          }
        }
      }

      // Tell the TaskTracker it's making progress
      this.context.progress();
    }
  }
}
