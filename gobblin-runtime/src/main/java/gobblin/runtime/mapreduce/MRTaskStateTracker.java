/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
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

import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.GobblinMetrics;
import gobblin.runtime.AbstractTaskStateTracker;
import gobblin.runtime.Task;
import gobblin.runtime.util.JobMetrics;
import gobblin.runtime.util.MetricGroup;
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

    try {
      if (GobblinMetrics.isEnabled(workUnit)) {
        task.updateRecordMetrics();
        task.updateByteMetrics();

        if (workUnit.getPropAsBoolean(ConfigurationKeys.MR_REPORT_METRICS_AS_COUNTERS_KEY,
            ConfigurationKeys.DEFAULT_MR_REPORT_METRICS_AS_COUNTERS)) {
          Map<String, Counter> counters = JobMetrics.get(null, task.getJobId()).getMetricContext().getCounters();
          for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            this.context.getCounter(MetricGroup.JOB.name(), entry.getKey()).setValue(entry.getValue().getCount());
          }
        }
      }
    } finally {
      task.markTaskCompletion();
    }

    LOG.info(String.format("Task %s completed in %dms with state %s",
        task.getTaskId(), task.getTaskState().getTaskDuration(), task.getTaskState().getWorkingState()));
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

      if (GobblinMetrics.isEnabled(workUnit)) {
        if (workUnit.getPropAsBoolean(ConfigurationKeys.MR_REPORT_METRICS_AS_COUNTERS_KEY,
            ConfigurationKeys.DEFAULT_MR_REPORT_METRICS_AS_COUNTERS)) {
          Map<String, Counter> counters = JobMetrics.get(null, task.getJobId()).getMetricContext().getCounters();
          for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            this.context.getCounter(MetricGroup.JOB.name(), entry.getKey()).setValue(entry.getValue().getCount());
          }
        }
      }

      // Tell the TaskTracker it's making progress
      this.context.progress();
    }
  }
}
