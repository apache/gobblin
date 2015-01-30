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

package com.linkedin.uif.runtime;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.stream.JsonWriter;

import com.linkedin.data.template.StringMap;
import com.linkedin.gobblin.rest.Metric;
import com.linkedin.gobblin.rest.MetricArray;
import com.linkedin.gobblin.rest.MetricTypeEnum;
import com.linkedin.gobblin.rest.Table;
import com.linkedin.gobblin.rest.TableTypeEnum;
import com.linkedin.gobblin.rest.TaskExecutionInfo;
import com.linkedin.gobblin.rest.TaskStateEnum;
import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.metrics.JobMetrics;
import com.linkedin.uif.source.workunit.Extract;


/**
 * An extension to {@link WorkUnitState} with run-time task state information.
 *
 * @author ynli
 */
public class TaskState extends WorkUnitState {

  private String jobId;
  private String taskId;
  private long startTime;
  private long endTime;
  private long duration;

  // Needed for serialization/deserialization
  public TaskState() {
  }

  public TaskState(WorkUnitState workUnitState) {
    // Since getWorkunit() returns an immutable WorkUnit object,
    // the WorkUnit object in this object is also immutable.
    super(workUnitState.getWorkunit());
    this.jobId = workUnitState.getProp(ConfigurationKeys.JOB_ID_KEY);
    this.taskId = workUnitState.getProp(ConfigurationKeys.TASK_ID_KEY);
    this.setId(this.taskId);
  }

  /**
   * Get the ID of the job this {@link TaskState} is for.
   *
   * @return ID of the job this {@link TaskState} is for
   */
  public String getJobId() {
    return this.jobId;
  }

  /**
   * Set the ID of the job this {@link TaskState} is for.
   *
   * @param jobId ID of the job this {@link TaskState} is for
   */
  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  /**
   * Get the ID of the task this {@link TaskState} is for.
   *
   * @return ID of the task this {@link TaskState} is for
   */
  public String getTaskId() {
    return this.taskId;
  }

  /**
   * Set the ID of the task this {@link TaskState} is for.
   *
   * @param taskId ID of the task this {@link TaskState} is for
   */
  public void setTaskId(String taskId) {
    this.taskId = taskId;
  }

  /**
   * Get task start time in milliseconds.
   *
   * @return task start time in milliseconds
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * Set task start time in milliseconds.
   *
   * @param startTime task start time in milliseconds
   */
  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  /**
   * Get task end time in milliseconds.
   *
   * @return task end time in milliseconds
   */
  public long getEndTime() {
    return endTime;
  }

  /**
   * set task end time in milliseconds.
   *
   * @param endTime task end time in milliseconds
   */
  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  /**
   * Get task duration in milliseconds.
   *
   * @return task duration in milliseconds
   */
  public long getTaskDuration() {
    return this.duration;
  }

  /**
   * Set task duration in milliseconds.
   *
   * @param duration task duration in milliseconds
   */
  public void setTaskDuration(long duration) {
    this.duration = duration;
  }

  /**
   * Update record-level metrics.
   *
   * @param recordsWritten number of records written by the writer
   */
  public void updateRecordMetrics(long recordsWritten) {
    JobMetrics metrics = JobMetrics.get(this.getProp(ConfigurationKeys.JOB_NAME_KEY), this.jobId);

    Counter taskRecordCounter =
        metrics.getCounter(JobMetrics.metricName(JobMetrics.MetricGroup.TASK, this.taskId, "records"));
    long inc = recordsWritten - taskRecordCounter.getCount();

    taskRecordCounter.inc(inc);
    metrics.getMeter(JobMetrics.metricName(JobMetrics.MetricGroup.TASK, this.taskId, "recordsPerSec")).mark(inc);
    metrics.getCounter(JobMetrics.metricName(JobMetrics.MetricGroup.JOB, this.jobId, "records")).inc(inc);
    metrics.getMeter(JobMetrics.metricName(JobMetrics.MetricGroup.JOB, this.jobId, "recordsPerSec")).mark(inc);
  }

  /**
   * Collect byte-level metrics.
   *
   * <p>
   *     This method is only supposed to be called after the writer commits.
   * </p>
   *
   * @param bytesWritten number of bytes written by the writer
   */
  public void updateByteMetrics(long bytesWritten) {
    JobMetrics metrics = JobMetrics.get(this.getProp(ConfigurationKeys.JOB_NAME_KEY), this.jobId);
    metrics.getCounter(JobMetrics.metricName(JobMetrics.MetricGroup.TASK, this.taskId, "bytes")).inc(bytesWritten);
    metrics.getMeter(JobMetrics.metricName(JobMetrics.MetricGroup.TASK, this.taskId, "bytesPerSec")).mark(bytesWritten);
    metrics.getCounter(JobMetrics.metricName(JobMetrics.MetricGroup.JOB, this.jobId, "bytes")).inc(bytesWritten);
    metrics.getMeter(JobMetrics.metricName(JobMetrics.MetricGroup.JOB, this.jobId, "bytesPerSec")).mark(bytesWritten);
  }

  /**
   * Adjust job-level metrics when the task gets retried.
   */
  public void adjustJobMetricsOnRetry() {
    JobMetrics metrics = JobMetrics.get(this.getProp(ConfigurationKeys.JOB_NAME_KEY), this.jobId);
    long recordsWritten =
        metrics.getCounter(JobMetrics.metricName(JobMetrics.MetricGroup.TASK, this.taskId, "records")).getCount();
    long bytesWritten =
        metrics.getCounter(JobMetrics.metricName(JobMetrics.MetricGroup.TASK, this.taskId, "bytes")).getCount();
    metrics.getCounter(JobMetrics.metricName(JobMetrics.MetricGroup.JOB, this.jobId, "records")).dec(recordsWritten);
    metrics.getCounter(JobMetrics.metricName(JobMetrics.MetricGroup.JOB, this.jobId, "bytes")).dec(bytesWritten);
  }

  /**
   * Remove all task-level metrics objects associated with this task.
   */
  public void removeMetrics() {
    JobMetrics metrics = JobMetrics.get(this.getProp(ConfigurationKeys.JOB_NAME_KEY), this.jobId);
    for (String name : metrics.getMetricsOfGroup(JobMetrics.MetricGroup.TASK).keySet()) {
      if (name.contains(this.taskId)) {
        metrics.removeMetric(name);
      }
    }
  }

  @Override
  public void readFields(DataInput in)
      throws IOException {
    Text text = new Text();
    text.readFields(in);
    this.jobId = text.toString();
    text.readFields(in);
    this.taskId = text.toString();
    this.setId(this.taskId);
    this.startTime = in.readLong();
    this.endTime = in.readLong();
    this.duration = in.readLong();
    super.readFields(in);
  }

  @Override
  public void write(DataOutput out)
      throws IOException {
    Text text = new Text();
    text.set(this.jobId);
    text.write(out);
    text.set(this.taskId);
    text.write(out);
    out.writeLong(this.startTime);
    out.writeLong(this.endTime);
    out.writeLong(this.duration);
    super.write(out);
  }

  /**
   * Convert this {@link TaskState} to a json document.
   *
   * @param jsonWriter a {@link com.google.gson.stream.JsonWriter} used to write the json document
   * @throws IOException
   */
  public void toJson(JsonWriter jsonWriter)
      throws IOException {
    jsonWriter.beginObject();

    jsonWriter.name("task id").value(this.getTaskId())
        .name("task state").value(this.getWorkingState().name())
        .name("start time").value(this.getStartTime())
        .name("end time").value(this.getEndTime())
        .name("duration").value(this.getTaskDuration())
        .name("high watermark").value(this.getHighWaterMark())
        .name("retry count").value(this.getPropAsInt(ConfigurationKeys.TASK_RETRIES_KEY, 0));

    // Also add failure exception information if it exists. This information is useful even in the
    // case that the task finally succeeds so we know what happened in the course of task execution.
    if (this.contains(ConfigurationKeys.TASK_FAILURE_EXCEPTION_KEY)) {
      jsonWriter.name("exception").value(this.getProp(ConfigurationKeys.TASK_FAILURE_EXCEPTION_KEY));
    }

    jsonWriter.endObject();
  }

  /**
   * Convert this {@link TaskState} instance to a {@link TaskExecutionInfo} instance.
   *
   * @return a {@link TaskExecutionInfo} instance
   */
  public TaskExecutionInfo toTaskExecutionInfo() {
    TaskExecutionInfo taskExecutionInfo = new TaskExecutionInfo();

    taskExecutionInfo.setJobId(this.jobId);
    taskExecutionInfo.setTaskId(this.taskId);
    taskExecutionInfo.setStartTime(this.startTime);
    taskExecutionInfo.setEndTime(this.endTime);
    taskExecutionInfo.setDuration(this.duration);
    taskExecutionInfo.setState(TaskStateEnum.valueOf(getWorkingState().name()));
    if (this.contains(ConfigurationKeys.TASK_FAILURE_EXCEPTION_KEY)) {
      taskExecutionInfo.setFailureException(this.getProp(ConfigurationKeys.TASK_FAILURE_EXCEPTION_KEY));
    }
    taskExecutionInfo.setHighWatermark(this.getHighWaterMark());

    // Add extract/table information
    Table table = new Table();
    Extract extract = this.getExtract();
    table.setNamespace(extract.getNamespace());
    table.setName(extract.getTable());
    if (extract.hasType()) {
      table.setType(TableTypeEnum.valueOf(extract.getType().name()));
    }
    taskExecutionInfo.setTable(table);

    // Add task metrics
    JobMetrics jobMetrics = JobMetrics.get(this.getProp(ConfigurationKeys.JOB_NAME_KEY), this.jobId);
    MetricArray metricArray = new MetricArray();

    for (Map.Entry<String, ? extends com.codahale.metrics.Metric> entry : jobMetrics
        .getMetricsOfType(JobMetrics.MetricType.COUNTER, JobMetrics.MetricGroup.TASK, this.taskId).entrySet()) {
      Metric counter = new Metric();
      counter.setGroup(JobMetrics.MetricGroup.TASK.name());
      counter.setName(entry.getKey());
      counter.setType(MetricTypeEnum.valueOf(JobMetrics.MetricType.COUNTER.name()));
      counter.setValue(Long.toString(((Counter) entry.getValue()).getCount()));
      metricArray.add(counter);
    }

    for (Map.Entry<String, ? extends com.codahale.metrics.Metric> entry : jobMetrics
        .getMetricsOfType(JobMetrics.MetricType.METER, JobMetrics.MetricGroup.TASK, this.taskId).entrySet()) {
      Metric meter = new Metric();
      meter.setGroup(JobMetrics.MetricGroup.TASK.name());
      meter.setName(entry.getKey());
      meter.setType(MetricTypeEnum.valueOf(JobMetrics.MetricType.METER.name()));
      meter.setValue(Double.toString(((Meter) entry.getValue()).getMeanRate()));
      metricArray.add(meter);
    }

    for (Map.Entry<String, ? extends com.codahale.metrics.Metric> entry : jobMetrics
        .getMetricsOfType(JobMetrics.MetricType.GAUGE, JobMetrics.MetricGroup.TASK, this.taskId).entrySet()) {
      Metric gauge = new Metric();
      gauge.setGroup(JobMetrics.MetricGroup.TASK.name());
      gauge.setName(entry.getKey());
      gauge.setType(MetricTypeEnum.valueOf(JobMetrics.MetricType.GAUGE.name()));
      gauge.setValue(((Gauge) entry.getValue()).getValue().toString());
      metricArray.add(gauge);
    }

    taskExecutionInfo.setMetrics(metricArray);

    // Add task properties
    Map<String, String> taskProperties = Maps.newHashMap();
    for (String name : this.getPropertyNames()) {
      String value = this.getProp(name);
      if (!Strings.isNullOrEmpty(value))
        taskProperties.put(name, value);
    }
    taskExecutionInfo.setTaskProperties(new StringMap(taskProperties));

    return taskExecutionInfo;
  }
}
