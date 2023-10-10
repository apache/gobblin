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

package org.apache.gobblin.runtime;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonWriter;
import com.linkedin.data.template.StringMap;

import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.rest.Metric;
import org.apache.gobblin.rest.MetricArray;
import org.apache.gobblin.rest.MetricTypeEnum;
import org.apache.gobblin.rest.Table;
import org.apache.gobblin.rest.TableTypeEnum;
import org.apache.gobblin.rest.TaskExecutionInfo;
import org.apache.gobblin.rest.TaskStateEnum;
import org.apache.gobblin.runtime.job.TaskProgress;
import org.apache.gobblin.runtime.troubleshooter.Issue;
import org.apache.gobblin.runtime.util.GsonUtils;
import org.apache.gobblin.runtime.util.MetricGroup;
import org.apache.gobblin.runtime.util.TaskMetrics;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.util.ForkOperatorUtils;


/**
 * An extension to {@link WorkUnitState} with run-time task state information.
 *
 * @author Yinan Li
 */
public class TaskState extends WorkUnitState implements TaskProgress {

  // Built-in metric names

  /**
   * @deprecated see {@link org.apache.gobblin.instrumented.writer.InstrumentedDataWriterBase}.
   */
  private static final String RECORDS = "records";

  /**
   * @deprecated see {@link org.apache.gobblin.instrumented.writer.InstrumentedDataWriterBase}.
   */
  private static final String RECORDS_PER_SECOND = "recordsPerSec";

  /**
   * @deprecated see {@link org.apache.gobblin.instrumented.writer.InstrumentedDataWriterBase}.
   */
  private static final String BYTES = "bytes";

  /**
   * @deprecated see {@link org.apache.gobblin.instrumented.writer.InstrumentedDataWriterBase}.
   */
  private static final String BYTES_PER_SECOND = "bytesPerSec";

  /** ID of the job this {@link TaskState} is for */
  @Getter @Setter
  private String jobId;
  /** ID of the task this {@link TaskState} is for */
  @Getter @Setter
  private String taskId;
  /** sequence number of the task this {@link TaskState} is for */
  @Getter
  private String taskKey;
  @Getter
  private Optional<String> taskAttemptId;

  /** task start time in milliseconds */
  @Getter @Setter
  private long startTime = 0;
  /** task end time in milliseconds */
  @Getter @Setter
  private long endTime = 0;
  /** task duration in milliseconds */
  @Getter @Setter
  private long taskDuration;

  // Needed for serialization/deserialization
  public TaskState() {}

  public TaskState(WorkUnitState workUnitState) {
    // Since getWorkunit() returns an immutable WorkUnit object,
    // the WorkUnit object in this object is also immutable.
    super(workUnitState.getWorkunit(), workUnitState.getJobState(), workUnitState.getTaskBrokerNullable());
    addAll(workUnitState);
    this.jobId = workUnitState.getProp(ConfigurationKeys.JOB_ID_KEY);
    this.taskId = workUnitState.getProp(ConfigurationKeys.TASK_ID_KEY);
    this.taskKey = workUnitState.getProp(ConfigurationKeys.TASK_KEY_KEY, "unknown_task_key");
    this.taskAttemptId = Optional.fromNullable(workUnitState.getProp(ConfigurationKeys.TASK_ATTEMPT_ID_KEY));
    this.setId(this.taskId);
  }

  public TaskState(TaskState taskState) {
    super(taskState.getWorkunit(), taskState.getJobState(), taskState.getTaskBrokerNullable());
    addAll(taskState);
    this.jobId = taskState.getProp(ConfigurationKeys.JOB_ID_KEY);
    this.taskId = taskState.getProp(ConfigurationKeys.TASK_ID_KEY);
    this.taskAttemptId = taskState.getTaskAttemptId();
    this.setId(this.taskId);
  }

  /**
   * Get the {@link ConfigurationKeys#TASK_FAILURE_EXCEPTION_KEY} if it exists, else return {@link Optional#absent()}.
   */
  public Optional<String> getTaskFailureException() {
    return Optional.fromNullable(this.getProp(ConfigurationKeys.TASK_FAILURE_EXCEPTION_KEY));
  }

  /**
   * If not already present, set the {@link ConfigurationKeys#TASK_FAILURE_EXCEPTION_KEY} to a {@link String}
   * representation of the given {@link Throwable}.
   */
  public void setTaskFailureException(Throwable taskFailureException) {
    if (!this.contains(ConfigurationKeys.TASK_FAILURE_EXCEPTION_KEY)) {
      this.setProp(ConfigurationKeys.TASK_FAILURE_EXCEPTION_KEY,
          Throwables.getStackTraceAsString(taskFailureException));
    }
  }

  /**
   * Returns list of task issues. Can be null, if no issues were set.
   */
  @Nullable
  public List<Issue> getTaskIssues() {
    String serializedIssues = this.getProp(ConfigurationKeys.TASK_ISSUES_KEY, null);
    if (serializedIssues == null) {
      return null;
    }

    Type issueListTypeToken = new TypeToken<ArrayList<Issue>>() {
    }.getType();

    return GsonUtils.GSON_WITH_DATE_HANDLING.fromJson(serializedIssues, issueListTypeToken);
  }

  /**
   * Saves the list of task issues into the state, so that parent job can aggregate and consume it.   *
   */
  public void setTaskIssues(List<Issue> issues) {
    if (issues == null) {
      this.removeProp(ConfigurationKeys.TASK_ISSUES_KEY);
    } else {
      String serializedIssues = GsonUtils.GSON_WITH_DATE_HANDLING.toJson(issues);
      this.setProp(ConfigurationKeys.TASK_ISSUES_KEY, serializedIssues);
    }
  }

  /**
   * Return whether the task has completed running or not.
   *
   * @return {@code true} if the task has completed or {@code false} otherwise
   */
  public boolean isCompleted() {
    WorkingState state = getWorkingState();
    return state == WorkingState.SUCCESSFUL || state == WorkingState.COMMITTED || state == WorkingState.FAILED;
  }

  /**
   * Update record-level metrics.
   *
   * @param recordsWritten number of records written by the writer
   * @param branchIndex fork branch index
   *
   * @deprecated see {@link org.apache.gobblin.instrumented.writer.InstrumentedDataWriterBase}.
   */
  public synchronized void updateRecordMetrics(long recordsWritten, int branchIndex) {
    TaskMetrics metrics = TaskMetrics.get(this);
    // chopping branch index from metric name
    // String forkBranchId = ForkOperatorUtils.getForkId(this.taskId, branchIndex);
    String forkBranchId = TaskMetrics.taskInstanceRemoved(this.taskId);

    Counter taskRecordCounter = metrics.getCounter(MetricGroup.TASK.name(), forkBranchId, RECORDS);
    long inc = recordsWritten - taskRecordCounter.getCount();
    taskRecordCounter.inc(inc);
    metrics.getMeter(MetricGroup.TASK.name(), forkBranchId, RECORDS_PER_SECOND).mark(inc);
    metrics.getCounter(MetricGroup.JOB.name(), this.jobId, RECORDS).inc(inc);
    metrics.getMeter(MetricGroup.JOB.name(), this.jobId, RECORDS_PER_SECOND).mark(inc);
  }

  /**
   * Collect byte-level metrics.
   *
   * @param bytesWritten number of bytes written by the writer
   * @param branchIndex fork branch index
   *
   * @deprecated see {@link org.apache.gobblin.instrumented.writer.InstrumentedDataWriterBase}.
   */
  public synchronized void updateByteMetrics(long bytesWritten, int branchIndex) {
    TaskMetrics metrics = TaskMetrics.get(this);
    String forkBranchId = TaskMetrics.taskInstanceRemoved(this.taskId);

    Counter taskByteCounter = metrics.getCounter(MetricGroup.TASK.name(), forkBranchId, BYTES);
    long inc = bytesWritten - taskByteCounter.getCount();
    taskByteCounter.inc(inc);
    metrics.getMeter(MetricGroup.TASK.name(), forkBranchId, BYTES_PER_SECOND).mark(inc);
    metrics.getCounter(MetricGroup.JOB.name(), this.jobId, BYTES).inc(inc);
    metrics.getMeter(MetricGroup.JOB.name(), this.jobId, BYTES_PER_SECOND).mark(inc);
  }

  /**
   * Adjust job-level metrics when the task gets retried.
   *
   * @param branches number of forked branches
   */
  public void adjustJobMetricsOnRetry(int branches) {
    TaskMetrics metrics = TaskMetrics.get(this);

    for (int i = 0; i < branches; i++) {
      String forkBranchId = ForkOperatorUtils.getForkId(this.taskId, i);
      long recordsWritten = metrics.getCounter(MetricGroup.TASK.name(), forkBranchId, RECORDS).getCount();
      long bytesWritten = metrics.getCounter(MetricGroup.TASK.name(), forkBranchId, BYTES).getCount();
      metrics.getCounter(MetricGroup.JOB.name(), this.jobId, RECORDS).dec(recordsWritten);
      metrics.getCounter(MetricGroup.JOB.name(), this.jobId, BYTES).dec(bytesWritten);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    Text text = new Text();
    text.readFields(in);
    this.jobId = text.toString().intern();
    text.readFields(in);
    this.taskId = text.toString().intern();
    this.taskAttemptId = Optional.absent();
    this.setId(this.taskId);
    this.startTime = in.readLong();
    this.endTime = in.readLong();
    this.taskDuration = in.readLong();
    super.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text text = new Text();
    text.set(this.jobId);
    text.write(out);
    text.set(this.taskId);
    text.write(out);
    out.writeLong(this.startTime);
    out.writeLong(this.endTime);
    out.writeLong(this.taskDuration);
    super.write(out);
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof TaskState)) {
      return false;
    }

    TaskState other = (TaskState) object;
    return super.equals(other) && this.jobId.equals(other.jobId) && this.taskId.equals(other.taskId);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + this.jobId.hashCode();
    result = prime * result + this.taskId.hashCode();
    return result;
  }

  /** @return Stringified form, including all properties, in pretty-printed JSON */
  public String toJsonString() {
    return toJsonString(true);
  }

  public String toJsonString(boolean includeProperties) {
    StringWriter stringWriter = new StringWriter();
    try (JsonWriter jsonWriter = new JsonWriter(stringWriter)) {
      jsonWriter.setIndent("\t");
      this.toJson(jsonWriter, includeProperties);
    } catch (IOException ioe) {
      // Ignored
    }
    return stringWriter.toString();
  }

  /**
   * Convert this {@link TaskState} to a json document.
   *
   * @param jsonWriter a {@link com.google.gson.stream.JsonWriter} used to write the json document
   * @throws IOException
   */
  public void toJson(JsonWriter jsonWriter, boolean keepConfig) throws IOException {
    jsonWriter.beginObject();

    jsonWriter.name("task id").value(this.getTaskId()).name("task state").value(this.getWorkingState().name())
        .name("start time").value(this.getStartTime()).name("end time").value(this.getEndTime()).name("duration")
        .value(this.getTaskDuration()).name("retry count")
        .value(this.getPropAsInt(ConfigurationKeys.TASK_RETRIES_KEY, 0));

    // Also add failure exception information if it exists. This information is useful even in the
    // case that the task finally succeeds so we know what happened in the course of task execution.
    if (getTaskFailureException().isPresent()) {
      jsonWriter.name("exception").value(getTaskFailureException().get());
    }

    if (keepConfig) {
      jsonWriter.name("properties");
      jsonWriter.beginObject();
      for (String key : this.getPropertyNames()) {
        jsonWriter.name(key).value(this.getProp(key));
      }
      jsonWriter.endObject();
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
    if (this.startTime > 0) {
      taskExecutionInfo.setStartTime(this.startTime);
    }
    if (this.endTime > 0) {
      taskExecutionInfo.setEndTime(this.endTime);
    }
    taskExecutionInfo.setDuration(this.taskDuration);
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
    TaskMetrics taskMetrics = TaskMetrics.get(this);
    MetricArray metricArray = new MetricArray();

    for (Map.Entry<String, ? extends com.codahale.metrics.Metric> entry : taskMetrics.getMetricContext().getCounters()
        .entrySet()) {
      Metric counter = new Metric();
      counter.setGroup(MetricGroup.TASK.name());
      counter.setName(entry.getKey());
      counter.setType(MetricTypeEnum.valueOf(GobblinMetrics.MetricType.COUNTER.name()));
      counter.setValue(Long.toString(((Counter) entry.getValue()).getCount()));
      metricArray.add(counter);
    }

    for (Map.Entry<String, ? extends com.codahale.metrics.Metric> entry : taskMetrics.getMetricContext().getMeters()
        .entrySet()) {
      Metric meter = new Metric();
      meter.setGroup(MetricGroup.TASK.name());
      meter.setName(entry.getKey());
      meter.setType(MetricTypeEnum.valueOf(GobblinMetrics.MetricType.METER.name()));
      meter.setValue(Double.toString(((Meter) entry.getValue()).getMeanRate()));
      metricArray.add(meter);
    }

    for (Map.Entry<String, ? extends com.codahale.metrics.Metric> entry : taskMetrics.getMetricContext().getGauges()
        .entrySet()) {
      Metric gauge = new Metric();
      gauge.setGroup(MetricGroup.TASK.name());
      gauge.setName(entry.getKey());
      gauge.setType(MetricTypeEnum.valueOf(GobblinMetrics.MetricType.GAUGE.name()));
      gauge.setValue(((Gauge<?>) entry.getValue()).getValue().toString());
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
