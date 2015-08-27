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

package gobblin.runtime;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.stream.JsonWriter;

import com.linkedin.data.template.StringMap;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.rest.JobExecutionInfo;
import gobblin.rest.JobStateEnum;
import gobblin.rest.LauncherTypeEnum;
import gobblin.rest.Metric;
import gobblin.rest.MetricArray;
import gobblin.rest.MetricTypeEnum;
import gobblin.rest.TaskExecutionInfoArray;
import gobblin.metrics.GobblinMetrics;
import gobblin.runtime.util.JobMetrics;


/**
 * A class for tracking job state information.
 *
 * @author ynli
 */
public class JobState extends SourceState {

  /**
   * An enumeration of possible job states, which are identical to
   * {@link gobblin.configuration.WorkUnitState.WorkingState}
   * in terms of naming.
   */
  public enum RunningState {
    PENDING, RUNNING, SUCCESSFUL, COMMITTED, FAILED, CANCELLED
  }

  private String jobName;
  private String jobId;
  private long startTime = 0;
  private long endTime = 0;
  private long duration = 0;
  private RunningState state = RunningState.PENDING;
  private int taskCount = 0;
  private final Map<String, TaskState> taskStates = Maps.newLinkedHashMap();

  // Necessary for serialization/deserialization
  public JobState() {
  }

  public JobState(String jobName, String jobId) {
    this.jobName = jobName;
    this.jobId = jobId;
    this.setId(jobId);
  }

  public JobState(State properties, Map<String, JobState.DatasetState> previousDatasetStates, String jobName,
      String jobId) {
    super(properties, previousDatasetStates, workUnitStatesFromDatasetStates(previousDatasetStates.values()));
    this.jobName = jobName;
    this.jobId = jobId;
    this.setId(jobId);
  }

  /**
   * Get job name.
   *
   * @return job name
   */
  public String getJobName() {
    return this.jobName;
  }

  /**
   * Set job name.
   *
   * @param jobName job name
   */
  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  /**
   * Get job ID.
   *
   * @return job ID
   */
  public String getJobId() {
    return jobId;
  }

  /**
   * Set job ID.
   *
   * @param jobId job ID
   */
  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  /**
   * Get job start time.
   *
   * @return job start time
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * Set job start time.
   *
   * @param startTime job start time
   */
  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  /**
   * Get job end time.
   *
   * @return job end time
   */
  public long getEndTime() {
    return endTime;
  }

  /**
   * Set job end time.
   *
   * @param endTime job end time
   */
  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  /**
   * Get job duration in milliseconds.
   *
   * @return job duration in milliseconds
   */
  public long getDuration() {
    return duration;
  }

  /**
   * Set job duration in milliseconds.
   *
   * @param duration job duration in milliseconds
   */
  public void setDuration(long duration) {
    this.duration = duration;
  }

  /**
   * Get job running state of type {@link RunningState}.
   *
   * @return job running state of type {@link RunningState}
   */
  public synchronized RunningState getState() {
    return this.state;
  }

  /**
   * Set job running state of type {@link RunningState}.
   *
   * @param state job running state of type {@link RunningState}
   */
  public synchronized void setState(RunningState state) {
    this.state = state;
  }

  /**
   * Get the number of tasks this job consists of.
   *
   * @return number of tasks this job consists of
   */
  public int getTaskCount() {
    return this.taskCount;
  }

  /**
   * Set the number of tasks this job consists of.
   *
   * @param taskCount number of tasks this job consists of
   */
  public void setTaskCount(int taskCount) {
    this.taskCount = taskCount;
  }

  /**
   * Increment the number of tasks by 1.
   */
  public void incrementTaskCount() {
    this.taskCount++;
  }

  /**
   * Add a single {@link TaskState}.
   *
   * @param taskState {@link TaskState} to add
   */
  public void addTaskState(TaskState taskState) {
    this.taskStates.put(taskState.getTaskId(), taskState);
  }

  /**
   * Add a collection of {@link TaskState}s.
   *
   * @param taskStates collection of {@link TaskState}s to add
   */
  public void addTaskStates(Collection<TaskState> taskStates) {
    for (TaskState taskState : taskStates) {
      this.taskStates.put(taskState.getTaskId(), taskState);
    }
  }

  /**
   * Get the number of completed tasks.
   *
   * @return number of completed tasks
   */
  public int getCompletedTasks() {
    int completedTasks = 0;
    for (TaskState taskState : this.taskStates.values()) {
      if (taskState.isCompleted()) {
        completedTasks++;
      }
    }

    return completedTasks;
  }

  /**
   * Get {@link TaskState}s of {@link Task}s of this job.
   *
   * @return a list of {@link TaskState}s
   */
  public List<TaskState> getTaskStates() {
    return ImmutableList.<TaskState>builder().addAll(this.taskStates.values()).build();
  }

  /**
   * Create a {@link Map} from dataset URNs (as being specified by {@link ConfigurationKeys#DATASET_URN_KEY} to
   * {@link DatasetState} objects that represent the dataset states and store {@link TaskState}s corresponding
   * to the datasets.
   *
   * <p>
   *   {@link TaskState}s that do not have {@link ConfigurationKeys#DATASET_URN_KEY} set will be added to
   *   the dataset state belonging to {@link ConfigurationKeys#DEFAULT_DATASET_URN}.
   * </p>
   *
   * @return a {@link Map} from dataset URNs to {@link DatasetState}s representing the dataset states
   */
  public Map<String, DatasetState> createDatasetStatesByUrns() {
    Map<String, DatasetState> datasetStatesByUrns = Maps.newHashMap();

    for (TaskState taskState : this.taskStates.values()) {
      String datasetUrn = taskState.getProp(ConfigurationKeys.DATASET_URN_KEY, ConfigurationKeys.DEFAULT_DATASET_URN);
      if (!datasetStatesByUrns.containsKey(datasetUrn)) {
        DatasetState datasetState = newDatasetState(false);
        datasetState.setProp(ConfigurationKeys.DATASET_URN_KEY, datasetUrn);
        datasetStatesByUrns.put(datasetUrn, datasetState);
      }

      datasetStatesByUrns.get(datasetUrn).incrementTaskCount();
      datasetStatesByUrns.get(datasetUrn).addTaskState(taskState);
    }

    return ImmutableMap.copyOf(datasetStatesByUrns);
  }

  /**
   * Get task states of {@link Task}s of this job as {@link WorkUnitState}s.
   *
   * @return a list of {@link WorkUnitState}s
   */
  public List<WorkUnitState> getTaskStatesAsWorkUnitStates() {
    ImmutableList.Builder<WorkUnitState> builder = ImmutableList.builder();
    for (TaskState taskState : this.taskStates.values()) {
      WorkUnitState workUnitState = new WorkUnitState(taskState.getWorkunit());
      workUnitState.setId(taskState.getId());
      workUnitState.addAll(taskState);
      builder.add(workUnitState);
    }

    return builder.build();
  }

  @Override
  public void readFields(DataInput in)
      throws IOException {
    Text text = new Text();
    text.readFields(in);
    this.jobName = text.toString();
    text.readFields(in);
    this.jobId = text.toString();
    this.setId(jobId);
    this.startTime = in.readLong();
    this.endTime = in.readLong();
    this.duration = in.readLong();
    text.readFields(in);
    this.state = RunningState.valueOf(text.toString());
    this.taskCount = in.readInt();
    int numTaskStates = in.readInt();
    for (int i = 0; i < numTaskStates; i++) {
      TaskState taskState = new TaskState();
      taskState.readFields(in);
      this.taskStates.put(taskState.getTaskId(), taskState);
    }
    super.readFields(in);
  }

  @Override
  public void write(DataOutput out)
      throws IOException {
    Text text = new Text();
    text.set(this.jobName);
    text.write(out);
    text.set(this.jobId);
    text.write(out);
    out.writeLong(this.startTime);
    out.writeLong(this.endTime);
    out.writeLong(this.duration);
    text.set(this.state.name());
    text.write(out);
    out.writeInt(this.taskCount);
    out.writeInt(this.taskStates.size());
    for (TaskState taskState : this.taskStates.values()) {
      taskState.write(out);
    }
    super.write(out);
  }

  /**
   * Convert this {@link JobState} to a json document.
   *
   * @param jsonWriter a {@link com.google.gson.stream.JsonWriter}
   *                   used to write the json document
   * @param keepConfig whether to keep all configuration properties
   * @throws IOException
   */
  public void toJson(JsonWriter jsonWriter, boolean keepConfig)
      throws IOException {
    jsonWriter.beginObject();

    jsonWriter.name("job name").value(this.getJobName()).name("job id").value(this.getJobId()).name("job state")
        .value(this.getState().name()).name("start time").value(this.getStartTime()).name("end time")
        .value(this.getEndTime()).name("duration").value(this.getDuration()).name("tasks").value(this.getTaskCount())
        .name("completed tasks").value(this.getCompletedTasks());

    jsonWriter.name("task states");
    jsonWriter.beginArray();
    for (TaskState taskState : taskStates.values()) {
      taskState.toJson(jsonWriter, keepConfig);
    }
    jsonWriter.endArray();

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

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof JobState)) {
      return false;
    }

    JobState other = (JobState) object;
    return super.equals(other) && this.jobName.equals(other.jobName) && this.jobId.equals(other.jobId);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + this.jobName.hashCode();
    result = prime * result + this.jobId.hashCode();
    return result;
  }

  @Override
  public String toString() {
    StringWriter stringWriter = new StringWriter();
    JsonWriter jsonWriter = new JsonWriter(stringWriter);
    jsonWriter.setIndent("\t");
    try {
      this.toJson(jsonWriter, false);
    } catch (IOException ioe) {
      // Ignored
    }
    return stringWriter.toString();
  }

  /**
   * Convert this {@link JobState} instance to a {@link JobExecutionInfo} instance.
   *
   * @return a {@link JobExecutionInfo} instance
   */
  public JobExecutionInfo toJobExecutionInfo() {
    JobExecutionInfo jobExecutionInfo = new JobExecutionInfo();

    jobExecutionInfo.setJobName(this.jobName);
    jobExecutionInfo.setJobId(this.jobId);
    if (this.startTime > 0) {
      jobExecutionInfo.setStartTime(this.startTime);
    }
    if (this.endTime > 0) {
      jobExecutionInfo.setEndTime(this.endTime);
    }
    jobExecutionInfo.setDuration(this.duration);
    jobExecutionInfo.setState(JobStateEnum.valueOf(this.state.name()));
    jobExecutionInfo.setLaunchedTasks(this.taskCount);
    jobExecutionInfo.setCompletedTasks(this.getCompletedTasks());
    jobExecutionInfo.setLauncherType(LauncherTypeEnum.valueOf(this.getProp(ConfigurationKeys.JOB_LAUNCHER_TYPE_KEY,
        JobLauncherFactory.JobLauncherType.LOCAL.name())));
    if (this.contains(ConfigurationKeys.JOB_TRACKING_URL_KEY)) {
      jobExecutionInfo.setTrackingUrl(this.getProp(ConfigurationKeys.JOB_TRACKING_URL_KEY));
    }

    // Add task execution information
    TaskExecutionInfoArray taskExecutionInfos = new TaskExecutionInfoArray();
    for (TaskState taskState : this.getTaskStates()) {
      taskExecutionInfos.add(taskState.toTaskExecutionInfo());
    }
    jobExecutionInfo.setTaskExecutions(taskExecutionInfos);

    // Add job metrics
    JobMetrics jobMetrics = JobMetrics.get(this);
    MetricArray metricArray = new MetricArray();

    for (Map.Entry<String, ? extends com.codahale.metrics.Metric> entry : jobMetrics.getMetricContext()
        .getCounters().entrySet()) {
      Metric counter = new Metric();
      counter.setGroup(MetricGroup.JOB.name());
      counter.setName(entry.getKey());
      counter.setType(MetricTypeEnum.valueOf(GobblinMetrics.MetricType.COUNTER.name()));
      counter.setValue(Long.toString(((Counter) entry.getValue()).getCount()));
      metricArray.add(counter);
    }

    for (Map.Entry<String, ? extends com.codahale.metrics.Metric> entry : jobMetrics.getMetricContext()
        .getMeters().entrySet()) {
      Metric meter = new Metric();
      meter.setGroup(MetricGroup.JOB.name());
      meter.setName(entry.getKey());
      meter.setType(MetricTypeEnum.valueOf(GobblinMetrics.MetricType.METER.name()));
      meter.setValue(Double.toString(((Meter) entry.getValue()).getMeanRate()));
      metricArray.add(meter);
    }

    for (Map.Entry<String, ? extends com.codahale.metrics.Metric> entry : jobMetrics.getMetricContext()
        .getGauges().entrySet()) {
      Metric gauge = new Metric();
      gauge.setGroup(MetricGroup.JOB.name());
      gauge.setName(entry.getKey());
      gauge.setType(MetricTypeEnum.valueOf(GobblinMetrics.MetricType.GAUGE.name()));
      gauge.setValue(((Gauge) entry.getValue()).getValue().toString());
      metricArray.add(gauge);
    }

    jobExecutionInfo.setMetrics(metricArray);

    // Add job properties
    Map<String, String> jobProperties = Maps.newHashMap();
    for (String name : this.getPropertyNames()) {
      String value = this.getProp(name);
      if (!Strings.isNullOrEmpty(value))
      jobProperties.put(name, value);
    }
    jobExecutionInfo.setJobProperties(new StringMap(jobProperties));

    return jobExecutionInfo;
  }

  /**
   * Create a new {@link JobState.DatasetState} based on this {@link JobState} instance.
   *
   * @param fullCopy whether to do a full copy of this {@link JobState} instance
   * @return a new {@link JobState.DatasetState} object
   */
  public DatasetState newDatasetState(boolean fullCopy) {
    DatasetState datasetState = new DatasetState(this.jobName, this.jobId);
    datasetState.addAll(this);
    if (fullCopy) {
      datasetState.setState(this.state);
      datasetState.setTaskCount(this.taskCount);
      datasetState.addTaskStates(this.taskStates.values());
    }
    return datasetState;
  }

  private static List<WorkUnitState> workUnitStatesFromDatasetStates(Iterable<JobState.DatasetState> datasetStates) {
    ImmutableList.Builder<WorkUnitState> taskStateBuilder = ImmutableList.builder();
    for (JobState datasetState : datasetStates) {
      taskStateBuilder.addAll(datasetState.getTaskStatesAsWorkUnitStates());
    }
    return taskStateBuilder.build();
  }

  /**
   * A subclass of {@link JobState} that is used to represent dataset states. This class is currently
   * identical to {@link JobState} except that the name is more meaningful and less confusing.
   */
  public static class DatasetState extends JobState {

    // For serialization/deserialization
    public DatasetState() {
      super();
    }

    public DatasetState(String jobName, String jobId) {
      super(jobName, jobId);
    }

    public void setDatasetUrn(String datasetUrn) {
      setProp(ConfigurationKeys.DATASET_URN_KEY, datasetUrn);
    }

    public String getDatasetUrn() {
      return getProp(ConfigurationKeys.DATASET_URN_KEY, ConfigurationKeys.DEFAULT_DATASET_URN);
    }
  }
}
