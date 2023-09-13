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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import lombok.Getter;
import lombok.Setter;

import org.apache.gobblin.metastore.DatasetStateStore;
import org.apache.gobblin.runtime.job.JobProgress;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.google.common.base.Enums;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.stream.JsonWriter;
import com.linkedin.data.template.StringMap;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.rest.JobExecutionInfo;
import org.apache.gobblin.rest.JobStateEnum;
import org.apache.gobblin.rest.LauncherTypeEnum;
import org.apache.gobblin.rest.Metric;
import org.apache.gobblin.rest.MetricArray;
import org.apache.gobblin.rest.MetricTypeEnum;
import org.apache.gobblin.rest.TaskExecutionInfoArray;
import org.apache.gobblin.runtime.api.MonitoredObject;
import org.apache.gobblin.runtime.util.JobMetrics;
import org.apache.gobblin.runtime.util.MetricGroup;
import org.apache.gobblin.source.extractor.JobCommitPolicy;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.ImmutableProperties;


/**
 * A class for tracking job state information.
 *
 * @author Yinan Li
 */
public class JobState extends SourceState implements JobProgress {

  /**
   * An enumeration of possible job states, which are identical to
   * {@link org.apache.gobblin.configuration.WorkUnitState.WorkingState}
   * in terms of naming.
   *
   * <p> Status state diagram:
   * <ul>
   *    <li> null => PENDING
   *    <li> PENDING => RUNNING
   *    <li> PENDING => CANCELLED
   *    <li> RUNNING => CANCELLED
   *    <li> RUNNING => SUCCESSFUL
   *    <li> RUNNING => FAILED
   *    <li> SUCCESSFUL => COMMITTED
   *    <li> SUCCESSFUL => CANCELLED  (cancelled before committing)
   * </ul>
   */
  public enum RunningState implements MonitoredObject {
    /** Pending creation of {@link WorkUnit}s. */
    PENDING,
    /** Starting the execution of {@link WorkUnit}s. */
    RUNNING,
    /** All {@link WorkUnit}s have finished successfully or the job commit policy is
     * {@link JobCommitPolicy#COMMIT_ON_PARTIAL_SUCCESS} */
    SUCCESSFUL,
    /** Job state has been committed */
    COMMITTED,
    /** At least one {@link WorkUnit}s has failed for a job with job commit policy
     *  {@link JobCommitPolicy#COMMIT_ON_FULL_SUCCESS}. */
    FAILED,
    /** The execution of the job was cancelled. */
    CANCELLED;

    public boolean isCancelled() {
      return this.equals(CANCELLED);
    }

    public boolean isDone() {
      return this.equals(COMMITTED) || this.equals(FAILED) || this.equals(CANCELLED);
    }

    public boolean isSuccess() {
      return this.equals(COMMITTED);
    }

    public boolean isFailure() {
      return this.equals(FAILED);
    }

    public boolean isRunningOrDone() {
      return isDone() || this.equals(RUNNING);
    }
  }

  @Getter @Setter
  private String jobName;
  @Getter @Setter
  private String jobId;
  /** job start time in milliseconds */
  @Getter @Setter
  private long startTime = 0;
  /** job end time in milliseconds */
  @Getter @Setter
  private long endTime = 0;
  /** job duration in milliseconds */
  @Getter @Setter
  private long duration = 0;
  private RunningState state = RunningState.PENDING;
  /** the number of tasks this job consists of */
  @Getter @Setter
  private int taskCount = 0;
  private final Map<String, TaskState> taskStates = Maps.newLinkedHashMap();
  // Skipped task states shouldn't be exposed to publisher, but they need to be in JobState and DatasetState so that they can be written to StateStore.
  private final Map<String, TaskState> skippedTaskStates = Maps.newLinkedHashMap();
  private DatasetStateStore datasetStateStore;

  // Necessary for serialization/deserialization
  public JobState() {
  }

  public JobState(String jobName, String jobId) {
    this.jobName = jobName;
    this.jobId = jobId;
    this.setId(jobId);
  }

  public JobState(State properties, String jobName, String jobId) {
    super(properties);
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

  public static String getJobNameFromState(State state) {
    return state.getProp(ConfigurationKeys.JOB_NAME_KEY);
  }

  public static String getJobNameFromProps(Properties props) {
    return props.getProperty(ConfigurationKeys.JOB_NAME_KEY);
  }

  public static String getJobGroupFromState(State state) {
    return state.getProp(ConfigurationKeys.JOB_GROUP_KEY);
  }

  public static String getJobGroupFromProps(Properties props) {
    return props.getProperty(ConfigurationKeys.JOB_GROUP_KEY);
  }

  public static String getJobDescriptionFromProps(State state) {
    return state.getProp(ConfigurationKeys.JOB_DESCRIPTION_KEY);
  }

  public static String getJobDescriptionFromProps(Properties props) {
    return props.getProperty(ConfigurationKeys.JOB_DESCRIPTION_KEY);
  }

  /**
   * Get the currently elapsed time for this job.
   * @return
   */
  public long getElapsedTime() {
    if (this.endTime > 0) {
      return  this.endTime - this.startTime;
    }
    if (this.startTime > 0) {
      return System.currentTimeMillis() - this.startTime;
    }
    return 0;
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
   * If not already present, set the {@link ConfigurationKeys#JOB_FAILURE_EXCEPTION_KEY} to a {@link String}
   * representation of the given {@link Throwable}.
   */
  public void setJobFailureException(Throwable jobFailureException) {
    String previousExceptions = this.getProp(ConfigurationKeys.JOB_FAILURE_EXCEPTION_KEY);
    String currentException = Throwables.getStackTraceAsString(jobFailureException);
    String aggregatedExceptions;

    if (StringUtils.isEmpty(previousExceptions)) {
      aggregatedExceptions = currentException;
    } else {
      aggregatedExceptions = currentException + "\n\n" + previousExceptions;
    }

    this.setProp(ConfigurationKeys.JOB_FAILURE_EXCEPTION_KEY, aggregatedExceptions);
  }

  /**
   * If not already present, set the {@link EventMetadataUtils#JOB_FAILURE_MESSAGE_KEY} to the given {@link String}.
   */
  public void setJobFailureMessage(String jobFailureMessage) {
    String previousMessages = this.getProp(ConfigurationKeys.JOB_FAILURE_EXCEPTION_KEY);
    String aggregatedMessages;

    if (StringUtils.isEmpty(previousMessages)) {
      aggregatedMessages = jobFailureMessage;
    } else {
      aggregatedMessages = jobFailureMessage + ", " + previousMessages;
    }

    this.setProp(EventMetadataUtils.JOB_FAILURE_MESSAGE_KEY, aggregatedMessages);
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

  public void addSkippedTaskState(TaskState taskState) {
    this.skippedTaskStates.put(taskState.getTaskId(), taskState);
  }

  public void removeTaskState(TaskState taskState) {
    this.taskStates.remove(taskState.getTaskId());
    this.taskCount--;
  }

  /**
   * Filter the task states corresponding to the skipped work units and add it to the skippedTaskStates
   */
  public void filterSkippedTaskStates() {
    List<TaskState> skippedTaskStates = new ArrayList<>();
    for (TaskState taskState : this.taskStates.values()) {
      if (taskState.getWorkingState() == WorkUnitState.WorkingState.SKIPPED) {
        skippedTaskStates.add(taskState);
      }
    }
    for (TaskState taskState : skippedTaskStates) {
      removeTaskState(taskState);
      addSkippedTaskState(taskState);
    }
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

  public void addSkippedTaskStates(Collection<TaskState> taskStates) {
    for (TaskState taskState : taskStates) {
      addSkippedTaskState(taskState);
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

  @Override
  public List<TaskState> getTaskProgress() {
    return getTaskStates();
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
      String datasetUrn = createDatasetUrn(datasetStatesByUrns, taskState);

      datasetStatesByUrns.get(datasetUrn).incrementTaskCount();
      datasetStatesByUrns.get(datasetUrn).addTaskState(taskState);
    }

    for (TaskState taskState : this.skippedTaskStates.values()) {
      String datasetUrn = createDatasetUrn(datasetStatesByUrns, taskState);
      datasetStatesByUrns.get(datasetUrn).addSkippedTaskState(taskState);
    }

    return ImmutableMap.copyOf(datasetStatesByUrns);
  }

  private String createDatasetUrn(Map<String, DatasetState> datasetStatesByUrns, TaskState taskState) {
    String datasetUrn = taskState.getProp(ConfigurationKeys.DATASET_URN_KEY, ConfigurationKeys.DEFAULT_DATASET_URN);
    if (!datasetStatesByUrns.containsKey(datasetUrn)) {
      DatasetState datasetState = newDatasetState(false);
      datasetState.setDatasetUrn(datasetUrn);
      datasetStatesByUrns.put(datasetUrn, datasetState);
    }
    return datasetUrn;
  }

  /**
   * Get task states of {@link Task}s of this job as {@link WorkUnitState}s.
   *
   * @return a list of {@link WorkUnitState}s
   */
  public List<WorkUnitState> getTaskStatesAsWorkUnitStates() {
    ImmutableList.Builder<WorkUnitState> builder = ImmutableList.builder();
    for (TaskState taskState : this.taskStates.values()) {
      WorkUnitState workUnitState = new WorkUnitState(taskState.getWorkunit(), taskState.getJobState());
      workUnitState.setId(taskState.getId());
      workUnitState.addAll(taskState);
      builder.add(workUnitState);
    }

    return builder.build();
  }

  /**
   * Get the {@link LauncherTypeEnum} for this {@link JobState}.
   */
  public LauncherTypeEnum getLauncherType() {
    return Enums.getIfPresent(LauncherTypeEnum.class,
        this.getProp(ConfigurationKeys.JOB_LAUNCHER_TYPE_KEY, JobLauncherFactory.JobLauncherType.LOCAL.name()))
        .or(LauncherTypeEnum.LOCAL);
  }

  /**
   * Sets the {@link LauncherTypeEnum} for this {@link JobState}.
   */
  public void setJobLauncherType(LauncherTypeEnum jobLauncherType) {
    this.setProp(ConfigurationKeys.JOB_LAUNCHER_TYPE_KEY, jobLauncherType.name());
  }

  /**
   * Get the tracking URL for this {@link JobState}.
   */
  public Optional<String> getTrackingURL() {
    return Optional.fromNullable(this.getProp(ConfigurationKeys.JOB_TRACKING_URL_KEY));
  }

  @Override
  public void readFields(DataInput in)
      throws IOException {
    Text text = new Text();
    text.readFields(in);
    this.jobName = text.toString().intern();
    text.readFields(in);
    this.jobId = text.toString().intern();
    this.setId(this.jobId);
    this.startTime = in.readLong();
    this.endTime = in.readLong();
    this.duration = in.readLong();
    text.readFields(in);
    this.state = RunningState.valueOf(text.toString());
    this.taskCount = in.readInt();
    int numTaskStates = in.readInt();
    getTaskStateWithCommonAndSpecWuProps(numTaskStates, in);
    super.readFields(in);
  }

  private void getTaskStateWithCommonAndSpecWuProps(int numTaskStates, DataInput in)
      throws IOException {
    Properties commonWuProps = new Properties();

    for (int i = 0; i < numTaskStates; i++) {
      TaskState taskState = new TaskState();
      taskState.readFields(in);
      if (i == 0) {
        commonWuProps.putAll(taskState.getWorkunit().getProperties());
      } else {
        Properties newCommonWuProps = new Properties();
        newCommonWuProps
            .putAll(Maps.difference(commonWuProps, taskState.getWorkunit().getProperties()).entriesInCommon());
        commonWuProps = newCommonWuProps;
      }

      this.taskStates.put(taskState.getTaskId().intern(), taskState);
    }
    ImmutableProperties immutableCommonProperties = new ImmutableProperties(commonWuProps);
    for (TaskState taskState : this.taskStates.values()) {
      Properties newSpecProps = new Properties();
      newSpecProps.putAll(
          Maps.difference(immutableCommonProperties, taskState.getWorkunit().getProperties()).entriesOnlyOnRight());
      taskState.setWuProperties(immutableCommonProperties, newSpecProps);
    }
  }

  @Override
  public void write(DataOutput out)
      throws IOException {
    write(out, true, true);
  }

  public void write(DataOutput out, boolean writeTasks, boolean writePreviousWorkUnitStates)
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
    if (writeTasks) {
      out.writeInt(this.taskStates.size() + this.skippedTaskStates.size());
      for (TaskState taskState : this.taskStates.values()) {
        taskState.write(out);
      }
      for (TaskState taskState : this.skippedTaskStates.values()) {
        taskState.write(out);
      }
    } else {
      out.writeInt(0);
    }
    super.write(out, writePreviousWorkUnitStates);
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
    writeStateSummary(jsonWriter);

    jsonWriter.name("task states");
    jsonWriter.beginArray();
    for (TaskState taskState : this.taskStates.values()) {
      taskState.toJson(jsonWriter, keepConfig);
    }
    for (TaskState taskState : this.skippedTaskStates.values()) {
      taskState.toJson(jsonWriter, keepConfig);
    }
    jsonWriter.endArray();

    if (keepConfig) {
      jsonWriter.name("properties");
      propsToJson(jsonWriter);
    }

    jsonWriter.endObject();
  }

  /**
   * Write a summary to the json document
   *
   * @param jsonWriter a {@link com.google.gson.stream.JsonWriter}
   *                   used to write the json document
   */
  protected void writeStateSummary(JsonWriter jsonWriter) throws IOException {
    jsonWriter.name("job name").value(this.getJobName()).name("job id").value(this.getJobId()).name("job state")
        .value(this.getState().name()).name("start time").value(this.getStartTime()).name("end time")
        .value(this.getEndTime()).name("duration").value(this.getDuration()).name("tasks").value(this.getTaskCount())
        .name("completed tasks").value(this.getCompletedTasks());
  }

  protected void propsToJson(JsonWriter jsonWriter)
      throws IOException {
    jsonWriter.beginObject();
    for (String key : this.getPropertyNames()) {
      jsonWriter.name(key).value(this.getProp(key));
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
    try (JsonWriter jsonWriter = new JsonWriter(stringWriter)) {
      jsonWriter.setIndent("\t");
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
    jobExecutionInfo.setLauncherType(getLauncherType());
    if (getTrackingURL().isPresent()) {
      jobExecutionInfo.setTrackingUrl(getTrackingURL().get());
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

    for (Map.Entry<String, ? extends com.codahale.metrics.Metric> entry : jobMetrics.getMetricContext().getCounters()
        .entrySet()) {
      Metric counter = new Metric();
      counter.setGroup(MetricGroup.JOB.name());
      counter.setName(entry.getKey());
      counter.setType(MetricTypeEnum.valueOf(GobblinMetrics.MetricType.COUNTER.name()));
      counter.setValue(Long.toString(((Counter) entry.getValue()).getCount()));
      metricArray.add(counter);
    }

    for (Map.Entry<String, ? extends com.codahale.metrics.Metric> entry : jobMetrics.getMetricContext().getMeters()
        .entrySet()) {
      Metric meter = new Metric();
      meter.setGroup(MetricGroup.JOB.name());
      meter.setName(entry.getKey());
      meter.setType(MetricTypeEnum.valueOf(GobblinMetrics.MetricType.METER.name()));
      meter.setValue(Double.toString(((Meter) entry.getValue()).getMeanRate()));
      metricArray.add(meter);
    }

    for (Map.Entry<String, ? extends com.codahale.metrics.Metric> entry : jobMetrics.getMetricContext().getGauges()
        .entrySet()) {
      Metric gauge = new Metric();
      gauge.setGroup(MetricGroup.JOB.name());
      gauge.setName(entry.getKey());
      gauge.setType(MetricTypeEnum.valueOf(GobblinMetrics.MetricType.GAUGE.name()));
      gauge.setValue(((Gauge<?>) entry.getValue()).getValue().toString());
      metricArray.add(gauge);
    }

    jobExecutionInfo.setMetrics(metricArray);

    // Add job properties
    Map<String, String> jobProperties = Maps.newHashMap();
    for (String name : this.getPropertyNames()) {
      String value = this.getProp(name);
      if (!Strings.isNullOrEmpty(value)) {
        jobProperties.put(name, value);
      }
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
    datasetState.setStartTime(this.startTime);
    datasetState.setEndTime(this.endTime);
    datasetState.setDuration(this.duration);
    if (fullCopy) {
      datasetState.setState(this.state);
      datasetState.setTaskCount(this.taskCount);
      datasetState.addTaskStates(this.taskStates.values());
      datasetState.addSkippedTaskStates(this.skippedTaskStates.values());
    }
    return datasetState;
  }

  public static List<WorkUnitState> workUnitStatesFromDatasetStates(Iterable<JobState.DatasetState> datasetStates) {
    ImmutableList.Builder<WorkUnitState> taskStateBuilder = ImmutableList.builder();
    for (JobState datasetState : datasetStates) {
      taskStateBuilder.addAll(datasetState.getTaskStatesAsWorkUnitStates());
    }
    return taskStateBuilder.build();
  }

  /**
   * A subclass of {@link JobState} that is used to represent dataset states.
   *
   * <p>
   *   A {@code DatasetState} does <em>not</em> contain any properties. Operations such as {@link #getProp(String)}
   *   and {@link #setProp(String, Object)} are not supported.
   * </p>
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
      super.setProp(ConfigurationKeys.DATASET_URN_KEY, datasetUrn);
    }

    public String getDatasetUrn() {
      return super.getProp(ConfigurationKeys.DATASET_URN_KEY, ConfigurationKeys.DEFAULT_DATASET_URN);
    }

    public void incrementJobFailures() {
      super.setProp(ConfigurationKeys.JOB_FAILURES_KEY,
          Integer.parseInt(super.getProp(ConfigurationKeys.JOB_FAILURES_KEY, "0")) + 1);
    }

    public void setNoJobFailure() {
      super.setProp(ConfigurationKeys.JOB_FAILURES_KEY, 0);
    }

    public int getJobFailures() {
      return Integer.parseInt(super.getProp(ConfigurationKeys.JOB_FAILURES_KEY));
    }

    @Override
    protected void propsToJson(JsonWriter jsonWriter)
        throws IOException {
      jsonWriter.beginObject();
      jsonWriter.name(ConfigurationKeys.DATASET_URN_KEY).value(getDatasetUrn());
      jsonWriter.name(ConfigurationKeys.JOB_FAILURES_KEY).value(getJobFailures());
      jsonWriter.endObject();
    }

    @Override
    public String getProp(String key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getProp(String key, String def) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setProp(String key, Object value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void addAll(Properties properties) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void addAllIfNotExist(Properties properties) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void overrideWith(Properties properties) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void writeStateSummary(JsonWriter jsonWriter)
        throws IOException {
      super.writeStateSummary(jsonWriter);
      jsonWriter.name("datasetUrn").value(getDatasetUrn());
    }
  }
}
