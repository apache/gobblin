package com.linkedin.uif.runtime;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Text;

import com.google.common.collect.Lists;
import com.google.gson.stream.JsonWriter;

import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.metrics.JobMetrics;

/**
 * A class for tracking job state information.
 *
 * @author ynli
 */
public class JobState extends SourceState {

    /**
     * An enumeration of possible job states, which are identical to
     * {@link com.linkedin.uif.configuration.WorkUnitState.WorkingState}
     * in terms of naming.
     */
    public enum RunningState {
        PENDING, WORKING, SUCCESSFUL, COMMITTED, FAILED, ABORTED
    }

    private String jobName;
    private String jobId;
    private long startTime;
    private long endTime;
    private long duration;
    private RunningState state = RunningState.PENDING;
    private int tasks;
    private List<TaskState> taskStates = Lists.newArrayList();

    // Necessary for serialization/deserialization
    public JobState() {}

    public JobState(String jobName, String jobId) {
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
    public RunningState getState() {
        return state;
    }

    /**
     * Set job running state of type {@link RunningState}.
     *
     * @param state job running state of type {@link RunningState}
     */
    public void setState(RunningState state) {
        this.state = state;
    }

    /**
     * Get the number of tasks this job consists of.
     *
     * @return number of tasks this job consists of
     */
    public int getTasks() {
        return this.tasks;
    }

    /**
     * Set the number of tasks this job consists of.
     *
     * @param tasks number of tasks this job consists of
     */
    public void setTasks(int tasks) {
        this.tasks = tasks;
    }

    /**
     * Add a single {@link TaskState}.
     *
     * @param taskState {@link TaskState} to add
     */
    public void addTaskState(TaskState taskState) {
        this.taskStates.add(taskState);
    }

    /**
     * Add a collection of {@link TaskState}s.
     *
     * @param taskStates collection of {@link TaskState}s to add
     */
    public void addTaskStates(Collection<TaskState> taskStates) {
        this.taskStates.addAll(taskStates);
    }

    /**
     * Get the number of completed tasks.
     *
     * @return number of completed tasks
     */
    public int getCompletedTasks() {
        return this.taskStates.size();
    }

    /**
     * Get {@link TaskState}s of {@link Task}s of this job.
     *
     * @return {@link TaskState}s of {@link Task}s of this job
     */
    public List<TaskState> getTaskStates() {
        return Collections.unmodifiableList(this.taskStates);
    }

    /**
     * Remove all job-level metrics objects associated with this job.
     */
    public void removeMetrics() {
        JobMetrics metrics = JobMetrics.get(this.jobName, this.jobId);
        metrics.removeMetric(JobMetrics.metricName(JobMetrics.MetricGroup.JOB, this.jobId, "records"));
        metrics.removeMetric(JobMetrics.metricName(JobMetrics.MetricGroup.JOB, this.jobId, "recordsPerSec"));
        metrics.removeMetric(JobMetrics.metricName(JobMetrics.MetricGroup.JOB, this.jobId, "bytes"));
        metrics.removeMetric(JobMetrics.metricName(JobMetrics.MetricGroup.JOB, this.jobId, "bytesPerSec"));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
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
        this.tasks = in.readInt();
        int numTaskStates = in.readInt();
        for (int i = 0; i < numTaskStates; i++) {
            TaskState taskState = new TaskState();
            taskState.readFields(in);
            this.taskStates.add(taskState);
        }
        super.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
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
        out.writeInt(this.tasks);
        out.writeInt(this.taskStates.size());
        for (TaskState taskState : this.taskStates) {
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
    public void toJson(JsonWriter jsonWriter, boolean keepConfig) throws IOException {
        jsonWriter.beginObject();

        jsonWriter.name("job name").value(this.getJobName())
                .name("job id").value(this.getJobId())
                .name("job state").value(this.getState().name())
                .name("start time").value(this.getStartTime())
                .name("end time").value(this.getEndTime())
                .name("duration").value(this.getDuration())
                .name("tasks").value(this.getTasks())
                .name("completed tasks").value(this.getCompletedTasks());

        jsonWriter.name("task states");
        jsonWriter.beginArray();
        for (TaskState taskState : taskStates) {
            taskState.toJson(jsonWriter);
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
}
