package com.linkedin.uif.runtime;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.codahale.metrics.Counter;

import com.google.gson.stream.JsonWriter;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.metrics.Metrics;

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
    public TaskState() {}

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
        Metrics metrics = Metrics.get(this.getProp(ConfigurationKeys.JOB_NAME_KEY), this.jobId);

        Counter taskRecordCounter = metrics.getCounter(
                Metrics.metricName(Metrics.MetricGroup.TASK, this.taskId, "records"));
        long inc = recordsWritten - taskRecordCounter.getCount();

        taskRecordCounter.inc(inc);
        metrics.getMeter(Metrics.metricName(
                Metrics.MetricGroup.TASK, this.taskId, "recordsPerSec")).mark(inc);
        metrics.getCounter(Metrics.metricName(
                Metrics.MetricGroup.JOB, this.jobId, "records")).inc(inc);
        metrics.getMeter(Metrics.metricName(
                Metrics.MetricGroup.JOB, this.jobId, "recordsPerSec")).mark(inc);
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
        Metrics metrics = Metrics.get(this.getProp(ConfigurationKeys.JOB_NAME_KEY), this.jobId);
        metrics.getCounter(Metrics.metricName(
                Metrics.MetricGroup.TASK, this.taskId, "bytes")).inc(bytesWritten);
        metrics.getMeter(Metrics.metricName(
                Metrics.MetricGroup.TASK, this.taskId, "bytesPerSec")).mark(bytesWritten);
        metrics.getCounter(Metrics.metricName(
                Metrics.MetricGroup.JOB, this.jobId, "bytes")).inc(bytesWritten);
        metrics.getMeter(Metrics.metricName(
                Metrics.MetricGroup.JOB, this.jobId, "bytesPerSec")).mark(bytesWritten);
    }

    /**
     * Adjust job-level metrics when the task gets retried.
     */
    public void adjustJobMetricsOnRetry() {
        Metrics metrics = Metrics.get(this.getProp(ConfigurationKeys.JOB_NAME_KEY), this.jobId);
        long recordsWritten = metrics.getCounter(Metrics.metricName(
                Metrics.MetricGroup.TASK, this.taskId, "records")).getCount();
        long bytesWritten = metrics.getCounter(Metrics.metricName(
                Metrics.MetricGroup.TASK, this.taskId, "bytes")).getCount();
        metrics.getCounter(Metrics.metricName(
                Metrics.MetricGroup.JOB, this.jobId, "records")).dec(recordsWritten);
        metrics.getCounter(Metrics.metricName(
                Metrics.MetricGroup.JOB, this.jobId, "bytes")).dec(bytesWritten);
    }

    /**
     * Remove all task-level metrics objects associated with this task.
     */
    public void removeMetrics() {
        Metrics metrics = Metrics.get(this.getProp(ConfigurationKeys.JOB_NAME_KEY), this.jobId);
        metrics.removeMetric(Metrics.metricName(Metrics.MetricGroup.TASK, this.taskId, "records"));
        metrics.removeMetric(Metrics.metricName(Metrics.MetricGroup.TASK, this.taskId, "recordsPerSec"));
        metrics.removeMetric(Metrics.metricName(Metrics.MetricGroup.TASK, this.taskId, "bytes"));
        metrics.removeMetric(Metrics.metricName(Metrics.MetricGroup.TASK, this.taskId, "bytesPerSec"));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
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
    public void write(DataOutput out) throws IOException {
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
     * @param jsonWriter a {@link com.google.gson.stream.JsonWriter}
     *                   used to write the json document
     * @throws IOException
     */
    public void toJson(JsonWriter jsonWriter) throws IOException {
        jsonWriter.beginObject();

        jsonWriter.name("task id").value(this.getTaskId())
                .name("task state").value(this.getWorkingState().name())
                .name("start time").value(this.getStartTime())
                .name("end time").value(this.getEndTime())
                .name("duration").value(this.getTaskDuration())
                .name("high watermark").value(this.getHighWaterMark());

        jsonWriter.endObject();
    }
}
