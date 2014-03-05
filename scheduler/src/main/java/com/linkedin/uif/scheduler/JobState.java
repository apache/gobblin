package com.linkedin.uif.scheduler;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.hadoop.io.Text;

import com.linkedin.uif.configuration.SourceState;

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
        PENDING, WORKING, FAILED, COMMITTED, ABORTED
    }

    private String jobName;
    private String jobId;
    private long startTime;
    private long endTime;
    private long duration;
    private RunningState state = RunningState.PENDING;
    private List<TaskState> taskStates;

    // Necessary for serialization/deserialization
    public JobState() {
        this.taskStates = Lists.newArrayList();
    }

    public JobState(String jobName, String jobId) {
        this.jobName = jobName;
        this.jobId = jobId;
        this.setId(jobId);
        this.taskStates = Lists.newArrayList();
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
     * Add {@link TaskState}s of {@link Task}s of this job.
     *
     * @param taskStates {@link TaskState}s to add
     */
    public void addTaskStates(List<TaskState> taskStates) {
        for (TaskState taskState : taskStates) {
            this.taskStates.add(taskState);
        }
    }

    /**
     * Get {@link TaskState}s of {@link Task}s of this job.
     *
     * @return {@link TaskState}s of {@link Task}s of this job
     */
    public List<TaskState> getTaskStates() {
        return Lists.newArrayList(this.taskStates);
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
        int numTaskStates = in.readInt();
        if (numTaskStates > 0) {
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
        out.writeInt(this.taskStates.size());
        for (TaskState taskState : taskStates) {
            taskState.write(out);
        }
        super.write(out);
    }
}
