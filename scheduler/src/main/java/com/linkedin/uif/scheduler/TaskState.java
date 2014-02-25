package com.linkedin.uif.scheduler;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;

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
     * Get the ID of the task this {@link TaskState} is for.
     *
     * @return ID of the task this {@link TaskState} is for
     */
    public String getTaskId() {
        return this.taskId;
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

    @Override
    public void readFields(DataInput in) throws IOException {
        Text text = new Text();
        text.readFields(in);
        this.jobId = text.toString();
        text.readFields(in);
        this.taskId = text.toString();
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
}
