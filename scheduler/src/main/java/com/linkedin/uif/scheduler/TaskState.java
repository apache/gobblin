package com.linkedin.uif.scheduler;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.workunit.WorkUnit;
import org.apache.hadoop.io.Text;

/**
 * An extension to {@link WorkUnitState} with run-time task state information.
 *
 * @author ynli
 */
public class TaskState extends WorkUnitState {

    private String jobId;
    private String taskId;

    // Needed for serialization/deserialization
    public TaskState() {}

    public TaskState(WorkUnitState workUnitState) {
        // Since getWorkunit() returns an immutable WorkUnit object,
        // the WorkUnit object in this object is also immutable.
        super(workUnitState.getWorkunit());
        this.jobId = workUnitState.getProp(ConfigurationKeys.JOB_ID_KEY);
        this.taskId = workUnitState.getProp(ConfigurationKeys.TASK_ID_KEY);
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

    @Override
    public void readFields(DataInput in) throws IOException {
        Text text = new Text();
        text.readFields(in);
        this.jobId = text.toString();
        text.readFields(in);
        this.taskId = text.toString();
        super.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text text = new Text();
        text.set(this.jobId);
        text.write(out);
        text.set(this.taskId);
        text.write(out);
        super.write(out);
    }
}
