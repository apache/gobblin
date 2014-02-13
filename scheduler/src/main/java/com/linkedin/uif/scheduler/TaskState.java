package com.linkedin.uif.scheduler;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.workunit.WorkUnit;

/**
 * An extension to {@link WorkUnitState} with run-time task state information.
 *
 * @author ynli
 */
public class TaskState extends WorkUnitState {

    public TaskState(WorkUnit workUnit) {
        super(workUnit);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
    }
}
