package com.linkedin.uif.configuration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.linkedin.uif.source.workunit.ImmutableWorkUnit;
import com.linkedin.uif.source.workunit.WorkUnit;

/**
 *
 * @author kgoodhop
 *
 */
public class WorkUnitState extends State
{
  public enum WorkingState
  {
    PENDING, WORKING, FAILED, COMMITTED, ABORTED
  }

  private WorkUnit workunit;

    // Necessary for serialization/deserialization
  public WorkUnitState() {
    this.workunit = new WorkUnit(null, null);
  }

  public WorkUnitState(WorkUnit workUnit) {
      this.workunit = workUnit;
  }

  public WorkUnit getWorkunit()
  {
    return new ImmutableWorkUnit(workunit);
  }

  public WorkingState getWorkingState()
  {
    return WorkingState.valueOf(getProp(ConfigurationKeys.WORK_UNIT_WORKING_STATE_KEY, WorkingState.PENDING.toString()));
  }

  public void setWorkingState(WorkingState state)
  {
    setProp(ConfigurationKeys.WORK_UNIT_WORKING_STATE_KEY, state.toString());
  }

  public long getHighWaterMark()
  {
    return getPropAsLong(ConfigurationKeys.WORK_UNIT_STATE_RUNTIME_HIGH_WATER_MARK);
  }

  public void setHighWaterMark(long value)
  {
    setProp(ConfigurationKeys.WORK_UNIT_STATE_RUNTIME_HIGH_WATER_MARK, value);
  }

  @Override
  public void readFields(DataInput in) throws IOException
  {
    workunit.readFields(in);
    super.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException
  {
    workunit.write(out);
    super.write(out);
  }

}
