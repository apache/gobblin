package com.linkedin.uif.configuration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

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

  private WorkingState state = WorkingState.PENDING;
  private long lowWaterMark = -1;
  private long highWaterMark = -1;

  private WorkUnit workunit;

    // Necessary for serialization/deserialization
  public WorkUnitState() {
      this.workunit = new WorkUnit();
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
    return state;
  }

  public void setWorkingState(WorkingState state)
  {
    this.state = state;
  }

  public long getHighWaterMark()
  {
    return highWaterMark;
  }

  public void setHighWaterMark(long highWaterMark)
  {
    this.highWaterMark = highWaterMark;
  }

  public long getLowWaterMark()
  {
    return this.lowWaterMark;
  }

  public void setLowWaterMark(long lowWaterMark)
  {
    this.lowWaterMark = lowWaterMark;
  }

  @Override
  public void readFields(DataInput in) throws IOException
  {
    Text txt = new Text();
    txt.readFields(in);
    state = WorkingState.valueOf(txt.toString());

    this.lowWaterMark = in.readLong();
    highWaterMark = in.readLong();

    workunit.readFields(in);
    super.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException
  {
    Text txt = new Text(state.toString());
    txt.write(out);

    out.writeLong(this.lowWaterMark);
    out.writeLong(highWaterMark);

    workunit.write(out);
    super.write(out);
  }

}
