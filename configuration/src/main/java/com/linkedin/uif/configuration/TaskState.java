package com.linkedin.uif.configuration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.linkedin.uif.source.workunit.WorkUnit;

public class TaskState extends State
{
  public enum WorkingState
  {
    PENDING, WORKING, FAILED, FAILED_EXTRACT, FAILED_WRITE, FAILED_QA_CHECK, FAILED_COMMIT, COMMITTED
  }

  private WorkingState state = WorkingState.PENDING;
  private long highWaterMark = -1;

  private WorkUnit workunit;

  public WorkUnit getWorkunit()
  {
    return workunit;
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

  @Override
  public void readFields(DataInput in) throws IOException
  {
    Text txt = new Text();
    txt.readFields(in);
    state = WorkingState.valueOf(txt.toString());

    highWaterMark = in.readLong();

    workunit.readFields(in);
    super.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException
  {
    Text txt = new Text(state.toString());
    txt.write(out);

    out.writeLong(highWaterMark);

    workunit.write(out);
    super.write(out);
  }

}
