package com.linkedin.uif.configuration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SourceState extends State
{
  private List<TaskState> previousTaskStates = new ArrayList<TaskState>();

  public SourceState()
  {
  }

  public SourceState(State properties, List<TaskState> previousTaskStates)
  {
    addAll(properties);
    this.previousTaskStates.addAll(previousTaskStates);
  }

  public List<TaskState> getPreviousStates()
  {
    return previousTaskStates;
  }

  @Override
  public void write(DataOutput out) throws IOException
  {
    out.writeInt(previousTaskStates.size());

    for (TaskState state : previousTaskStates)
    {
      state.write(out);
    }
    super.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException
  {
    int size = in.readInt();

    for (int i = 0; i < size; i++)
    {
      TaskState state = new TaskState();
      state.readFields(in);

      previousTaskStates.add(state);
    }
    super.readFields(in);
  }

}
