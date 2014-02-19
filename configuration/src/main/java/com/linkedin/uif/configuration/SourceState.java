package com.linkedin.uif.configuration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.linkedin.uif.scheduler.TaskState;

public class SourceState extends State
{
  private List<WorkUnitState> previousTaskStates = new ArrayList<WorkUnitState>();

  public SourceState()
  {
  }

  public SourceState(State properties, List<WorkUnitState> previousTaskStates)
  {
    addAll(properties);
    this.previousTaskStates.addAll(previousTaskStates);
  }

  public List<WorkUnitState> getPreviousStates()
  {
    return previousTaskStates;
  }

  public MetaStoreClient buildMetaStoreClient(State state) throws Exception {
      MetaStoreClientBuilder builder = new MetaStoreClientBuilderFactory().newMetaStoreClientBuilder(state);
      return builder.build();
  }
  
  @Override
  public void write(DataOutput out) throws IOException
  {
    out.writeInt(previousTaskStates.size());

    for (WorkUnitState state : previousTaskStates)
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
      WorkUnitState state = new WorkUnitState();
      state.readFields(in);

      previousTaskStates.add(state);
    }
    super.readFields(in);
  }

}
