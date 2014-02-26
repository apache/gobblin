package com.linkedin.uif.configuration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import com.linkedin.uif.source.workunit.Extract;
import com.linkedin.uif.source.workunit.Extract.TableType;
import com.linkedin.uif.source.workunit.WorkUnit;

public class SourceState extends State
{
  private List<WorkUnitState> previousTaskStates = new ArrayList<WorkUnitState>();
  private static SimpleDateFormat DTF = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US);

  public SourceState()
  {
  }

  public SourceState(State properties, List<WorkUnitState> previousTaskStates)
  {
    DTF.setTimeZone(TimeZone.getTimeZone("UTC"));
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
  
  /**
   * builder for Extract that correctly populates Extract from config if needed and
   * uses current date/time for extractId
   * @param type
   * @param namespace
   * @param table
   * @return
   */
  public Extract createExtract(TableType type, String namespace, String table)
  {
    return new Extract(this, type, namespace, table, DTF.format(new Date()));
  }

  /**
   * builder for Extract that correctly populates Extract from config if needed
   * @param type
   * @param namespace
   * @param table
   * @param extractId
   * @return
   */
  public Extract createExtract(TableType type, String namespace, String table, String extractId)
  {
    return new Extract(this, type, namespace, table, extractId);
  }

  /**
   * builder for WorkUnit that correctly populates WorkUnit from config if needed
   * @param extract
   * @return
   */
  public WorkUnit createWorkUnit(Extract extract){
    return new WorkUnit(this, extract);
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
