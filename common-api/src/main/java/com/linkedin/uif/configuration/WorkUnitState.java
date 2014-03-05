package com.linkedin.uif.configuration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import com.linkedin.uif.source.workunit.Extract;
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
  protected String getProperty(String key) {
    String propStr = super.getProperty(key);
    if (propStr != null)
      return propStr;
    else
      return workunit.getProperty(key);
  }
  
  @Override
  protected String getProperty(String key, String def) {
    String propStr = super.getProperty(key);
    if (propStr != null)
      return propStr;
    else
      return workunit.getProperty(key, def);
  }
  
  @Override
  public Set<String> getPropertyNames() {
    Set<String> set = super.getPropertyNames();
    set.addAll(workunit.getPropertyNames());
    return set;
  }
  
  @Override
  public boolean contains(String key) {
    return super.contains(key) || workunit.contains(key);
  }
  
  public Extract getExtract() {
    return workunit.getExtract();
  }
  
  public State getPreviousTableState() {
    return getExtract().getPreviousTableState();
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
