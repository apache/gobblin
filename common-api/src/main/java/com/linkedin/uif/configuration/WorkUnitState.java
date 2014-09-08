package com.linkedin.uif.configuration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import com.linkedin.uif.source.workunit.Extract;
import com.linkedin.uif.source.workunit.ImmutableWorkUnit;
import com.linkedin.uif.source.workunit.WorkUnit;

/**
 * <p> Holds all the task runtime state for a single WorkUnit.  WorkUnit properties can be 
 * overridden at runtime, with the original values available through the {@link #getWorkunit()} method.
 * Getters will return properties set at task runtime if available, or the corresponding values from {@link WorkUnit}
 * if they are not set at task runtime.
 * </p>
 * <p> All WorkUnitStates are collected at job completion, populated to {@link SourceState}, and made available through
 * {@link SourceState#getPreviousStates()}.
 * </p>
 * 
 * @author kgoodhop
 *
 */
public class WorkUnitState extends State
{
  /**
   * <p>Runtime state for this {@link WorkUnit}.  The final state is COMMITTED.
   * SUCCESSFUL only implies a task has finished, but doesn't imply the work
   * has been committed.
   * </p>
   * @author ynli
   *
   */
  public enum WorkingState {
    PENDING, RUNNING, SUCCESSFUL, COMMITTED, FAILED, CANCELLED
  }

  private WorkUnit workunit;

  /**
   * Default, should only be used by the framework for SerDe
   */
  public WorkUnitState() {
    this.workunit = new WorkUnit(null, null);
  }

  /**
   * Constructor used by the framework to encapsulate task runtime state for every
   * {@link WorkUnit}
   * @param workUnit
   */
  public WorkUnitState(WorkUnit workUnit) {
      this.workunit = workUnit;
  }

  /**
   * <p>Returns an {@link ImmutableWorkUnit}, useful for checking pre-runtime properties.</p>
   * @return
   */
  public WorkUnit getWorkunit()
  {
    return new ImmutableWorkUnit(workunit);
  }
  
  /**
   * @see State#addAll(State)
   * @param otherState
   */
  public void addAll(WorkUnitState otherState) {
    super.addAll(otherState);
    this.workunit = otherState.workunit;
  }

  /**
   * Returns current runtime state
   * @return {@link WorkingState}
   */
  public WorkingState getWorkingState()
  {
    return WorkingState.valueOf(getProp(ConfigurationKeys.WORK_UNIT_WORKING_STATE_KEY, WorkingState.PENDING.toString()));
  }

  /**
   * Setter for runtime state
   * @param state
   */
  public void setWorkingState(WorkingState state)
  {
    setProp(ConfigurationKeys.WORK_UNIT_WORKING_STATE_KEY, state.toString());
  }

  /**
   * Returns high water mark as set by {@link Extractor}
   * @return
   */
  public long getHighWaterMark()
  {
    return getPropAsLong(ConfigurationKeys.WORK_UNIT_STATE_RUNTIME_HIGH_WATER_MARK, ConfigurationKeys.DEFAULT_WATERMARK_VALUE);
  }

  /**
   * Used by {@link Extractor} to set high water mark
   * @param value
   */
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
  
  /**
   * Getter for the {@link Extract} instance for the {@link WorkUnit}
   * @return
   */
  public Extract getExtract() {
    Extract curExtract = new Extract(workunit.getExtract());
    curExtract.addAll(this);
    return curExtract;
  }
  
  /**
   * Getter for properties set in the previous run for the same table as this {@link WorkUnit}
   * @return
   */
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
  
  @Override
  public String toString() {
    return super.toString() + "\nWorkUnit: " + getWorkunit().toString() + "\nExtract: " + getExtract().toString();
  }

}
