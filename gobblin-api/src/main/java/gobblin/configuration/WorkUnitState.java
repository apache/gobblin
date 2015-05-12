/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.configuration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import gobblin.source.extractor.Watermark;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.ImmutableWorkUnit;
import gobblin.source.workunit.WorkUnit;


/**
 * This class encapsulates a {@link WorkUnit} instance and additionally holds all the
 * task runtime state of that {@link WorkUnit}.
 *
 * <p>
 *   Properties set in the encapsulated {@link WorkUnit} can be overridden at runtime,
 *   with the original values available through the {@link #getWorkunit()} method.
 *   Getters will return values set at task runtime if available, or the corresponding
 *   values from encapsulated {@link WorkUnit} if they are not set at task runtime.
 * </p>
 *
 * @author kgoodhop
 */
public class WorkUnitState extends State {

  private Watermark actualHighWatermark;

  private static final Gson GSON = new Gson();

  /**
   * Runtime state of the {@link WorkUnit}.
   *
   * <p>
   *   The final state indicating successfully completed work is COMMITTED.
   *   SUCCESSFUL only implies a task has finished, but doesn't imply the work
   *   has been committed.
   * </p>
   */
  public enum WorkingState {
    PENDING, RUNNING, SUCCESSFUL, COMMITTED, FAILED, CANCELLED
  }

  private WorkUnit workunit;

  /**
   * Default constructor used for deserialization.
   */
  public WorkUnitState() {
    this.workunit = new WorkUnit(null, null);
  }

  /**
   * Constructor.
   *
   * @param workUnit a {@link WorkUnit} instance based on which a {@link WorkUnitState} instance is constructed
   */
  public WorkUnitState(WorkUnit workUnit) {
    this.workunit = workUnit;
  }

  /**
   * Get an {@link ImmutableWorkUnit} that wraps the internal {@link WorkUnit}.
   *
   * @return an {@link ImmutableWorkUnit} that wraps the internal {@link WorkUnit}
   */
  public WorkUnit getWorkunit() {
    return new ImmutableWorkUnit(workunit);
  }

  /**
   * Get the current runtime state of the {@link WorkUnit}.
   *
   * @return {@link WorkingState} of the {@link WorkUnit}
   */
  public WorkingState getWorkingState() {
    return WorkingState
        .valueOf(getProp(ConfigurationKeys.WORK_UNIT_WORKING_STATE_KEY, WorkingState.PENDING.toString()));
  }

  /**
   * Set the current runtime state of the {@link WorkUnit}.
   *
   * @param state {@link WorkingState} of the {@link WorkUnit}
   */
  public void setWorkingState(WorkingState state) {
    setProp(ConfigurationKeys.WORK_UNIT_WORKING_STATE_KEY, state.toString());
  }

  /**
   * Get the actual high {@link Watermark} as a {@link JsonElement}.
   *
   * @return a {@link JsonElement} representing the actual high {@link Watermark}.
   */
  public JsonElement getActualHighWatermark() {
    return GSON.toJsonTree(getProp(ConfigurationKeys.WORK_UNIT_STATE_COMPLEX_ACTUAL_HIGH_WATER_MARK_KEY));
  }

  /**
   * This method should set the actual, runtime high {@link Watermark} for this {@link WorkUnitState}. A high
   * {@link Watermark} indicates that all data for the source has been pulled up to a specific point.
   *
   * <p>
   *  This method should be called inside the {@link gobblin.source.extractor.Extractor} class, during the initialization
   *  of the class, before any calls to {@link gobblin.source.extractor.Extractor#readRecord(Object)} are executed. This
   *  method keeps a local point to the given {@link Watermark} and expects the following invariant to always be upheld.
   *  The invariant for this {@link Watermark} is that it should cover all records up to and including the most recent
   *  record returned by {@link gobblin.source.extractor.Extractor#readRecord(Object)}.
   * </p>

   * <p>
   *  The {@link Watermark} set in this method may be polled by the framework multiple times, in order to track the
   *  progress of how the {@link Watermark} changes. This is important for reporting percent completion of a
   *  {@link gobblin.source.workunit.WorkUnit}.
   * </p>
   *
   * TODO - Once we are ready to make a backwards incompatible change to the {@link gobblin.source.extractor.Extractor}
   * interface, this method should become part of the {@link gobblin.source.extractor.Extractor} interface. For example,
   * a method such as getCurrentHighWatermark() should be added.
   */
  public void setActualHighWatermark(Watermark watermark) {
    this.actualHighWatermark = watermark;

    /**
     * TODO
     *
     * Hack until a state-store migration can be done. The watermark is converted to a {@link String} and then stored
     * internally in via a configuration key. Once a state-store migration can be done, the {@link Watermark} can be
     * stored as Binary JSON.
     */
    setProp(ConfigurationKeys.WORK_UNIT_STATE_COMPLEX_ACTUAL_HIGH_WATER_MARK_KEY, this.actualHighWatermark.toJson());
  }

  /**
   * Get the high watermark as set in {@link gobblin.source.extractor.Extractor}.
   *
   * @return high watermark
   */
  @Deprecated
  public long getHighWaterMark() {
    return getPropAsLong(ConfigurationKeys.WORK_UNIT_STATE_RUNTIME_HIGH_WATER_MARK,
        ConfigurationKeys.DEFAULT_WATERMARK_VALUE);
  }

  /**
   * Set the high watermark.
   *
   * @param value high watermark
   */
  @Deprecated
  public void setHighWaterMark(long value) {
    setProp(ConfigurationKeys.WORK_UNIT_STATE_RUNTIME_HIGH_WATER_MARK, value);
  }

  @Override
  protected String getProperty(String key) {
    String propStr = super.getProperty(key);
    if (propStr != null) {
      return propStr;
    } else {
      return workunit.getProperty(key);
    }
  }

  @Override
  protected String getProperty(String key, String def) {
    String propStr = super.getProperty(key);
    if (propStr != null) {
      return propStr;
    } else {
      return workunit.getProperty(key, def);
    }
  }

  @Override
  public Set<String> getPropertyNames() {
    Set<String> set = Sets.newHashSet(super.getPropertyNames());
    set.addAll(workunit.getPropertyNames());
    return set;
  }

  @Override
  public boolean contains(String key) {
    return super.contains(key) || workunit.contains(key);
  }

  /**
   * Get the {@link gobblin.source.workunit.Extract} associated with the {@link WorkUnit}.
   *
   * @return {@link gobblin.source.workunit.Extract} associated with the {@link WorkUnit}
   */
  public Extract getExtract() {
    Extract curExtract = new Extract(workunit.getExtract());
    curExtract.addAll(this);
    return curExtract;
  }

  /**
   * Get properties set in the previous run for the same table as the {@link WorkUnit}.
   *
   * @return properties as a {@link State} object
   */
  public State getPreviousTableState() {
    return getExtract().getPreviousTableState();
  }

  @Override
  public void readFields(DataInput in)
      throws IOException {
    this.workunit.readFields(in);
    super.readFields(in);
  }

  @Override
  public void write(DataOutput out)
      throws IOException {
    this.workunit.write(out);
    super.write(out);
  }

  @Override
  public String toString() {
    return super.toString() + "\nWorkUnit: " + getWorkunit().toString() + "\nExtract: " + getExtract().toString();
  }
}
