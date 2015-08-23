/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
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
import java.util.Properties;
import java.util.Set;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

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

  private static final String FINAL_CONSTRUCT_STATE_PREFIX = "construct.final.state.";

  private static final JsonParser JSON_PARSER = new JsonParser();

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
    PENDING,
    RUNNING,
    SUCCESSFUL,
    COMMITTED,
    FAILED,
    CANCELLED
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
   * @return a {@link JsonElement} representing the actual high {@link Watermark},
   *         or {@code null} if the actual  high {@link Watermark} is not set.
   */
  public JsonElement getActualHighWatermark() {
    if (!contains(ConfigurationKeys.WORK_UNIT_STATE_ACTUAL_HIGH_WATER_MARK_KEY)) {
      return null;
    }
    return JSON_PARSER.parse(getProp(ConfigurationKeys.WORK_UNIT_STATE_ACTUAL_HIGH_WATER_MARK_KEY));
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
    /**
     * TODO
     *
     * Hack until a state-store migration can be done. The watermark is converted to a {@link String} and then stored
     * internally in via a configuration key. Once a state-store migration can be done, the {@link Watermark} can be
     * stored as Binary JSON.
     */
    setProp(ConfigurationKeys.WORK_UNIT_STATE_ACTUAL_HIGH_WATER_MARK_KEY, watermark.toJson().toString());
  }

  /**
   * Backoff the actual high watermark to the low watermark returned by {@link WorkUnit#getLowWatermark()}.
   */
  public void backoffActualHighWatermark() {
    JsonElement lowWatermark = this.workunit.getLowWatermark();
    if (lowWatermark == null) {
      return;
    }
    setProp(ConfigurationKeys.WORK_UNIT_STATE_ACTUAL_HIGH_WATER_MARK_KEY, lowWatermark.toString());
  }

  /**
   * Get the high watermark as set in {@link gobblin.source.extractor.Extractor}.
   *
   * @return high watermark
   * @deprecated use {@link #getActualHighWatermark}.
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
   * @deprecated use {@link #setActualHighWatermark(Watermark)}.
   */
  @Deprecated
  public void setHighWaterMark(long value) {
    setProp(ConfigurationKeys.WORK_UNIT_STATE_RUNTIME_HIGH_WATER_MARK, value);
  }

  @Override
  public Properties getProperties() {
    Properties props = new Properties();
    props.putAll(this.workunit.getProperties());
    props.putAll(super.getProperties());
    return props;
  }

  @Override
  public String getProp(String key) {
    return getProperty(key);
  }

  @Override
  public String getProp(String key, String def) {
    return getProperty(key, def);
  }

  /**
   * @deprecated Use {@link #getProp(String)}
   */
  @Deprecated
  @Override
  protected String getProperty(String key) {
    String propStr = super.getProperty(key);
    if (propStr != null) {
      return propStr;
    } else {
      return workunit.getProperty(key);
    }
  }

  /**
   * @deprecated Use {@link #getProp(String, String)}
   */
  @Deprecated
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
  public void readFields(DataInput in) throws IOException {
    this.workunit.readFields(in);
    super.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    this.workunit.write(out);
    super.write(out);
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof WorkUnitState)) {
      return false;
    }

    WorkUnitState other = (WorkUnitState) object;
    return ((this.workunit == null && other.workunit == null)
        || (this.workunit != null && this.workunit.equals(other.workunit))) && super.equals(other);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + (this.workunit == null ? 0 : this.workunit.hashCode());
    return result;
  }

  @Override
  public String toString() {
    return super.toString() + "\nWorkUnit: " + getWorkunit().toString() + "\nExtract: " + getExtract().toString();
  }

  /**
   * Adds all properties from {@link gobblin.configuration.State} to this {@link gobblin.configuration.WorkUnitState}.
   *
   * <p>
   *   A property with name "property" will be added to this object with the key
   *   "{@link #FINAL_CONSTRUCT_STATE_PREFIX}[.<infix>].property"
   * </p>
   *
   * @param infix Optional infix used for the name of the property in the {@link gobblin.configuration.WorkUnitState}.
   * @param finalConstructState {@link gobblin.configuration.State} for which all properties should be added to this
   *                                                               object.
   */
  public void addFinalConstructState(String infix, State finalConstructState) {
    for (String property : finalConstructState.getPropertyNames()) {
      if (Strings.isNullOrEmpty(infix)) {
        setProp(FINAL_CONSTRUCT_STATE_PREFIX + property, finalConstructState.getProp(property));
      } else {
        setProp(FINAL_CONSTRUCT_STATE_PREFIX + infix + "." + property, finalConstructState.getProp(property));
      }
    }
  }

  /**
   * Builds a State containing all properties added with {@link #addFinalConstructState}
   * to this {@link gobblin.configuration.WorkUnitState}. All such properties will be stripped of
   * {@link #FINAL_CONSTRUCT_STATE_PREFIX} but not of any infixes.
   *
   * <p>
   *   For example, if state={sample.property: sampleValue}
   *   then
   *   <pre>
   *     {@code
   *        this.addFinalConstructState("infix",state);
   *        this.getFinalConstructState();
   *      }
   *   </pre>
   *   will return state={infix.sample.property: sampleValue}
   * </p>
   *
   * @return State containing all properties added with {@link #addFinalConstructState}.
   */
  public State getFinalConstructStates() {
    State constructState = new State();
    for (String property : getPropertyNames()) {
      if (property.startsWith(FINAL_CONSTRUCT_STATE_PREFIX)) {
        constructState.setProp(property.substring(FINAL_CONSTRUCT_STATE_PREFIX.length()), getProp(property));
      }
    }
    return constructState;
  }
}
