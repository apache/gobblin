/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.configuration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.source.extractor.Watermark;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.ImmutableWorkUnit;
import gobblin.source.workunit.WorkUnit;

import javax.annotation.Nullable;
import lombok.Getter;


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
    PENDING,
    RUNNING,
    SUCCESSFUL,
    COMMITTED,
    FAILED,
    CANCELLED,
    SKIPPED
  }

  private final WorkUnit workUnit;

  @Getter
  private State jobState;

  transient private final SharedResourcesBroker<GobblinScopeTypes> taskBroker;

  /**
   * Default constructor used for deserialization.
   */
  public WorkUnitState() {
    this.workUnit = WorkUnit.createEmpty();
    this.jobState = new State();
    // Not available on deserialization
    this.taskBroker = null;
  }

  /**
   * Constructor.
   *
   * @param workUnit a {@link WorkUnit} instance based on which a {@link WorkUnitState} instance is constructed
   * @deprecated It is recommended to use {@link #WorkUnitState(WorkUnit, State)} rather than combining properties
   * in the job state into the workunit.
   */
  @Deprecated
  public WorkUnitState(WorkUnit workUnit) {
    this.workUnit = workUnit;
    this.jobState = new State();
    this.taskBroker = null;
  }

  /**
   * If creating a {@link WorkUnitState} for use by a task, use {@link #WorkUnitState(WorkUnit, State, SharedResourcesBroker)}
   * instead.
   */
  public WorkUnitState(WorkUnit workUnit, State jobState) {
    this(workUnit, jobState, null);
  }

  public WorkUnitState(WorkUnit workUnit, State jobState, SharedResourcesBroker<GobblinScopeTypes> taskBroker) {
    this.workUnit = workUnit;
    this.jobState = jobState;
    this.taskBroker = taskBroker;
  }

  /**
   * Get a {@link SharedResourcesBroker} scoped for this task.
   */
  public SharedResourcesBroker<GobblinScopeTypes> getTaskBroker() {
    if (this.taskBroker == null) {
      throw new UnsupportedOperationException("Task broker is only available within a task. If this exception was thrown "
          + "from within a task, the JobLauncher did not specify a task broker.");
    }
    return this.taskBroker;
  }

  /**
   * Get a {@link SharedResourcesBroker} scoped for this task or null if it doesn't exist. This is used for internal calls.
   */
  @Nullable public SharedResourcesBroker<GobblinScopeTypes> getTaskBrokerNullable() {
    return this.taskBroker;
  }

  /**
   * Get an {@link ImmutableWorkUnit} that wraps the internal {@link WorkUnit}.
   *
   * @return an {@link ImmutableWorkUnit} that wraps the internal {@link WorkUnit}
   */
  public WorkUnit getWorkunit() {
    return new ImmutableWorkUnit(this.workUnit);
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
   * Get the actual high {@link Watermark}. If the {@code WorkUnitState} does not contain the actual high watermark
   * (which may be caused by task failures), the low watermark in the corresponding {@link WorkUnit} will be returned.
   *
   * @param watermarkClass the watermark class for this {@code WorkUnitState}.
   * @param gson a {@link Gson} object used to deserialize the watermark.
   * @return the actual high watermark in this {@code WorkUnitState}. null is returned if this {@code WorkUnitState}
   * does not contain an actual high watermark, and the corresponding {@code WorkUnit} does not contain a low
   * watermark.
   */
  public <T extends Watermark> T getActualHighWatermark(Class<T> watermarkClass, Gson gson) {
    JsonElement json = getActualHighWatermark();
    if (json == null) {
      json = this.workUnit.getLowWatermark();
      if (json == null) {
        return null;
      }
    }
    return gson.fromJson(json, watermarkClass);
  }

  /**
   * Get the actual high {@link Watermark}. If the {@code WorkUnitState} does not contain the actual high watermark
   * (which may be caused by task failures), the low watermark in the corresponding {@link WorkUnit} will be returned.
   *
   * <p>A default {@link Gson} object will be used to deserialize the watermark.</p>
   *
   * @param watermarkClass the watermark class for this {@code WorkUnitState}.
   * @return the actual high watermark in this {@code WorkUnitState}. null is returned if this {@code WorkUnitState}
   * does not contain an actual high watermark, and the corresponding {@code WorkUnit} does not contain a low
   * watermark.
   */
  public <T extends Watermark> T getActualHighWatermark(Class<T> watermarkClass) {
    return getActualHighWatermark(watermarkClass, GSON);
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
    JsonElement lowWatermark = this.workUnit.getLowWatermark();
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
    props.putAll(this.jobState.getProperties());
    props.putAll(this.workUnit.getProperties());
    props.putAll(super.getProperties());
    return props;
  }

  @Override
  public String getProp(String key) {
    String value = super.getProp(key);
    if (value == null) {
      value = this.workUnit.getProp(key);
    }
    if (value == null) {
      value = this.jobState.getProp(key);
    }
    return value;
  }

  @Override
  public String getProp(String key, String def) {
    String value = super.getProp(key);
    if (value == null) {
      value = this.workUnit.getProp(key);
    }
    if (value == null) {
      value = this.jobState.getProp(key, def);
    }
    return value;
  }

  /**
   * @deprecated Use {@link #getProp(String)}
   */
  @Deprecated
  @Override
  protected String getProperty(String key) {
    return getProp(key);
  }

  /**
   * @deprecated Use {@link #getProp(String, String)}
   */
  @Deprecated
  @Override
  protected String getProperty(String key, String def) {
    return getProp(key, def);
  }

  @Override
  public Set<String> getPropertyNames() {
    Set<String> set = Sets.newHashSet(super.getPropertyNames());
    set.addAll(this.workUnit.getPropertyNames());
    set.addAll(this.jobState.getPropertyNames());
    return set;
  }

  @Override
  public boolean contains(String key) {
    return super.contains(key) || this.workUnit.contains(key) || this.jobState.contains(key);
  }

  /**
   * Get the {@link gobblin.source.workunit.Extract} associated with the {@link WorkUnit}.
   *
   * @return {@link gobblin.source.workunit.Extract} associated with the {@link WorkUnit}
   */
  public Extract getExtract() {
    return new Extract(this.workUnit.getExtract());
  }

  /**
   * Get properties set in the previous run for the same table as the {@link WorkUnit}.
   *
   * @return properties as a {@link State} object
   */
  public State getPreviousTableState() {
    return getExtract().getPreviousTableState();
  }

  public void setJobState(State jobState) {
    this.jobState = jobState;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.workUnit.readFields(in);
    super.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    this.workUnit.write(out);
    super.write(out);
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof WorkUnitState)) {
      return false;
    }

    WorkUnitState other = (WorkUnitState) object;
    return ((this.workUnit == null && other.workUnit == null)
        || (this.workUnit != null && this.workUnit.equals(other.workUnit)))
        && ((this.jobState == null && other.jobState == null)
            || (this.jobState != null && this.jobState.equals(other.jobState)))
        && super.equals(other);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + (this.workUnit == null ? 0 : this.workUnit.hashCode());
    return result;
  }

  @Override
  public String toString() {
    return super.toString() + "\nWorkUnit: " + getWorkunit().toString() + "\nExtract: " + getExtract().toString()
        + "\nJobState: " + this.jobState.toString();
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
