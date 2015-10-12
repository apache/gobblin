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

package gobblin.source.workunit;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.Watermark;
import gobblin.source.extractor.WatermarkInterval;


/**
 * A logic concept that defines a unit of work or task for extracting a portion of the data
 * to be pulled in a job run.
 * <p>
 *   An instance of this class should contain all the properties an {@link Extractor} needs
 *   to extract the schema and data records.
 * </p>
 *
 * @author kgoodhop
 */
public class WorkUnit extends State {

  private Extract extract;

  private static final JsonParser JSON_PARSER = new JsonParser();

  /**
   * Default constructor.
   *
   * @deprecated Use {@link #createEmpty()}
   */
  @Deprecated
  public WorkUnit() {
    this(null, null);
  }

  /**
   * Constructor.
   *
   * @param state a {@link SourceState} the properties of which will be copied into this {@link WorkUnit} instance
   * @param extract an {@link Extract}
   *
   * @deprecated Properties in {@link SourceState} should not be added to a {@link WorkUnit}. Having each 
   * {@link WorkUnit} contain a copy of {@link SourceState} is a waste of memory. Use {@link #create(Extract)}.
   */
  @Deprecated
  public WorkUnit(SourceState state, Extract extract) {
    // Values should only be null for deserialization
    if (state != null) {
      super.addAll(state);
    }

    if (extract != null) {
      this.extract = extract;
    } else {
      this.extract = new Extract(null, null, null, null);
    }
  }

  /**
   * Constructor for a {@link WorkUnit} given a {@link SourceState}, {@link Extract}, and a {@link WatermarkInterval}.
   *
   * @param state a {@link gobblin.configuration.SourceState} the properties of which will be copied into this {@link WorkUnit} instance.
   * @param extract an {@link Extract}.
   * @param watermarkInterval a {@link WatermarkInterval} which defines the range of data this {@link WorkUnit} will process.
   *
   * @deprecated Properties in {@link SourceState} should not be added to a {@link WorkUnit}. Having each 
   * {@link WorkUnit} contain a copy of {@link SourceState} is a waste of memory. Use {@link #create(Extract, WatermarkInterval)}.
   */
  @Deprecated
  public WorkUnit(SourceState state, Extract extract, WatermarkInterval watermarkInterval) {
    this(state, extract);

    /**
     * TODO
     *
     * Hack that stores a {@link WatermarkInterval} by using its {@link WatermarkInterval#toJson()} method. Until a
     * state-store migration, or a new state-store format is chosen, this hack will be the way that the
     * {@link WatermarkInterval} is serialized / de-serialized. Once a state-store migration can be done, the
     * {@link Watermark} can be stored as Binary JSON.
     */
    setProp(ConfigurationKeys.WATERMARK_INTERVAL_VALUE_KEY, watermarkInterval.toJson().toString());
  }

  /**
   * Constructor.
   *
   * @param extract a {@link Extract} object
   */
  public WorkUnit(Extract extract) {
    this.extract = extract;
  }

  /**
   * Copy constructor.
   *
   * @param other the other {@link WorkUnit} instance
   *
   * @deprecated Use {@link #copyOf(WorkUnit)}
   */
  @Deprecated
  public WorkUnit(WorkUnit other) {
    super.addAll(other);
    this.extract = other.getExtract();
  }

  /**
   * Facotry method.
   *
   * @return An empty {@link WorkUnit}.
   */
  public static WorkUnit createEmpty() {
    return new WorkUnit();
  }

  /**
   * Facotry method.
   *
   * @param extract {@link Extract}
   * @return A {@link WorkUnit} with the given {@link Extract}
   */
  public static WorkUnit create(Extract extract) {
    return new WorkUnit(null, extract);
  }

  /**
   * Facotry method.
   *
   * @param extract {@link Extract}
   * @param watermarkInterval {@link WatermarkInterval}
   * @return A {@link WorkUnit} with the given {@link Extract} and {@link WatermarkInterval}
   */
  public static WorkUnit create(Extract extract, WatermarkInterval watermarkInterval) {
    return new WorkUnit(null, extract, watermarkInterval);
  }

  /**
   * Facotry method.
   *
   * @param workUnit a {@link WorkUnit} instance
   * @return A copy of the given {@link WorkUnit} instance
   */
  public static WorkUnit copyOf(WorkUnit other) {
    return new WorkUnit(other);
  }

  /**
   * Get the {@link Extract} associated with this {@link WorkUnit}.
   *
   * @return the {@link Extract} associated with this {@link WorkUnit}
   */
  public Extract getExtract() {
    return new ImmutableExtract(this.extract);
  }

  /**
   * Get the low {@link Watermark} as a {@link JsonElement}.
   *
   * @return a {@link JsonElement} representing the low {@link Watermark} or
   *         {@code null} if the low {@link Watermark} is not set.
   */
  public JsonElement getLowWatermark() {
    if (!contains(ConfigurationKeys.WATERMARK_INTERVAL_VALUE_KEY)) {
      return null;
    }
    return JSON_PARSER.parse(getProp(ConfigurationKeys.WATERMARK_INTERVAL_VALUE_KEY)).getAsJsonObject()
        .get(WatermarkInterval.LOW_WATERMARK_TO_JSON_KEY);
  }

  /**
   * Get the expected high {@link Watermark} as a {@link JsonElement}.
   *
   * @return a {@link JsonElement} representing the expected high {@link Watermark}.
   */
  public JsonElement getExpectedHighWatermark() {
    return JSON_PARSER.parse(getProp(ConfigurationKeys.WATERMARK_INTERVAL_VALUE_KEY)).getAsJsonObject()
        .get(WatermarkInterval.EXPECTED_HIGH_WATERMARK_TO_JSON_KEY);
  }

  /**
   * Get the high watermark of this {@link WorkUnit}.
   *
   * @return high watermark
   * @deprecated use the {@link #getExpectedHighWatermark()} method.
   */
  @Deprecated
  public long getHighWaterMark() {
    return getPropAsLong(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY);
  }

  /**
   * Set {@link WatermarkInterval} for a {@link WorkUnit}.
   */
  public void setWatermarkInterval(WatermarkInterval watermarkInterval) {
    setProp(ConfigurationKeys.WATERMARK_INTERVAL_VALUE_KEY, watermarkInterval.toJson().toString());
  }

  /**
   * Set the high watermark of this {@link WorkUnit}.
   *
   * @param highWaterMark high watermark
   * @deprecated use {@link #setWatermarkInterval(WatermarkInterval)}.
   */
  @Deprecated
  public void setHighWaterMark(long highWaterMark) {
    setProp(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY, highWaterMark);
  }

  /**
   * Get the low watermark of this {@link WorkUnit}.
   *
   * @return low watermark
   * @deprecated use the {@link #getLowWatermark()} method.
   */
  @Deprecated
  public long getLowWaterMark() {
    return getPropAsLong(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY);
  }

  /**
   * Set the low watermark of this {@link WorkUnit}.
   *
   * @param lowWaterMark low watermark
   * @deprecated use {@link #setWatermarkInterval(WatermarkInterval)}.
   */
  @Deprecated
  public void setLowWaterMark(long lowWaterMark) {
    setProp(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY, lowWaterMark);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.extract.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    this.extract.write(out);
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof WorkUnit)) {
      return false;
    }

    WorkUnit other = (WorkUnit) object;
    return ((this.extract == null && other.extract == null)
        || (this.extract != null && this.extract.equals(other.extract))) && super.equals(other);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((this.extract == null) ? 0 : this.extract.hashCode());
    return result;
  }
}
