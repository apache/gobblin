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

package gobblin.source.workunit;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import com.google.common.base.Charsets;
import com.google.common.io.Closer;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

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
  private WatermarkInterval watermarkInterval;

  private static final Gson GSON = new Gson();

  /**
   * Default constructor.
   */
  public WorkUnit() {
    this(null, null);
  }

  /**
   * Constructor.
   *
   * @param state a {@link gobblin.configuration.SourceState} the properties of which will be copied into this {@link WorkUnit} instance
   * @param extract an {@link Extract}
   */
  public WorkUnit(SourceState state, Extract extract) {
    // Values should only be null for deserialization
    if (state != null) {
      this.addAll(state);
    }

    if (extract != null) {
      this.extract = extract;
    } else {
      this.extract = new Extract(null, null, null, null);
    }
  }

  public WorkUnit(SourceState state, Extract extract, WatermarkInterval watermarkInterval) {
    this(state, extract);
    this.watermarkInterval = watermarkInterval;
  }

  /**
   * Copy constructor.
   *
   * @param other the other {@link WorkUnit} instance
   */
  public WorkUnit(WorkUnit other) {
    addAll(other);
    this.extract = other.getExtract();
  }

  /**
   * Get the {@link Extract} associated with this {@link WorkUnit}.
   *
   * @return the {@link Extract} associated with this {@link WorkUnit}
   */
  public Extract getExtract() {
    return this.extract;
  }

  public Watermark getExpectedHighWatermark() {
    return this.watermarkInterval.getExpectedHighWatermark();
  }

  public Watermark getLowWatermark() {
    return this.watermarkInterval.getLowWatermark();
  }

  /**
   * Get the high watermark of this {@link WorkUnit}.
   *
   * @return high watermark
   */
  @Deprecated
  public long getHighWaterMark() {
    return getPropAsLong(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY);
  }

  /**
   * Set the high watermark of this {@link WorkUnit}.
   *
   * @param highWaterMark high watermark
   */
  @Deprecated
  public void setHighWaterMark(long highWaterMark) {
    setProp(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY, highWaterMark);
  }

  /**
   * Get the low watermark of this {@link WorkUnit}.
   *
   * @return low watermark
   */
  @Deprecated
  public long getLowWaterMark() {
    return getPropAsLong(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY);
  }

  /**
   * Set the low watermark of this {@link WorkUnit}.
   *
   * @param lowWaterMark low watermark
   */
  @Deprecated
  public void setLowWaterMark(long lowWaterMark) {
    setProp(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY, lowWaterMark);
  }

  @Override
  public void readFields(DataInput in)
      throws IOException {
    super.readFields(in);
    this.extract.readFields(in);

    /**
     * TODO
     *
     * Hack that constructs a {@link WatermarkInterval} by using its {@link WatermarkInterval#fromJson(JsonElement)}
     * method. Until a state-store migration, or a new state-store format is chosen, this hack will be the way that
     * the {@link WatermarkInterval} is serialized / de-serialized. Also, see comments in
     * {@link WorkUnitState#readFields(DataInput)}.
     */
    if (contains(ConfigurationKeys.WATERMARK_INTERVAL_VALUE_KEY)) {
      this.watermarkInterval = new WatermarkInterval();
      this.watermarkInterval.fromJson(GSON.fromJson(getProp(ConfigurationKeys.WATERMARK_INTERVAL_VALUE_KEY), JsonElement.class));
    }
  }

  @Override
  public void write(DataOutput out)
      throws IOException {

    /**
     * TODO
     *
     * See comments inside {@link #readFields(DataInput)}.
     */
    setProp(ConfigurationKeys.WATERMARK_INTERVAL_VALUE_KEY, this.watermarkInterval.toJson());

    super.write(out);
    this.extract.write(out);
  }
}
