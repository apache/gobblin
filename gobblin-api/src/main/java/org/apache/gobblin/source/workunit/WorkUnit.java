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

package org.apache.gobblin.source.workunit;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringWriter;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonWriter;

import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.Watermark;
import org.apache.gobblin.source.extractor.WatermarkInterval;
import lombok.ToString;


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
@ToString(callSuper=true)
public class WorkUnit extends State {

  private Extract extract;

  private static final JsonParser JSON_PARSER = new JsonParser();

  private static final Gson GSON = new Gson();

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
   * @param state a {@link org.apache.gobblin.configuration.SourceState} the properties of which will be copied into this {@link WorkUnit} instance.
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
   * Factory method.
   *
   * @return An empty {@link WorkUnit}.
   */
  public static WorkUnit createEmpty() {
    return new WorkUnit();
  }

  /**
   * Factory method.
   *
   * @param extract {@link Extract}
   * @return A {@link WorkUnit} with the given {@link Extract}
   */
  public static WorkUnit create(Extract extract) {
    return new WorkUnit(null, extract);
  }

  /**
   * Factory method.
   *
   * @param extract {@link Extract}
   * @param watermarkInterval {@link WatermarkInterval}
   * @return A {@link WorkUnit} with the given {@link Extract} and {@link WatermarkInterval}
   */
  public static WorkUnit create(Extract extract, WatermarkInterval watermarkInterval) {
    return new WorkUnit(null, extract, watermarkInterval);
  }

  /**
   * Factory method.
   *
   * @param other a {@link WorkUnit} instance
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
   * This method will allow a work unit to be skipped if needed.
   */
  public void skip() {
    this.setProp(ConfigurationKeys.WORK_UNIT_SKIP_KEY, true);
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
   * Get the low {@link Watermark}.
   *
   * @param watermarkClass the watermark class for this {@code WorkUnit}.
   * @param gson a {@link Gson} object used to deserialize the watermark.
   * @return the low watermark in this {@code WorkUnit}.
   */
  public <T extends Watermark> T getLowWatermark(Class<T> watermarkClass, Gson gson) {
    JsonElement json = getLowWatermark();
    if (json == null) {
      return null;
    }
    return gson.fromJson(json, watermarkClass);
  }

  /**
   * Get the low {@link Watermark}. A default {@link Gson} object will be used to deserialize the watermark.
   *
   * @param watermarkClass the watermark class for this {@code WorkUnit}.
   * @return the low watermark in this {@code WorkUnit}.
   */
  public <T extends Watermark> T getLowWatermark(Class<T> watermarkClass) {
    return getLowWatermark(watermarkClass, GSON);
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
   * Get the expected high {@link Watermark}.
   *
   * @param watermarkClass the watermark class for this {@code WorkUnit}.
   * @param gson a {@link Gson} object used to deserialize the watermark.
   * @return the expected high watermark in this {@code WorkUnit}.
   */
  public <T extends Watermark> T getExpectedHighWatermark(Class<T> watermarkClass, Gson gson) {
    JsonElement json = getExpectedHighWatermark();
    if (json == null) {
      return null;
    }
    return gson.fromJson(json, watermarkClass);
  }

  /**
   * Get the expected high {@link Watermark}. A default {@link Gson} object will be used to deserialize the watermark.
   *
   * @param watermarkClass the watermark class for this {@code WorkUnit}.
   * @return the expected high watermark in this {@code WorkUnit}.
   */
  public <T extends Watermark> T getExpectedHighWatermark(Class<T> watermarkClass) {
    return getExpectedHighWatermark(watermarkClass, GSON);
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

  @Override
  public boolean contains(String key) {
    return super.contains(key) || this.extract.contains(key);
  }

  @Override
  public String getProp(String key) {
    return getProp(key, null);
  }

  @Override
  public String getProp(String key, String def) {
    String value = super.getProp(key);
    if (value == null) {
      value = this.extract.getProp(key, def);
    }
    return value;
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

  /** @return Stringified form, in pretty-printed JSON */
  public String toJsonString() {
    StringWriter stringWriter = new StringWriter();
    try (JsonWriter jsonWriter = new JsonWriter(stringWriter)) {
      jsonWriter.setIndent("\t");
      this.toJson(jsonWriter);
    } catch (IOException ioe) {
      // Ignored
    }
    return stringWriter.toString();
  }

  public void toJson(JsonWriter jsonWriter) throws IOException {
    jsonWriter.beginObject();

    jsonWriter.name("id").value(this.getId());
    jsonWriter.name("properties");
    jsonWriter.beginObject();
    for (String key : this.getPropertyNames()) {
      jsonWriter.name(key).value(this.getProp(key));
    }
    jsonWriter.endObject();

    jsonWriter.name("extract");
    jsonWriter.beginObject();
    jsonWriter.name("extractId").value(this.getExtract().getId());
    jsonWriter.name("extractProperties");
    jsonWriter.beginObject();
    for (String key : this.getExtract().getPropertyNames()) {
      jsonWriter.name(key).value(this.getExtract().getProp(key));
    }
    jsonWriter.endObject();

    State prevTableState = this.getExtract().getPreviousTableState();
    if (prevTableState != null) {
      jsonWriter.name("extractPrevTableState");
      jsonWriter.beginObject();
      jsonWriter.name("prevStateId").value(prevTableState.getId());
      jsonWriter.name("prevStateProperties");
      jsonWriter.beginObject();
      for (String key : prevTableState.getPropertyNames()) {
        jsonWriter.name(key).value(prevTableState.getProp(key));
      }
      jsonWriter.endObject();
      jsonWriter.endObject();
    }
    jsonWriter.endObject();

    jsonWriter.endObject();
  }

  public String getOutputFilePath() {
    // Search for the properties in the workunit.
    // This search for the property first in State and then in the Extract of this workunit.
    String namespace = getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY, "");
    String table = getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, "");
    String extractId = getProp(ConfigurationKeys.EXTRACT_EXTRACT_ID_KEY, "");
    // getPropAsBoolean and other similar methods are not overridden in WorkUnit class
    // Thus, to enable searching in WorkUnit's Extract, we use getProp, and not getPropAsBoolean
    boolean isFull =  Boolean.parseBoolean(getProp(ConfigurationKeys.EXTRACT_IS_FULL_KEY));

    return namespace.replaceAll("\\.", "/") + "/" + table + "/" + extractId + "_"
        + (isFull ? "full" : "append");
  }
}
