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

package org.apache.gobblin.multistage.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.util.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

/**
 * This class encapsulates Watermark definitions, and provide function to manage
 * features generate  milli-seconds or date time ranges
 *
 * @author chrli
 */
@Slf4j
@Getter
@Setter
public class WatermarkDefinition {
  final private static Gson GSON = new Gson();
  final private static String DEFAULT_TIMEZONE = "America/Los_Angeles";
  final private static String KEY_WORD_NAME = "name";
  final private static String KEY_WORD_RANGE = "range";
  final private static String KEY_WORD_RANGE_FROM = "from";
  final private static String KEY_WORD_RANGE_TO = "to";
  final private static String KEY_WORD_TYPE = "type";
  final private static String KEY_WORD_UNITS = "units";

  public enum WatermarkTypes {
    DATETIME("datetime"),
    UNIT("unit");

    private final String name;

    WatermarkTypes(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  private String name;
  private WatermarkTypes type;
  private Pair<String, String> range;
  private Boolean isPartialPartition = false;
  private WorkUnitPartitionTypes workUnitPartitionType = null;

  // units is the internal storage of work units string, it should be
  // a JsonArray formatted as String
  private String units;
  private String timezone = "";

  /**
   * A constructor that creates a Unit watermark definition
   * <p>
   * A Unit watermark has a list of String values
   * @param name the name or the watermark
   * @param units the units in a JsonArray
   */
  public WatermarkDefinition(String name, JsonArray units) {
    this.setName(name);
    this.setType(WatermarkTypes.UNIT);
    this.setUnits(units.toString());
  }

  /**
   * A constructor that creates a Unit watermark definition from a units string
   *
   * <p>
   * A Unit watermark has a list of name : value pairs coded as a JsonArray of JsonObjects
   * @param name the name or the watermark
   * @param commaSeparatedUnits the comma separated units, either a JsonArray or a simple String list
   */
  public WatermarkDefinition(String name, String commaSeparatedUnits) {
    setUnits(name, commaSeparatedUnits);
  }

  /**
   * If the string is JsonArray, it will be stored as is.
   * Otherwise, the string is broken down as a list of values. And then the values
   * will be combined with the unit watermark name as name : value pairs.
   * @param name the name or the watermark
   * @param commaSeparatedUnits the comma separated units, either a JsonArray or a simple String list
   * @return the watermark definition object
   */
  public WatermarkDefinition setUnits(String name, String commaSeparatedUnits) {
    boolean isJsonArrayUnits = true;
    this.setName(name);
    this.setType(WatermarkTypes.UNIT);
    try {
        GSON.fromJson(commaSeparatedUnits, JsonArray.class);
    } catch (Exception e) {
      log.info("Assuming simple Unit Watermark definition as the unit watermark cannot be converted to JsonArray");
      log.info("Origin unit watermark definition: {} : {}", name, commaSeparatedUnits);
      isJsonArrayUnits = false;
    }

    if (isJsonArrayUnits) {
      this.setUnits(commaSeparatedUnits);
    } else {
      JsonArray unitArray = new JsonArray();
      List<String> units = Lists.newArrayList(commaSeparatedUnits.split(StringUtils.COMMA_STR));
      for (String unit: units) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty(name, unit);
        unitArray.add(jsonObject);
      }
      this.setUnits(unitArray.toString());
    }
    return this;
  }

  /**
   * A constructor that creates a Datetime watermark definition
   * <p>
   * A Datetime watermark has a date range
   * @param name the name of the watermark
   * @param startDate the start date string in yyyy-MM-dd format
   * @param endDate the end date string in yyyy-MM-dd format or - for current date
   */
  public WatermarkDefinition(String name, String startDate, String endDate) {
    this(name, startDate, endDate, false);
  }

  public WatermarkDefinition(String name, String startDate, String endDate, boolean isPartialPartition) {
    this.setName(name);
    this.setType(WatermarkTypes.DATETIME);
    this.setRange(new ImmutablePair<>(startDate, endDate));
    this.isPartialPartition = isPartialPartition;
  }

  public WatermarkDefinition(JsonObject definition, boolean isPartialPartition) {
    this(definition, isPartialPartition, null);
  }

  public WatermarkDefinition(JsonObject definition, boolean isPartialPartition,
      WorkUnitPartitionTypes workUnitPartitionType) {
    this.setName(definition.get(KEY_WORD_NAME).getAsString());
    this.setIsPartialPartition(isPartialPartition);
    if (definition.get(KEY_WORD_TYPE).getAsString().equalsIgnoreCase(WatermarkTypes.DATETIME.name)) {
      this.setType(WatermarkTypes.DATETIME);
      this.setRange(new ImmutablePair<>(
          definition.get(KEY_WORD_RANGE).getAsJsonObject().get(KEY_WORD_RANGE_FROM).getAsString(),
          definition.get(KEY_WORD_RANGE).getAsJsonObject().get(KEY_WORD_RANGE_TO).getAsString()));
      this.setWorkUnitPartitionType(workUnitPartitionType);
    } else if (definition.get(KEY_WORD_TYPE).getAsString().equalsIgnoreCase(WatermarkTypes.UNIT.name)) {
      this.setType(WatermarkTypes.UNIT);
      this.setUnits(definition.get(KEY_WORD_NAME).getAsString(), definition.get(KEY_WORD_UNITS).getAsString());
    }
  }

  @VisibleForTesting
  DateTime getDateTime(String input) {
    DateTimeZone timeZone = DateTimeZone.forID(timezone.isEmpty() ? DEFAULT_TIMEZONE : timezone);
    /**
     * Weekly/Monthly partitioned jobs/sources expect the fromDate to be less than toDate.
     * Keeping the precision at day level for Weekly and Monthly partitioned watermarks.
     *
     * If partial partition is set to true, we don't floor the watermark for a given
     * partition type.
     * For daily partition type, 2019-01-01T12:31:00 will be rounded to 2019-01-01T00:00:00,
     * if partial partition is false.
     */
    if (input.equals("-")) {
      if (WorkUnitPartitionTypes.isMultiDayPartitioned(workUnitPartitionType)) {
        return DateTime.now().withZone(timeZone).dayOfMonth().roundFloorCopy();
      }
      if (this.getIsPartialPartition()) {
        return DateTime.now().withZone(timeZone);
      }
      return DateTime.now().withZone(timeZone).dayOfMonth().roundFloorCopy();
    } else if (input.matches("P\\d+D(T\\d+H){0,1}")) {
      /*
      The standard ISO format - PyYmMwWdDThHmMsS
      Only supporting DAY and HOUR. DAY component is mandatory.
      e.g.P1D, P2DT5H, P0DT7H
      */
      Period period = Period.parse(input);
      DateTime dt = DateTime.now().withZone(timeZone).minus(period);
      if (WorkUnitPartitionTypes.isMultiDayPartitioned(workUnitPartitionType)) {
        return dt.dayOfMonth().roundFloorCopy();
      }
      if (this.getIsPartialPartition()) {
        return dt;
      }
      return dt.dayOfMonth().roundFloorCopy();
    }

    return DateTimeUtils.parse(input, timezone.isEmpty() ? DEFAULT_TIMEZONE : timezone);
  }

  private Long getMillis(String input) {
    return getDateTime(input).getMillis();
  }

  public ImmutablePair<DateTime, DateTime> getRangeInDateTime() {
    return new ImmutablePair<>(getDateTime(range.getKey()), getDateTime(range.getValue()));
  }

  public ImmutablePair<Long, Long> getRangeInMillis() {
    return new ImmutablePair<>(getMillis(range.getKey()), getMillis(range.getValue()));
  }

  /**
   * get a list of work units, with each coded as a name : value pair.
   *
   * The internal storage of work units string should be a JsonArray string
   * @return list of work units
   */
  public List<String> getUnits() {
    List<String> unitList =  Lists.newArrayList();
    JsonArray unitArray = GSON.fromJson(units, JsonArray.class);
    for (JsonElement unit: unitArray) {
      unitList.add(unit.toString());
    }
    return unitList;
  }

  public String getLongName() {
    return "watermark." + name;
  }
}
