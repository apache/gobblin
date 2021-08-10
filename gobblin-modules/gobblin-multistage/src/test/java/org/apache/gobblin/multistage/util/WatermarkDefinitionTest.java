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

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import org.apache.gobblin.multistage.util.WatermarkDefinition;
import org.apache.gobblin.multistage.util.WorkUnitPartitionTypes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.gobblin.multistage.util.DateTimeUtils.*;


@Test
public class WatermarkDefinitionTest {
  private String expected;
  WatermarkDefinition definitions;
  /**
   * test typical watermark definitions
   */
  @Test
  public void testInitialization() {
    expected = "(1546329600000,1546416000000)";
    definitions = new WatermarkDefinition("primary", "2019-01-01", "2019-01-02");
    Assert.assertEquals(expected, definitions.getRangeInMillis().toString());

    expected = "(2019-01-01T00:00:00.000-08:00,2019-01-02T00:00:00.000-08:00)";
    Assert.assertEquals(expected, definitions.getRangeInDateTime().toString());

    Gson gson = new Gson();
    String def = "[{\"name\":\"system\",\"type\":\"datetime\",\"range\":{\"from\":\"2017-01-02\",\"to\":\"-\"}}]";
    JsonArray defArray = gson.fromJson(def, JsonArray.class);
    definitions = new WatermarkDefinition(defArray.get(0).getAsJsonObject(), false);
    Assert.assertNotNull(definitions);
  }

  /**
   * test partition precision at day level for Weekly and Monthly partition types
   */
  @Test
  public void testPartitionPrecisionAtDayLevelForWeeklyAndMonthlyPartitionTypes() {
    DateTimeZone timeZone = DateTimeZone.forID("America/Los_Angeles");

    // Testing with 'to' as 'current date time(-)'
    String expectedInMillis = String.format("(%s,%s)",
        "1585810800000",
        DateTime.now().withZone(timeZone).dayOfMonth().roundFloorCopy().getMillis());
    String expectedInDateTime = String.format("(%s,%s)",
        "2020-04-02T00:00:00.000-07:00",
        DateTime.now().withZone(timeZone).dayOfMonth().roundFloorCopy());
    String jsonDef = "[{\"name\":\"system\",\"type\":\"datetime\",\"range\":{\"from\":\"2020-04-02\",\"to\":\"-\"}}]";
    helperPartitionPrecisionAtDayLevelForWeeklyAndMonthlyPartitionTypes(jsonDef, false, WorkUnitPartitionTypes.WEEKLY,
        expectedInMillis, expectedInDateTime);
    helperPartitionPrecisionAtDayLevelForWeeklyAndMonthlyPartitionTypes(jsonDef, true, WorkUnitPartitionTypes.WEEKLY,
        expectedInMillis, expectedInDateTime);
    helperPartitionPrecisionAtDayLevelForWeeklyAndMonthlyPartitionTypes(jsonDef, false, WorkUnitPartitionTypes.MONTHLY,
        expectedInMillis, expectedInDateTime);
    helperPartitionPrecisionAtDayLevelForWeeklyAndMonthlyPartitionTypes(jsonDef, true, WorkUnitPartitionTypes.MONTHLY,
        expectedInMillis, expectedInDateTime);

    // Testing with 'to' as 'two days ago(P2D)'
    expectedInMillis = String.format("(%s,%s)",
        "1585810800000",
        DateTime.now().withZone(timeZone).minusDays(2).dayOfMonth().roundFloorCopy().getMillis());
    expectedInDateTime = String.format("(%s,%s)",
        "2020-04-02T00:00:00.000-07:00",
        DateTime.now().withZone(timeZone).minusDays(2).dayOfMonth().roundFloorCopy());
    jsonDef = "[{\"name\":\"system\",\"type\":\"datetime\",\"range\":{\"from\":\"2020-04-02\",\"to\":\"P2D\"}}]";
    helperPartitionPrecisionAtDayLevelForWeeklyAndMonthlyPartitionTypes(jsonDef, false, WorkUnitPartitionTypes.WEEKLY,
        expectedInMillis, expectedInDateTime);
    helperPartitionPrecisionAtDayLevelForWeeklyAndMonthlyPartitionTypes(jsonDef, true, WorkUnitPartitionTypes.WEEKLY,
        expectedInMillis, expectedInDateTime);
    helperPartitionPrecisionAtDayLevelForWeeklyAndMonthlyPartitionTypes(jsonDef, false, WorkUnitPartitionTypes.MONTHLY,
        expectedInMillis, expectedInDateTime);
    helperPartitionPrecisionAtDayLevelForWeeklyAndMonthlyPartitionTypes(jsonDef, true, WorkUnitPartitionTypes.MONTHLY,
        expectedInMillis, expectedInDateTime);
  }

  private void helperPartitionPrecisionAtDayLevelForWeeklyAndMonthlyPartitionTypes(String jsonDef,
      boolean isPartialPartition, WorkUnitPartitionTypes workUnitPartitionType,
      String expectedInMillis, String expectedInDateTime) {
    Gson gson = new Gson();
    JsonArray defArray = gson.fromJson(jsonDef, JsonArray.class);
    definitions = new WatermarkDefinition(defArray.get(0).getAsJsonObject(),
        isPartialPartition, workUnitPartitionType);

    Assert.assertEquals(definitions.getRangeInMillis().toString(), expectedInMillis);
    Assert.assertEquals(definitions.getRangeInDateTime().toString(), expectedInDateTime);
  }


  /**
   * a unit watermark can be a simple list of value strings, in such case the watermark name
   * and individual values in the string will be made name : value pairs
   */
  @Test
  public void testSimpleUnitWatermarkDefintion() {
    expected = "[{\"secondary\":\"2018\"}, {\"secondary\":\"2019\"}]";
    definitions = new WatermarkDefinition("secondary", "2018,2019");
    Assert.assertEquals(definitions.getUnits().toString(), expected);
  }

  @Test
  public void testGetDateTimePartialWithJson() {
    Gson gson = new Gson();
    String def = "[{\"name\":\"system\",\"type\":\"datetime\",\"range\":{\"from\":\"2017-01-02\",\"to\":\"-\"}}]";
    JsonArray defArray = gson.fromJson(def, JsonArray.class);
    definitions = new WatermarkDefinition(defArray.get(0).getAsJsonObject(), true);
    DateTimeZone timeZone = DateTimeZone.forID("America/Los_Angeles");
    // If partial partition is set to true, time should not round to 00:00:00-000
    Assert.assertNotEquals(definitions.getRangeInDateTime().getRight(),
        DateTime.now().withZone(timeZone).dayOfMonth().roundFloorCopy());
    definitions = new WatermarkDefinition(defArray.get(0).getAsJsonObject(), false);
    // If partial partition is set to false, time should be rounded to 00:00:00-000
    Assert.assertEquals(definitions.getRangeInDateTime().getRight(),
        DateTime.now().withZone(timeZone).dayOfMonth().roundFloorCopy());
  }

  @Test
  public void testGetDateTime() {
    DateTimeZone timeZone = DateTimeZone.forID("America/Los_Angeles");

    // P1D
    definitions = new WatermarkDefinition("primary", "2020-01-01", "P1D", false);
    WatermarkDefinition definitionsIsPartial = new WatermarkDefinition("primary", "2020-01-01", "P1D", true);

    Assert.assertEquals(definitions.getRangeInDateTime().getRight(),
        DateTime.now().withZone(timeZone).minusDays(1).dayOfMonth().roundFloorCopy());

    // the millis second difference can often fail the test
    // truncating the seconds and milli seconds to ensure success
    Assert.assertEquals(definitionsIsPartial.getRangeInDateTime().getRight().minuteOfDay().roundFloorCopy(),
        DateTime.now().withZone(timeZone).minusDays(1).minuteOfDay().roundFloorCopy());

    // P2DT5H
    definitions = new WatermarkDefinition("primary", "2020-01-01", "P2DT5H", false);
    definitionsIsPartial = new WatermarkDefinition("primary", "2020-01-01", "P2DT5H", true);

    Assert.assertEquals(definitions.getRangeInDateTime().getRight(),
        DateTime.now().withZone(timeZone).minusDays(2).minusHours(5).dayOfMonth().roundFloorCopy());

    // the millis second difference can often fail the test
    // truncating the seconds and milli seconds to ensure success
    Assert.assertEquals(definitionsIsPartial.getRangeInDateTime().getRight().minuteOfDay().roundFloorCopy(),
        DateTime.now().withZone(timeZone).minusDays(2).minusHours(5).minuteOfDay().roundFloorCopy());

    // P0DT7H
    definitions = new WatermarkDefinition("primary", "2020-01-01", "P0DT7H", false);
    definitionsIsPartial = new WatermarkDefinition("primary", "2020-01-01", "P0DT7H", true);

    Assert.assertEquals(definitions.getRangeInDateTime().getRight(),
        DateTime.now().withZone(timeZone).minusHours(7).dayOfMonth().roundFloorCopy());

    // the millis second difference can often fail the test
    // truncating the seconds and milli seconds to ensure success
    Assert.assertEquals(definitionsIsPartial.getRangeInDateTime().getRight().minuteOfDay().roundFloorCopy(),
        DateTime.now().withZone(timeZone).minusHours(7).minuteOfDay().roundFloorCopy());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetDateTimeWithInvalidIsoFormat1() {
    DateTimeZone timeZone = DateTimeZone.forID("America/Los_Angeles");
    definitions = new WatermarkDefinition("primary", "2020-01-01", "Pfoobarfoobar", false);
    definitions.getRangeInDateTime().getRight();
  }

  @Test(expectedExceptions = StringIndexOutOfBoundsException.class)
  public void testGetDateTimeWithInvalidIsoFormat2() {
    DateTimeZone timeZone = DateTimeZone.forID("America/Los_Angeles");
    definitions = new WatermarkDefinition("primary", "2020-01-01", "Pfoobar", false);
    definitions.getRangeInDateTime().getRight();
  }

  @Test(expectedExceptions = StringIndexOutOfBoundsException.class)
  public void testGetDateTimeWithValidMonthButUnsupportedIsoFormat() {
    DateTimeZone timeZone = DateTimeZone.forID("America/Los_Angeles");
    definitions = new WatermarkDefinition("primary", "2020-01-01", "P1M", false);
    definitions.getRangeInDateTime().getRight();
  }

  @Test(expectedExceptions = StringIndexOutOfBoundsException.class)
  public void testGetDateTimeWithValidMinutesButUnsupportedIsoFormat() {
    DateTimeZone timeZone = DateTimeZone.forID("America/Los_Angeles");
    definitions = new WatermarkDefinition("primary", "2020-01-01", "PT10M", false);
    definitions.getRangeInDateTime().getRight();
  }

  @Test
  public void testToString() {
    Assert.assertEquals(WatermarkDefinition.WatermarkTypes.UNIT.toString(), "unit");
    Assert.assertEquals(WatermarkDefinition.WatermarkTypes.DATETIME.toString(), "datetime");
  }

  @Test
  public void testSetUnits() {
    String watermarkString = "[{\"secondary\":\"2018\"}, {\"secondary\":\"2019\"}]";
    definitions = new WatermarkDefinition("secondary", watermarkString);
    Assert.assertEquals(definitions.getName(), "secondary");
    Assert.assertEquals(definitions.getUnits().toString(), watermarkString);
  }

  @Test
  public void testGetDateTimeII() {
    definitions = new WatermarkDefinition("primary", "[{\"secondary\":\"2018\"}]");
    Assert.assertEquals(definitions.getDateTime("2020-01-01 10:00:30").toString(),
        "2020-01-01T10:00:30.000-08:00");

    definitions.setTimezone(DEFAULT_TIMEZONE);
    Assert.assertEquals(definitions.getDateTime("2020-01-01 10:00:30").toString(),
        "2020-01-01T10:00:30.000-08:00");
  }
}
