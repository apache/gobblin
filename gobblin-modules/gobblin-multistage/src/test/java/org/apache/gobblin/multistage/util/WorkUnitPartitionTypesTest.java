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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.gobblin.multistage.util.WorkUnitPartitionTypes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class WorkUnitPartitionTypesTest {

  private final static DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.UTC);

  /**
   * test breaking apart a range to hourly brackets, allowing partial hour
   */
  @Test
  public void testGetHourlyRangesPartial() {
    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.UTC);
    String expected =
        "[(1546304400000,1546308000000), (1546308000000,1546311600000), (1546311600000,1546315200000), (1546315200000,1546315506000)]";
    Assert.assertEquals(expected,
        WorkUnitPartitionTypes.HOURLY.getRanges(formatter.parseDateTime("2019-01-01 01:00:00"), formatter.parseDateTime("2019-01-01 04:05:06"), true).toString());
    expected = "[(1546304400000,1546308000000), (1546308000000,1546311600000), (1546311600000,1546315200000)]";
    // High watermark truncated
    Assert.assertEquals(expected,
        WorkUnitPartitionTypes.HOURLY.getRanges(formatter.parseDateTime("2019-01-01 01:00:00"), formatter.parseDateTime("2019-01-01 04:00:00"), true).toString());
  }

  /**
   * test breaking apart a range to hourly brackets
   */
  @Test
  public void testGetHourlyRanges() {
    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.UTC);
    String expected = "[(1546304400000,1546308000000), (1546308000000,1546311600000), (1546311600000,1546315200000)]";
    Assert.assertEquals(WorkUnitPartitionTypes.HOURLY.getRanges(formatter.parseDateTime("2019-01-01 01:00:00"),
        formatter.parseDateTime("2019-01-01 04:05:06"), false).toString(),
        expected);
    expected = "[(1546304400000,1546308000000), (1546308000000,1546311600000)]";
    // High watermark truncated
    Assert.assertEquals(WorkUnitPartitionTypes.HOURLY.getRanges(formatter.parseDateTime("2019-01-01 01:00:00"),
        formatter.parseDateTime("2019-01-01 03:00:00"), false).toString(),
        expected);
  }

  /**
   * test breaking apart a range to daily brackets
   */
  @Test
  public void testGetDailyRanges() {
    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.UTC);

    String expected = "[(1546300800000,1546387200000), (1546387200000,1546473600000)]";
    Assert.assertEquals(WorkUnitPartitionTypes.DAILY.getRanges(formatter.parseDateTime("2019-01-01 00:00:00"),
        formatter.parseDateTime("2019-01-03 00:00:00"), true).toString(),
        expected);

    expected = "[(1546300800000,1546387200000), (1546387200000,1546473600000), (1546473600000,1546477323000)]";
    // High watermark not truncated
    Assert.assertEquals(WorkUnitPartitionTypes.DAILY.getRanges(formatter.parseDateTime("2019-01-01 00:00:00"),
        formatter.parseDateTime("2019-01-03 01:02:03"), true).toString(),
        expected);
  }

  /**
   * test breaking apart a recent range to daily brackets
   */
  @Test
  public void testGetDailyRangesRecent() {
    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.UTC);
    Assert.assertEquals(
        WorkUnitPartitionTypes.DAILY.getRanges(DateTime.now().minusDays(2).dayOfMonth().roundFloorCopy(), DateTime.now(), false).size(),
        2);
    Assert.assertEquals(
        WorkUnitPartitionTypes.DAILY.getRanges(DateTime.now().minusDays(2).dayOfMonth().roundFloorCopy(), DateTime.now(), true).size(),
        3);
  }

  /**
   * test breaking apart a range to a weekly brackets
   */
  @Test
  public void testGetWeeklyRanges() {
    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.UTC);
    String expected = "[(1546300800000,1546905600000), (1546905600000,1547078400000)]";
    Assert.assertEquals(WorkUnitPartitionTypes.WEEKLY.getRanges(formatter.parseDateTime("2019-01-01"),
        formatter.parseDateTime("2019-01-10"), true).toString(),
        expected);
    expected = "[(1546300800000,1546905600000)]";
    Assert.assertEquals(WorkUnitPartitionTypes.WEEKLY.getRanges(formatter.parseDateTime("2019-01-01"),
        formatter.parseDateTime("2019-01-10"), false).toString(),
        expected);
    Assert.assertEquals(WorkUnitPartitionTypes.WEEKLY.getRanges(formatter.parseDateTime("2019-01-01"),
        formatter.parseDateTime("2019-01-10")).toString(),
        expected);
  }

  /**
   * Test from string
   */
  @Test
  public void testFromString() {
    Assert.assertEquals(null, WorkUnitPartitionTypes.fromString(null));
    Assert.assertEquals(WorkUnitPartitionTypes.HOURLY, WorkUnitPartitionTypes.fromString("hourly"));
    Assert.assertEquals(WorkUnitPartitionTypes.DAILY, WorkUnitPartitionTypes.fromString("daily"));
    Assert.assertEquals(WorkUnitPartitionTypes.WEEKLY, WorkUnitPartitionTypes.fromString("weekly"));
    Assert.assertEquals(WorkUnitPartitionTypes.MONTHLY, WorkUnitPartitionTypes.fromString("monthly"));
  }

  /**
   * Test is weekly or monthly partitioned
   */
  @Test
  public void testIsDayPartitioned() {
    Assert.assertFalse(WorkUnitPartitionTypes.isMultiDayPartitioned(null));
    Assert.assertFalse(WorkUnitPartitionTypes.isMultiDayPartitioned(WorkUnitPartitionTypes.HOURLY));
    Assert.assertFalse(WorkUnitPartitionTypes.isMultiDayPartitioned(WorkUnitPartitionTypes.DAILY));
    Assert.assertTrue(WorkUnitPartitionTypes.isMultiDayPartitioned(WorkUnitPartitionTypes.WEEKLY));
    Assert.assertTrue(WorkUnitPartitionTypes.isMultiDayPartitioned(WorkUnitPartitionTypes.MONTHLY));
  }

  /**
   * test breaking apart a range to a monthly brackets
   */
  @Test
  public void testGetMonthlyRanges() {
    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.UTC);
    String expected = "[(1546300800000,1548979200000), (1548979200000,1549756800000)]";
    Assert.assertEquals(WorkUnitPartitionTypes.MONTHLY.getRanges(
        formatter.parseDateTime("2019-01-01"), formatter.parseDateTime("2019-02-10"), true).toString(),
        expected);
  }

  /**
   * Test getRanges() with date range which doesn't allow partial partitions
   * Expected: list of full ranges
   */
  @Test
  public void testGetRangesWithoutPartitions() {
    String expected = "[(1546387200000,1546992000000)]";// [2019-01-02 -> 2019-01-08]
    Assert.assertEquals(
        WorkUnitPartitionTypes.WEEKLY.getRanges(
            new ImmutablePair<>(DATE_FORMATTER.parseDateTime("2019-01-02"), DATE_FORMATTER.parseDateTime("2019-01-10"))).toString(),
        expected
    );
  }

  @Test
  public void testString() {
    Assert.assertEquals(WorkUnitPartitionTypes.WEEKLY.toString(), "weekly");
  }
}
