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

package gobblin.source.extractor.watermark;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.extract.jdbc.MysqlExtractor;
import gobblin.source.extractor.extract.jdbc.SqlServerExtractor;

/**
 * Unit tests for {@link TimestampWatermark}.
 *
 * @author ziliu
 */
public class TimestampWatermarkTest {

  private static final long WATERMARK_VALUE = 20141029133015L;
  private static final long LOW_WATERMARK_VALUE = 20130501130000L;
  private static final long HIGH_WATERMARK_VALUE = 20130502080000L;
  private static final String COLUMN = "my_column";
  private static final String OPERATOR = ">=";

  private TimestampWatermark tsWatermark;
  private final String watermarkFormat = "yyyyMMddHHmmss";
  private final WorkUnitState workunitState = new WorkUnitState();

  @BeforeClass
  public void setUpBeforeClass() throws Exception {
    tsWatermark = new TimestampWatermark(COLUMN, watermarkFormat);
    workunitState.setId("");
  }

  @Test
  public void testGetWatermarkConditionMySql() throws Exception {
    MysqlExtractor extractor = new MysqlExtractor(workunitState);
    Assert.assertEquals(tsWatermark.getWatermarkCondition(extractor, WATERMARK_VALUE, OPERATOR),
        COLUMN + " " + OPERATOR + " '2014-10-29 13:30:15'");
  }

  @Test
  public void testGetWatermarkConditionSqlServer() throws Exception {
    SqlServerExtractor extractor = new SqlServerExtractor(workunitState);
    Assert.assertEquals(tsWatermark.getWatermarkCondition(extractor, WATERMARK_VALUE, OPERATOR),
        COLUMN + " " + OPERATOR + " '2014-10-29 13:30:15'");
  }

  @Test(expectedExceptions = java.lang.IllegalArgumentException.class)
  public void testGetIntervalsPartitionIntervalNegative() throws Exception {
    tsWatermark.getIntervals(LOW_WATERMARK_VALUE, HIGH_WATERMARK_VALUE, Integer.MIN_VALUE, 1000);
  }

  @Test(expectedExceptions = java.lang.IllegalArgumentException.class)
  public void testGetIntervalsPartitionIntervalZero() throws Exception {
    tsWatermark.getIntervals(LOW_WATERMARK_VALUE, HIGH_WATERMARK_VALUE, 0, 1000);
  }

  @Test
  public void testGetIntervalsPartitionIntervalLargerThanDiff() throws ParseException {
    Map<Long, Long> expected = getIntervals(LOW_WATERMARK_VALUE, HIGH_WATERMARK_VALUE, 1000);
    Map<Long, Long> actual = tsWatermark.getIntervals(LOW_WATERMARK_VALUE, HIGH_WATERMARK_VALUE, 1000, 1000);
    Assert.assertEquals(actual, expected);
  }

  @Test
  public void testGetIntervalsNumIntervalsExceedsMaxInterval() throws ParseException {
    Map<Long, Long> expected = getIntervals(LOW_WATERMARK_VALUE, HIGH_WATERMARK_VALUE, 1000);
    Map<Long, Long> actual = tsWatermark.getIntervals(LOW_WATERMARK_VALUE, HIGH_WATERMARK_VALUE, 1, 1);
    Assert.assertEquals(actual, expected);
  }

  @Test(expectedExceptions = java.lang.IllegalArgumentException.class)
  public void testGetIntervalsMaxIntervalsIsZero() {
    tsWatermark.getIntervals(LOW_WATERMARK_VALUE, HIGH_WATERMARK_VALUE, 1, 0);
  }

  @Test(expectedExceptions = java.lang.IllegalArgumentException.class)
  public void testGetIntervalsMaxIntervalsIsNegative() {
    tsWatermark.getIntervals(LOW_WATERMARK_VALUE, HIGH_WATERMARK_VALUE, 1, -1);
  }

  @Test
  public void testGetIntervalsLowWatermarkExceedsHighWatermark() {
    Map<Long, Long> expected = new HashMap<Long, Long>();
    Map<Long, Long> actual = tsWatermark.getIntervals(HIGH_WATERMARK_VALUE, LOW_WATERMARK_VALUE, 1, 10);
    Assert.assertEquals(actual, expected);
  }

  @Test
  public void testGetIntervalsLowWatermarkEqualsHighWatermark() throws ParseException {
    Map<Long, Long> expected = getIntervals(LOW_WATERMARK_VALUE, LOW_WATERMARK_VALUE, 1000);
    Map<Long, Long> actual = tsWatermark.getIntervals(LOW_WATERMARK_VALUE, LOW_WATERMARK_VALUE, 1, 10);
    Assert.assertEquals(actual, expected);
  }

  private Map<Long, Long> getIntervals(long lowWatermarkValue, long highWatermarkValue, int partitionInterval) throws ParseException {
    Map<Long, Long> intervals = new HashMap<Long, Long>();
    if (lowWatermarkValue > highWatermarkValue || partitionInterval <= 0)
      return intervals;
    final SimpleDateFormat inputFormat = new SimpleDateFormat(watermarkFormat);
    Date startTime = inputFormat.parse(String.valueOf(lowWatermarkValue));
    Date endTime = inputFormat.parse(String.valueOf(highWatermarkValue));
    Calendar cal = Calendar.getInstance();

    while (startTime.compareTo(endTime) <= 0) {
      cal.setTime(startTime);
      cal.add(Calendar.HOUR, partitionInterval);
      Date nextTime = cal.getTime();
      if (nextTime.compareTo(endTime) > 0) {
        nextTime = endTime;
      }
      intervals.put(Long.parseLong(inputFormat.format(startTime)), Long.parseLong(inputFormat.format(nextTime)));
      cal.add(Calendar.SECOND, 1);
      startTime = cal.getTime();
    }
    return intervals;
  }
}
