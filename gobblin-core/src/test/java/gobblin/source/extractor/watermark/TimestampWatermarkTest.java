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


/**
 * Unit tests for {@link TimestampWatermark}.
 *
 * @author Ziyang Liu
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
    this.tsWatermark = new TimestampWatermark(COLUMN, this.watermarkFormat);
    this.workunitState.setId("");
  }

  @Test(expectedExceptions = java.lang.IllegalArgumentException.class)
  public void testGetIntervalsPartitionIntervalNegative() throws Exception {
    this.tsWatermark.getIntervals(LOW_WATERMARK_VALUE, HIGH_WATERMARK_VALUE, Integer.MIN_VALUE, 1000);
  }

  @Test(expectedExceptions = java.lang.IllegalArgumentException.class)
  public void testGetIntervalsPartitionIntervalZero() throws Exception {
    this.tsWatermark.getIntervals(LOW_WATERMARK_VALUE, HIGH_WATERMARK_VALUE, 0, 1000);
  }

  @Test
  public void testGetIntervalsPartitionIntervalLargerThanDiff() throws ParseException {
    Map<Long, Long> expected = getIntervals(LOW_WATERMARK_VALUE, HIGH_WATERMARK_VALUE, 1000);
    Map<Long, Long> actual = this.tsWatermark.getIntervals(LOW_WATERMARK_VALUE, HIGH_WATERMARK_VALUE, 1000, 1000);
    Assert.assertEquals(actual, expected);
  }

  @Test
  public void testGetIntervalsNumIntervalsExceedsMaxInterval() throws ParseException {
    Map<Long, Long> expected = getIntervals(LOW_WATERMARK_VALUE, HIGH_WATERMARK_VALUE, 1000);
    Map<Long, Long> actual = this.tsWatermark.getIntervals(LOW_WATERMARK_VALUE, HIGH_WATERMARK_VALUE, 1, 1);
    Assert.assertEquals(actual, expected);
  }

  @Test(expectedExceptions = java.lang.IllegalArgumentException.class)
  public void testGetIntervalsMaxIntervalsIsZero() {
    this.tsWatermark.getIntervals(LOW_WATERMARK_VALUE, HIGH_WATERMARK_VALUE, 1, 0);
  }

  @Test(expectedExceptions = java.lang.IllegalArgumentException.class)
  public void testGetIntervalsMaxIntervalsIsNegative() {
    this.tsWatermark.getIntervals(LOW_WATERMARK_VALUE, HIGH_WATERMARK_VALUE, 1, -1);
  }

  @Test
  public void testGetIntervalsLowWatermarkExceedsHighWatermark() {
    Map<Long, Long> expected = new HashMap<Long, Long>();
    Map<Long, Long> actual = this.tsWatermark.getIntervals(HIGH_WATERMARK_VALUE, LOW_WATERMARK_VALUE, 1, 10);
    Assert.assertEquals(actual, expected);
  }

  @Test
  public void testGetIntervalsLowWatermarkEqualsHighWatermark() throws ParseException {
    Map<Long, Long> expected = getIntervals(LOW_WATERMARK_VALUE, LOW_WATERMARK_VALUE, 1000);
    Map<Long, Long> actual = this.tsWatermark.getIntervals(LOW_WATERMARK_VALUE, LOW_WATERMARK_VALUE, 1, 10);
    Assert.assertEquals(actual, expected);
  }

  private Map<Long, Long> getIntervals(long lowWatermarkValue, long highWatermarkValue, int partitionInterval)
      throws ParseException {
    Map<Long, Long> intervals = new HashMap<Long, Long>();
    if (lowWatermarkValue > highWatermarkValue || partitionInterval <= 0)
      return intervals;
    final SimpleDateFormat inputFormat = new SimpleDateFormat(this.watermarkFormat);
    Date startTime = inputFormat.parse(String.valueOf(lowWatermarkValue));
    Date endTime = inputFormat.parse(String.valueOf(highWatermarkValue));
    Calendar cal = Calendar.getInstance();

    while (startTime.compareTo(endTime) < 0) {
      cal.setTime(startTime);
      cal.add(Calendar.HOUR, partitionInterval);
      Date nextTime = cal.getTime();
      if (nextTime.compareTo(endTime) > 0) {
        nextTime = endTime;
      }
      intervals.put(Long.parseLong(inputFormat.format(startTime)), Long.parseLong(inputFormat.format(nextTime)));
      startTime = nextTime;
    }
    return intervals;
  }
}
