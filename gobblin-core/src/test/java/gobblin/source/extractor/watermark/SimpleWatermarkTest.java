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

import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import gobblin.source.extractor.extract.QueryBasedExtractor;

/**
 * Unit tests for {@link SimpleWatermark}.
 *
 * @author ziliu
 */
public class SimpleWatermarkTest {

  private static final String COLUMN = "my_column";
  private static final String GREATER_THAN = ">=";

  private SimpleWatermark simpleWatermark;

  @BeforeClass
  public void setUpBeforeClass() {
    simpleWatermark = new SimpleWatermark(COLUMN, "no_format_needed");
  }

  @Test
  public void testGetWatermarkCondition() {
    QueryBasedExtractor<?, ?> extractor = null;

    //normal case
    Assert.assertEquals(COLUMN + " " + GREATER_THAN + " " + Long.MAX_VALUE,
        simpleWatermark.getWatermarkCondition(extractor, Long.MAX_VALUE, GREATER_THAN));

    //operater is null
    Assert.assertEquals(COLUMN + " null " + Long.MIN_VALUE,
        simpleWatermark.getWatermarkCondition(extractor, Long.MIN_VALUE, null));
  }

  @Test
  public void testGetIntervalsPartitionIntervalNegative() {
    try {
      simpleWatermark.getIntervals(0, 100, Integer.MIN_VALUE, 1000);
      Assert.fail("Expected java.lang.IllegalArgumentException, but didn't get one");
    } catch (java.lang.IllegalArgumentException e) {
    }
  }

  @Test
  public void testGetIntervalsPartitionIntervalZero() {
    try {
      simpleWatermark.getIntervals(0, 100, 0, 1000);
      Assert.fail("Expected java.lang.IllegalArgumentException, but didn't get one");
    } catch (java.lang.IllegalArgumentException e) {
    }
  }

  @Test
  public void testGetIntervalsPartitionIntervalLargerThanDiff() {
    Map<Long, Long> expected = getIntervals(0, 100, 110);
    Map<Long, Long> actual = simpleWatermark.getIntervals(0, 100, 110, 1000);
    Assert.assertEquals(actual, expected);
  }

  @Test
  public void testGetIntervalsNumIntervalsExceedsMaxInterval() {
    int partitionInterval = 100 / 7 + 1;
    Map<Long, Long> expected = getIntervals(0, 100, partitionInterval);
    Map<Long, Long> actual = simpleWatermark.getIntervals(0, 100, 3, 7);
    Assert.assertEquals(actual, expected);
  }

  @Test
  public void testGetIntervalsMaxIntervalsIsOne() {
    Map<Long, Long> expected = getIntervals(0, 100, 100);
    Map<Long, Long> actual = simpleWatermark.getIntervals(0, 100, 1, 1);
    Assert.assertEquals(actual, expected);
  }

  @Test
  public void testGetIntervalsMaxIntervalsIsZero() {
    try {
      simpleWatermark.getIntervals(0, 100, 1, 0);
      Assert.fail("Expected java.lang.IllegalArgumentException, but didn't get one");
    } catch (java.lang.IllegalArgumentException e) {
    }
  }

  @Test
  public void testGetIntervalsMaxIntervalsIsNegative() {
    try {
      simpleWatermark.getIntervals(0, 100, 1, -1);
      Assert.fail("Expected java.lang.IllegalArgumentException, but didn't get one");
    } catch (java.lang.IllegalArgumentException e) {
    }
  }

  @Test
  public void testGetIntervalsLowWatermarkEqualsHighWatermark() {
    Map<Long, Long> expected = getIntervals(100, 100, 1);
    Map<Long, Long> actual = simpleWatermark.getIntervals(100, 100, 10, 10);
    Assert.assertEquals(actual, expected);
  }

  @Test
  public void testGetIntervalsLowWatermarkExceedsHighWatermark() {
    Map<Long, Long> expected = new HashMap<Long, Long>();
    Map<Long, Long> actual = simpleWatermark.getIntervals(110, 100, 10, 10);
    Assert.assertEquals(actual, expected);
  }

  @Test
  public void testGetIntervalsHighWatermarkIsLongMaxValue() {
    Map<Long, Long> expected = getIntervals(Long.MAX_VALUE - 100, Long.MAX_VALUE, 10);
    Map<Long, Long> actual = simpleWatermark.getIntervals(Long.MAX_VALUE - 100, Long.MAX_VALUE, 10, 100);
    Assert.assertEquals(actual, expected);
  }

  @Test
  public void testGetIntervalsLowWatermarkIsLongMinValue() {
    Map<Long, Long> expected = getIntervals(Long.MIN_VALUE, Long.MIN_VALUE + 100, 10);
    Map<Long, Long> actual = simpleWatermark.getIntervals(Long.MIN_VALUE, Long.MIN_VALUE + 100, 10, 100);
    Assert.assertEquals(actual, expected);
  }

  @Test
  public void testGetDeltaNumForNextWatermark() {
    Assert.assertEquals(simpleWatermark.getDeltaNumForNextWatermark(), 1);
  }

  private Map<Long, Long> getIntervals(long lowWatermarkValue, long highWatermarkValue, int partitionInterval) {
    Map<Long, Long> intervals = new HashMap<Long, Long>();
    if (lowWatermarkValue > highWatermarkValue || partitionInterval <= 0)
      return intervals;
    boolean overflow = false;
    for (Long i = lowWatermarkValue; i <= highWatermarkValue && !overflow;) {
      overflow = (Long.MAX_VALUE - partitionInterval < i);
      long end = overflow ? Long.MAX_VALUE : Math.min(i + partitionInterval, highWatermarkValue);
      intervals.put(i, end);
      i = end + 1;
    }
    return intervals;
  }
}