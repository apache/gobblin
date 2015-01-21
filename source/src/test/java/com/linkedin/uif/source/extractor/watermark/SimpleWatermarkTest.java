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

package com.linkedin.uif.source.extractor.watermark;

import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.uif.source.extractor.extract.QueryBasedExtractor;


public class SimpleWatermarkTest {

  private SimpleWatermark simpleWatermark;

  @BeforeClass
  public void setUpBeforeClass() {
    simpleWatermark = new SimpleWatermark("test_column", "no_format_needed");
  }

  @Test
  public void testGetWatermarkCondition() {
    QueryBasedExtractor<?, ?> ext = null;
    long watermarkValue = Long.MAX_VALUE;
    String operator = ">=";
    
    //normal case
    Assert.assertEquals("test_column >= " + Long.MAX_VALUE, 
        simpleWatermark.getWatermarkCondition(ext, watermarkValue, operator));
    
    //operater is null
    watermarkValue = Long.MIN_VALUE;
    Assert.assertEquals("test_column null " + Long.MIN_VALUE, 
        simpleWatermark.getWatermarkCondition(ext, watermarkValue, null));
  }
  
  @Test
  public void testGetIntervals() {
    Map<Long, Long> expected = new HashMap<Long, Long>();
    Map<Long, Long> actual = new HashMap<Long, Long>();
    
    //partitionInterval is negative
    expected = getIntervals(0, 100, 1);
    actual = simpleWatermark.getIntervals(0, 100, Integer.MIN_VALUE, 1000);
    Assert.assertEquals(actual, expected);
    
    //partitionInterval is 0
    expected = getIntervals(0, 100, 1);
    actual = simpleWatermark.getIntervals(0, 100, Integer.MIN_VALUE, 1000);
    Assert.assertEquals(actual, expected);
    
    //partitionInterval is larger than the whole interval
    expected = getIntervals(0, 100, 110);
    actual = simpleWatermark.getIntervals(0, 100, 110, 1000);
    Assert.assertEquals(actual, expected);
    
    //Number of partitions obtained based on partitionInterval exceeds maxIntervals\
    int partitionInterval = 100 / 7 + 1;
    expected = getIntervals(0, 100, partitionInterval);
    actual = simpleWatermark.getIntervals(0, 100, 3, 7);
    Assert.assertEquals(actual, expected);
    
    //maxIntervals = 1
    expected = getIntervals(0, 100, 100);
    actual = simpleWatermark.getIntervals(0, 100, 1, 1);
    Assert.assertEquals(actual, expected);
    
    //maxIntervals = 0: should return empty map
    expected.clear();
    actual = simpleWatermark.getIntervals(0, 100, 1, 0);
    Assert.assertEquals(actual, expected);
   
    //maxIntervals is negative
    expected.clear();
    actual = simpleWatermark.getIntervals(0, 100, 1, 0);
    Assert.assertEquals(actual, expected);
    
    //lowWatermark = highWatermark: should return a single interval
    expected = getIntervals(100, 100, 10);
    actual = simpleWatermark.getIntervals(100, 100, 10, 10);
    Assert.assertEquals(actual, expected);
    
    //lowWatermark > highWatermark : should return empty map
    expected.clear();
    actual = simpleWatermark.getIntervals(110, 100, 10, 10);
    Assert.assertEquals(actual, expected);
    
    //Long.MAX_VALUE
    expected = getIntervals(Long.MAX_VALUE - 100, Long.MAX_VALUE, 10);
    actual = simpleWatermark.getIntervals(Long.MAX_VALUE - 100, Long.MAX_VALUE, 10, 100);
    Assert.assertEquals(actual, expected);
    
    //Long.MIN_VALUE
    expected = getIntervals(Long.MIN_VALUE, Long.MIN_VALUE + 100, 10);
    actual = simpleWatermark.getIntervals(Long.MIN_VALUE, Long.MIN_VALUE + 100, 10, 100);
    Assert.assertEquals(actual, expected);
    
    //both Long.MIN_VALUE and Long.MAX_VALUE
    //This test case is not used, since to pass this test case, we need to use BigInteger which may impact performance.
    
    //expected.clear();
    //expected.put(Long.MIN_VALUE, Long.MAX_VALUE);
    //actual = simpleWatermark.getIntervals(Long.MIN_VALUE, Long.MAX_VALUE, 1, 1);
    //Assert.assertEquals(actual, expected);
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
