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

package org.apache.gobblin.time;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link TimeIterator}
 */
public class TimeIteratorTest {

  private ZoneId zone = ZoneId.of("America/Los_Angeles");

  /**
   * A representative unit test to cover iterating. Actual computations are covered by {@link #testInc()}
   */
  @Test
  public void testIterator() {
    ZonedDateTime startTime = ZonedDateTime.of(2019,12,20,11,
        20,30, 0, zone);
    ZonedDateTime endTime = startTime.plusDays(12);
    TimeIterator iterator = new TimeIterator(startTime, endTime, TimeIterator.Granularity.DAY);
    int days = 0;
    while (iterator.hasNext()) {
      Assert.assertEquals(iterator.next(), startTime.plusDays(days++));
    }
    Assert.assertEquals(days, 13);
  }

  @Test
  public void testInc() {
    ZonedDateTime startTime = ZonedDateTime.of(2019,12,20,11,
        20,30, 0, zone);
    Assert.assertEquals(TimeIterator.inc(startTime, TimeIterator.Granularity.MINUTE, 40).toString(),
        "2019-12-20T12:00:30-08:00[America/Los_Angeles]");
    Assert.assertEquals(TimeIterator.inc(startTime, TimeIterator.Granularity.HOUR, 13).toString(),
        "2019-12-21T00:20:30-08:00[America/Los_Angeles]");
    Assert.assertEquals(TimeIterator.inc(startTime, TimeIterator.Granularity.DAY, 12).toString(),
        "2020-01-01T11:20:30-08:00[America/Los_Angeles]");
    Assert.assertEquals(TimeIterator.inc(startTime, TimeIterator.Granularity.MONTH, 1).toString(),
        "2020-01-20T11:20:30-08:00[America/Los_Angeles]");
  }

  @Test
  public void testDec() {
    ZonedDateTime startTime = ZonedDateTime.of(2019,12,20,11,
        20,30, 0, zone);
    Assert.assertEquals(TimeIterator.dec(startTime, TimeIterator.Granularity.MINUTE, 21).toString(),
        "2019-12-20T10:59:30-08:00[America/Los_Angeles]");
    Assert.assertEquals(TimeIterator.dec(startTime, TimeIterator.Granularity.HOUR, 12).toString(),
        "2019-12-19T23:20:30-08:00[America/Los_Angeles]");
    Assert.assertEquals(TimeIterator.dec(startTime, TimeIterator.Granularity.DAY, 20).toString(),
        "2019-11-30T11:20:30-08:00[America/Los_Angeles]");
    Assert.assertEquals(TimeIterator.dec(startTime, TimeIterator.Granularity.MONTH, 12).toString(),
        "2018-12-20T11:20:30-08:00[America/Los_Angeles]");
  }
}
