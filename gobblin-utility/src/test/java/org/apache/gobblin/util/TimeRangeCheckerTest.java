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

package org.apache.gobblin.util;

import com.google.common.collect.Lists;

import org.apache.gobblin.configuration.ConfigurationKeys;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = {"gobblin.util"})
public class TimeRangeCheckerTest {

  @Test
  public void testTimeRangeChecker() {
    // January 1st, 2015 (a Thursday)
    DateTime dateTime = new DateTime(2015, 1, 1, 0, 0, 0, DateTimeZone.forID(ConfigurationKeys.PST_TIMEZONE_NAME));

    // Positive Tests

    // Base hour
    Assert.assertTrue(TimeRangeChecker.isTimeInRange(Lists.newArrayList("THURSDAY"), "00-00", "06-00", dateTime));

    // Valid minute
    Assert.assertTrue(TimeRangeChecker.isTimeInRange(Lists.newArrayList("THURSDAY"), "00-00", "00-01", dateTime));

    // Multiple days
    Assert.assertTrue(TimeRangeChecker.isTimeInRange(Lists.newArrayList("MONDAY", "THURSDAY"), "00-00", "06-00", dateTime));

    // Negative Tests

    // Invalid day
    Assert.assertFalse(TimeRangeChecker.isTimeInRange(Lists.newArrayList("MONDAY"), "00-00", "06-00", dateTime));

    // Invalid minute
    Assert.assertFalse(TimeRangeChecker.isTimeInRange(Lists.newArrayList("THURSDAY"), "00-01", "06-00", dateTime));

    // Invalid hour
    Assert.assertFalse(TimeRangeChecker.isTimeInRange(Lists.newArrayList("THURSDAY"), "01-00", "06-00", dateTime));
  }
}
