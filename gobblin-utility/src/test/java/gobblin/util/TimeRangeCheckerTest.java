// Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied.

package gobblin.util;

import com.google.common.collect.Lists;

import org.joda.time.DateTime;

import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = {"gobblin.util"})
public class TimeRangeCheckerTest {

  @Test
  public void testTimeRangeChecker() {
    DateTime dateTime = new DateTime(2015, 1, 1, 0, 0, 0); // January 1st, 2015 (a Thursday)

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
