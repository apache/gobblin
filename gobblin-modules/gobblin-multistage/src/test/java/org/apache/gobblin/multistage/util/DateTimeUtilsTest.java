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

import org.apache.gobblin.multistage.util.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class DateTimeUtilsTest {
  @Test
  public void testParser() {
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01").toString("yyyy-MM-dd"), "2020-01-01");
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01 11:11:11").toString("yyyy-MM-dd HH:mm:ss"),
        "2020-01-01 11:11:11");
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01T11:11:11").toString("yyyy-MM-dd'T'HH:mm:ss"),
        "2020-01-01T11:11:11");
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01T11:11:11.9").toString("yyyy-MM-dd'T'HH:mm:ss.SSS"),
        "2020-01-01T11:11:11.900");
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01T11:11:11.99").toString("yyyy-MM-dd'T'HH:mm:ss.SSS"),
        "2020-01-01T11:11:11.990");
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01T11:11:11.999").toString("yyyy-MM-dd'T'HH:mm:ss.SSS"),
        "2020-01-01T11:11:11.999");
    // test microseconds truncation as Joda supports only milliseconds
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01T11:11:11.9999").toString("yyyy-MM-dd'T'HH:mm:ss.SSS"),
        "2020-01-01T11:11:11.999");
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01T11:11:11.99999").toString("yyyy-MM-dd'T'HH:mm:ss.SSS"),
        "2020-01-01T11:11:11.999");
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01T11:11:11.999999").toString("yyyy-MM-dd'T'HH:mm:ss.SSS"),
        "2020-01-01T11:11:11.999");

    Assert.assertEquals(DateTimeUtils.parse("2020-01-01 10:00:00-07:00")
        .withZone(DateTimeZone.UTC)
        .toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ"), "2020-01-01T17:00:00.000+0000");
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01 10:00:00.000-0700")
        .withZone(DateTimeZone.UTC)
        .toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ"), "2020-01-01T17:00:00.000+0000");
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01 10:00:00.000-07:00")
        .withZone(DateTimeZone.UTC)
        .toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ"), "2020-01-01T17:00:00.000+0000");

    Assert.assertEquals(DateTimeUtils.parse("2020-01-01T10:00:00-0700")
        .withZone(DateTimeZone.UTC)
        .toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ"), "2020-01-01T17:00:00.000+0000");
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01T10:00:00.000-0700")
        .withZone(DateTimeZone.UTC)
        .toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ"), "2020-01-01T17:00:00.000+0000");
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01T10:00:00.000-07:00")
        .withZone(DateTimeZone.UTC)
        .toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ"), "2020-01-01T17:00:00.000+0000");

    Assert.assertEquals(DateTimeUtils.parse("2020-01-01 10:00:00PST").getZone().getID(), "America/Los_Angeles");
    Assert.assertEquals(
        DateTimeUtils.parse("2020-01-01 10:00:00PST").withZone(DateTimeZone.UTC).toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
        "2020-01-01T18:00:00.000+0000");

    // time will be truncated in this case because Joda doesn't process long form timezone name well
    Assert.assertEquals(DateTimeUtils.parse("2020-01-01 10:00:00America/Los_Angeles")
        .withZone(DateTimeZone.UTC)
        .toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ"), "2020-01-01T08:00:00.000+0000");

    // time will be truncated in this case because of unrecognizable format
    Assert.assertEquals(
        DateTimeUtils.parse("2020-01-01 10:00").withZone(DateTimeZone.UTC).toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
        "2020-01-01T08:00:00.000+0000");
  }

  /**
   *  Timezone: "America/Los_Angeles will be used if the timezone parameter is empty
   */
  @Test
  public void parse_emptyTimezone_defaultTimezone() {
    Assert.assertEquals(
        DateTimeUtils.parse("2020-01-01 10:00", "").withZone(DateTimeZone.UTC).toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
        "2020-01-01T08:00:00.000+0000");

    Assert.assertEquals(DateTimeUtils.parse("2020-01-01 10:00:30", "")
        .withZone(DateTimeZone.UTC)
        .toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ"), "2020-01-01T18:00:30.000+0000");

    Assert.assertEquals(DateTimeUtils.parse("2020-01-01 10:00:00-07:00", "")
        .withZone(DateTimeZone.UTC)
        .toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ"), "2020-01-01T17:00:00.000+0000");
  }

  /**
   * Exception is expected when dtString is null
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void parse_nulldtString_illegalArgumentException() {
    DateTimeUtils.parse(null, "");
  }

  /**
   * Exception is expected when timezone is null
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void parse_nullTimezone_illegalArgumentException() {
    DateTimeUtils.parse("", null);
  }
}
