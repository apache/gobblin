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

import java.util.List;
import java.util.Map;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import org.apache.gobblin.configuration.ConfigurationKeys;
import lombok.AllArgsConstructor;

import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Utility class to check if a given time is in a pre-defined range. Particularly useful for Job-Schedulers such as
 * Azkaban that don't provide a day level scheduling granularity.
 */
public class TimeRangeChecker {

  private static final DateTimeZone DATE_TIME_ZONE = DateTimeZone.forID(ConfigurationKeys.PST_TIMEZONE_NAME);
  private static final String HOUR_MINUTE_FORMAT = "HH-mm";
  private static final DateTimeFormatter HOUR_MINUTE_FORMATTER = DateTimeFormat.forPattern("HH-mm");
  private static final Map<Integer, String> DAYS_OF_WEEK = new ImmutableMap.Builder<Integer, String>()
                                                               .put(DateTimeConstants.MONDAY, "MONDAY")
                                                               .put(DateTimeConstants.TUESDAY, "TUESDAY")
                                                               .put(DateTimeConstants.WEDNESDAY, "WEDNESDAY")
                                                               .put(DateTimeConstants.THURSDAY, "THURSDAY")
                                                               .put(DateTimeConstants.FRIDAY, "FRIDAY")
                                                               .put(DateTimeConstants.SATURDAY, "SATURDAY")
                                                               .put(DateTimeConstants.SUNDAY, "SUNDAY").build();

  /**
   * Checks if a specified time is on a day that is specified the a given {@link List} of acceptable days, and that the
   * hours + minutes of the specified time fall into a range defined by startTimeStr and endTimeStr.
   *
   * @param days is a {@link List} of days, if the specified {@link DateTime} does not have a day that falls is in this
   * {@link List} then this method will return false.
   * @param startTimeStr defines the start range that the currentTime can fall into. This {@link String} should be of
   * the pattern defined by {@link #HOUR_MINUTE_FORMAT}.
   * @param endTimeStr defines the start range that the currentTime can fall into. This {@link String} should be of
   * the pattern defined by {@link #HOUR_MINUTE_FORMAT}.
   * @param currentTime is a {@link DateTime} for which this method will check if it is in the given {@link List} of
   * days and falls into the time range defined by startTimeStr and endTimeStr.
   *
   * @return true if the given time is in the defined range, false otherwise.
   */
  public static boolean isTimeInRange(List<String> days, String startTimeStr, String endTimeStr, DateTime currentTime) {

    if (!Iterables.any(days, new AreDaysEqual(DAYS_OF_WEEK.get(currentTime.getDayOfWeek())))) {
      return false;
    }

    DateTime startTime = null;
    DateTime endTime = null;

    try {
      startTime = HOUR_MINUTE_FORMATTER.withZone(DATE_TIME_ZONE).parseDateTime(startTimeStr);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("startTimeStr format is invalid, must be of format " + HOUR_MINUTE_FORMAT, e);
    }

    try {
      endTime = HOUR_MINUTE_FORMATTER.withZone(DATE_TIME_ZONE).parseDateTime(endTimeStr);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("endTimeStr format is invalid, must be of format " + HOUR_MINUTE_FORMAT, e);
    }

    startTime = startTime.withDate(currentTime.getYear(), currentTime.getMonthOfYear(), currentTime.getDayOfMonth());
    endTime = endTime.withDate(currentTime.getYear(), currentTime.getMonthOfYear(), currentTime.getDayOfMonth());

    Interval interval = new Interval(startTime.getMillis(), endTime.getMillis(), DATE_TIME_ZONE);
    return interval.contains(currentTime.getMillis());
  }

  @AllArgsConstructor
  private static class AreDaysEqual implements Predicate<String> {

    private String day;

    @Override
    public boolean apply(String day) {
      return this.day.equalsIgnoreCase(day);
    }
  }
}
