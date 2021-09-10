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

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.NoSuchElementException;

import lombok.Getter;


/**
 * A {@link TimeIterator} iterates over time points within [{@code startTime}, {@code endTime}]. It
 * supports time points in various granularities (See {@link Granularity}
 */
public class TimeIterator implements Iterator {

  public enum Granularity {
    MINUTE, HOUR, DAY, MONTH
  }

  @Getter
  private ZonedDateTime startTime;
  private ZonedDateTime endTime;
  private Granularity granularity;
  private boolean reverse;

  public TimeIterator(ZonedDateTime startTime, ZonedDateTime endTime, Granularity granularity) {
    this(startTime, endTime, granularity, false);
  }

  public TimeIterator(ZonedDateTime startTime, ZonedDateTime endTime, Granularity granularity, boolean reverse) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.granularity = granularity;
    this.reverse = reverse;
  }

  @Override
  public boolean hasNext() {
    return (reverse) ? !endTime.isAfter(startTime) : !startTime.isAfter(endTime);
  }

  @Override
  public ZonedDateTime next() {
    if ((!reverse && startTime.isAfter(endTime) || (reverse && endTime.isAfter(startTime)))) {
      throw new NoSuchElementException();
    }
    ZonedDateTime dateTime = startTime;
    startTime = (reverse) ? dec(startTime, granularity, 1) : inc(startTime, granularity, 1);
    return dateTime;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  /**
   * Increase the given time by {@code units}, which must be positive, of {@code granularity}
   */
  public static ZonedDateTime inc(ZonedDateTime time, Granularity granularity, long units) {
    switch (granularity) {
      case MINUTE:
        return time.plusMinutes(units);
      case HOUR:
        return time.plusHours(units);
      case DAY:
        return time.plusDays(units);
      case MONTH:
        return time.plusMonths(units);
    }
    throw new RuntimeException("Unsupported granularity: " + granularity);
  }

  /**
   * Decrease the given time by {@code units}, which must be positive, of {@code granularity}
   */
  public static ZonedDateTime dec(ZonedDateTime time, Granularity granularity, long units) {
    switch (granularity) {
      case MINUTE:
        return time.minusMinutes(units);
      case HOUR:
        return time.minusHours(units);
      case DAY:
        return time.minusDays(units);
      case MONTH:
        return time.minusMonths(units);
    }
    throw new RuntimeException("Unsupported granularity: " + granularity);
  }

  /**
   * Return duration as long between 2 datetime objects based on granularity
   * @param d1
   * @param d2
   * @param granularity
   * @return a long representing the duration
   */
  public static long durationBetween(ZonedDateTime d1, ZonedDateTime d2, Granularity granularity) {
    switch (granularity) {
      case HOUR:
        return Duration.between(d1, d2).toHours();
      case MINUTE:
        return Duration.between(d1, d2).toMinutes();
      case DAY:
        return Duration.between(d1, d2).toDays();
      case MONTH:
        return ChronoUnit.MONTHS.between(d1, d2);
    }
    throw new RuntimeException("Unsupported granularity: " + granularity);
  }

}
