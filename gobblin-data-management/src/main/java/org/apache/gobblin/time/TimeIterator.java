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

import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * A {@link TimeIterator} iterates over time points within [{@code startTime}, {@code endTime}]. It
 * supports time points in various granularities (See {@link Granularity}
 */
public class TimeIterator implements Iterator {

  public enum Granularity {
    MINUTE, HOUR, DAY, MONTH
  }

  private ZonedDateTime startTime;
  private ZonedDateTime endTime;
  private Granularity granularity;

  public TimeIterator(ZonedDateTime startTime, ZonedDateTime endTime, Granularity granularity) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.granularity = granularity;
  }

  @Override
  public boolean hasNext() {
    return !startTime.isAfter(endTime);
  }

  @Override
  public ZonedDateTime next() {
    if (startTime.isAfter(endTime)) {
      throw new NoSuchElementException();
    }
    ZonedDateTime dateTime = startTime;
    startTime = inc(startTime, granularity, 1);
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
}
