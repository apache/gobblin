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

package org.apache.gobblin.data.management.copy;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.testng.Assert;

import com.google.common.collect.Lists;

import org.apache.gobblin.configuration.ConfigurationKeys;

public class TimeAwareRecursiveCopyableDataset extends RecursiveCopyableDataset {
  private static final String CONFIG_PREFIX = CopyConfiguration.COPY_PREFIX + ".recursive";
  public static final String DATE_PATTERN_KEY = CONFIG_PREFIX + ".date.pattern";
  public static final String LOOKBACK_TIME_KEY = CONFIG_PREFIX + ".lookback.time";
  public static final String DEFAULT_DATE_PATTERN_TIMEZONE = ConfigurationKeys.PST_TIMEZONE_NAME;
  public static final String DATE_PATTERN_TIMEZONE_KEY = CONFIG_PREFIX + ".datetime.timezone";

  private final String lookbackTime;
  private final String datePattern;
  private final Period lookbackPeriod;
  private final boolean isPatternDaily;
  private final boolean isPatternHourly;
  private final boolean isPatternMinutely;
  private final LocalDateTime currentTime;
  private final DatePattern pattern;

  enum DatePattern {
    MINUTELY, HOURLY, DAILY
  }

  public TimeAwareRecursiveCopyableDataset(FileSystem fs, Path rootPath, Properties properties, Path glob) {
    super(fs, rootPath, properties, glob);
    this.lookbackTime = properties.getProperty(LOOKBACK_TIME_KEY);
    PeriodFormatter periodFormatter = new PeriodFormatterBuilder().appendDays().appendSuffix("d").appendHours().appendSuffix("h").appendMinutes().appendSuffix("m").toFormatter();
    this.lookbackPeriod = periodFormatter.parsePeriod(lookbackTime);
    this.datePattern = properties.getProperty(DATE_PATTERN_KEY);
    this.isPatternMinutely = isDatePatternMinutely(datePattern);
    this.isPatternHourly = !this.isPatternMinutely && isDatePatternHourly(datePattern);
    this.isPatternDaily = !this.isPatternMinutely && !this.isPatternHourly;
    this.currentTime = properties.containsKey(DATE_PATTERN_TIMEZONE_KEY) ? LocalDateTime.now(
        DateTimeZone.forID(DATE_PATTERN_TIMEZONE_KEY))
        : LocalDateTime.now(DateTimeZone.forID(DEFAULT_DATE_PATTERN_TIMEZONE));

    if (this.isPatternDaily) {
      Preconditions.checkArgument(isLookbackTimeStringDaily(this.lookbackTime), "Expected day format for lookback time; found hourly or minutely format");
      pattern = DatePattern.DAILY;
    } else if (this.isPatternHourly) {
      Preconditions.checkArgument(isLookbackTimeStringHourly(this.lookbackTime), "Expected hourly format for lookback time; found minutely format");
      pattern = DatePattern.HOURLY;
    } else {
      pattern = DatePattern.MINUTELY;
    }
  }

  /**
   * TODO: Replace it with {@link org.apache.gobblin.time.TimeIterator} as {@link LocalDateTime} will not adjust time
   * to a given time zone
   */
  public static class DateRangeIterator implements Iterator {
    private LocalDateTime startDate;
    private LocalDateTime endDate;
    private DatePattern datePattern;

    public DateRangeIterator(LocalDateTime startDate, LocalDateTime endDate, DatePattern datePattern) {
      this.startDate = startDate;
      this.endDate = endDate;
      this.datePattern = datePattern;
    }

    @Override
    public boolean hasNext() {
      return !startDate.isAfter(endDate);
    }

    @Override
    public LocalDateTime next() {
      LocalDateTime dateTime = startDate;

      switch (datePattern) {
        case MINUTELY:
          startDate = startDate.plusMinutes(1);
          break;
        case HOURLY:
          startDate = startDate.plusHours(1);
          break;
        case DAILY:
          startDate = startDate.plusDays(1);
          break;
      }

      return dateTime;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private boolean isDatePatternHourly(String datePattern) {
    DateTimeFormatter formatter = DateTimeFormat.forPattern(datePattern);
    LocalDateTime refDateTime = new LocalDateTime(2017, 01, 01, 10, 0, 0);
    String refDateTimeString = refDateTime.toString(formatter);
    LocalDateTime refDateTimeAtStartOfDay = refDateTime.withHourOfDay(0);
    String refDateTimeStringAtStartOfDay = refDateTimeAtStartOfDay.toString(formatter);
    return !refDateTimeString.equals(refDateTimeStringAtStartOfDay);
  }

  private boolean isDatePatternMinutely(String datePattern) {
    DateTimeFormatter formatter = DateTimeFormat.forPattern(datePattern);
    LocalDateTime refDateTime = new LocalDateTime(2017, 01, 01, 10, 59, 0);
    String refDateTimeString = refDateTime.toString(formatter);
    LocalDateTime refDateTimeAtStartOfHour = refDateTime.withMinuteOfHour(0);
    String refDateTimeStringAtStartOfHour = refDateTimeAtStartOfHour.toString(formatter);
    return !refDateTimeString.equals(refDateTimeStringAtStartOfHour);
  }

  private boolean isLookbackTimeStringDaily(String lookbackTime) {
    PeriodFormatter periodFormatter = new PeriodFormatterBuilder().appendDays().appendSuffix("d").toFormatter();
    try {
      periodFormatter.parsePeriod(lookbackTime);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private boolean isLookbackTimeStringHourly(String lookbackTime) {
    PeriodFormatter periodFormatter = new PeriodFormatterBuilder().appendDays().appendSuffix("d").appendHours().appendSuffix("h").toFormatter();
    try {
      periodFormatter.parsePeriod(lookbackTime);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  protected List<FileStatus> getFilesAtPath(FileSystem fs, Path path, PathFilter fileFilter) throws IOException {
    DateTimeFormatter formatter = DateTimeFormat.forPattern(datePattern);
    LocalDateTime endDate = currentTime;
    LocalDateTime startDate = endDate.minus(this.lookbackPeriod);

    DateRangeIterator dateRangeIterator = new DateRangeIterator(startDate, endDate, pattern);
    List<FileStatus> fileStatuses = Lists.newArrayList();
    while (dateRangeIterator.hasNext()) {
      Path pathWithDateTime = new Path(path, dateRangeIterator.next().toString(formatter));
      fileStatuses.addAll(super.getFilesAtPath(fs, pathWithDateTime, fileFilter));
    }
    return fileStatuses;
  }
}
