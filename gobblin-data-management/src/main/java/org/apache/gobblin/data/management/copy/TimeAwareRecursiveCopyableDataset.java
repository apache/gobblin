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
  private final boolean isPatternHourly;
  private final LocalDateTime currentTime;

  public TimeAwareRecursiveCopyableDataset(FileSystem fs, Path rootPath, Properties properties, Path glob) {
    super(fs, rootPath, properties, glob);
    this.lookbackTime = properties.getProperty(LOOKBACK_TIME_KEY);
    PeriodFormatter periodFormatter = new PeriodFormatterBuilder().appendDays().appendSuffix("d").appendHours().appendSuffix("h").toFormatter();
    this.lookbackPeriod = periodFormatter.parsePeriod(lookbackTime);
    this.datePattern = properties.getProperty(DATE_PATTERN_KEY);
    this.isPatternHourly = isDatePatternHourly(datePattern);
    this.currentTime = properties.containsKey(DATE_PATTERN_TIMEZONE_KEY) ? LocalDateTime.now(
        DateTimeZone.forID(DATE_PATTERN_TIMEZONE_KEY))
        : LocalDateTime.now(DateTimeZone.forID(DEFAULT_DATE_PATTERN_TIMEZONE));

    //Daily directories cannot have a "hourly" lookback pattern. But hourly directories can accept lookback pattern with days.
    if (!this.isPatternHourly) {
      Assert.assertTrue(isLookbackTimeStringDaily(this.lookbackTime), "Expected day format for lookback time; found hourly format");
    }
  }

  public static class DateRangeIterator implements Iterator {
    private LocalDateTime startDate;
    private LocalDateTime endDate;
    private boolean isDatePatternHourly;

    public DateRangeIterator(LocalDateTime startDate, LocalDateTime endDate, boolean isDatePatternHourly) {
      this.startDate = startDate;
      this.endDate = endDate;
      this.isDatePatternHourly = isDatePatternHourly;
    }

    @Override
    public boolean hasNext() {
      return !startDate.isAfter(endDate);
    }

    @Override
    public LocalDateTime next() {
      LocalDateTime dateTime = startDate;
      startDate = this.isDatePatternHourly ? startDate.plusHours(1) : startDate.plusDays(1);
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

  private boolean isLookbackTimeStringDaily(String lookbackTime) {
    PeriodFormatter periodFormatter = new PeriodFormatterBuilder().appendDays().appendSuffix("d").toFormatter();
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

    DateRangeIterator dateRangeIterator = new DateRangeIterator(startDate, endDate, isPatternHourly);
    List<FileStatus> fileStatuses = Lists.newArrayList();
    while (dateRangeIterator.hasNext()) {
      Path pathWithDateTime = new Path(path, dateRangeIterator.next().toString(formatter));
      fileStatuses.addAll(super.getFilesAtPath(fs, pathWithDateTime, fileFilter));
    }
    return fileStatuses;
  }
}
