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
import java.nio.file.FileSystems;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
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

import com.google.common.collect.Lists;

import org.apache.gobblin.configuration.ConfigurationKeys;

@Slf4j
public class TimeAwareRecursiveCopyableDataset extends RecursiveCopyableDataset {
  private static final String CONFIG_PREFIX = CopyConfiguration.COPY_PREFIX + ".recursive";
  public static final String DATE_PATTERN_KEY = CONFIG_PREFIX + ".date.pattern";
  public static final String LOOKBACK_TIME_KEY = CONFIG_PREFIX + ".lookback.time";
  public static final String DEFAULT_DATE_PATTERN_TIMEZONE = ConfigurationKeys.PST_TIMEZONE_NAME;
  public static final String DATE_PATTERN_TIMEZONE_KEY = CONFIG_PREFIX + ".datetime.timezone";

  private final String lookbackTime;
  private final String datePattern;
  private final Period lookbackPeriod;
  private final LocalDateTime currentTime;
  private final DatePattern patternQualifier;

  enum DatePattern {
    SECONDLY, MINUTELY, HOURLY, DAILY
  }

  public TimeAwareRecursiveCopyableDataset(FileSystem fs, Path rootPath, Properties properties, Path glob) {
    super(fs, rootPath, properties, glob);
    this.lookbackTime = properties.getProperty(LOOKBACK_TIME_KEY);
    PeriodFormatter periodFormatter = new PeriodFormatterBuilder().appendDays().appendSuffix("d").appendHours().appendSuffix("h").appendMinutes().appendSuffix("m").toFormatter();
    this.lookbackPeriod = periodFormatter.parsePeriod(lookbackTime);
    this.datePattern = properties.getProperty(DATE_PATTERN_KEY);

    this.currentTime = properties.containsKey(DATE_PATTERN_TIMEZONE_KEY) ? LocalDateTime.now(
        DateTimeZone.forID(DATE_PATTERN_TIMEZONE_KEY))
        : LocalDateTime.now(DateTimeZone.forID(DEFAULT_DATE_PATTERN_TIMEZONE));

    this.patternQualifier = this.validateLookbackWithDatePatternFormat(this.datePattern, this.lookbackTime);
    if (this.patternQualifier == null) {
      throw new IllegalArgumentException("Could not validate date format with lookback time");
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
        case SECONDLY:
          startDate = startDate.plusSeconds(1);
          break;
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

  private DatePattern validateLookbackWithDatePatternFormat(String datePattern, String lookbackTime) {
    DateTimeFormatter formatter = DateTimeFormat.forPattern(datePattern);
    LocalDateTime refDateTime = new LocalDateTime(2017, 01, 31, 10, 59, 59);
    String refDateTimeString = refDateTime.toString(formatter);
    PeriodFormatterBuilder formatterBuilder;

    // Validate that the lookback is supported for the time format
    if (!refDateTime.withSecondOfMinute(0).toString(formatter).equals(refDateTimeString)) {
      formatterBuilder = new PeriodFormatterBuilder().appendDays().appendSuffix("d").appendHours().appendSuffix("h").appendMinutes().appendSuffix("m").appendSeconds().appendSuffix("s");
      if (!lookbackTimeMatchesFormat(formatterBuilder, lookbackTime)) {
        throw new IllegalArgumentException(String.format("Expected lookback time to be in daily or hourly or minutely or secondly format, check %s", LOOKBACK_TIME_KEY));
      }
      return DatePattern.SECONDLY;
    } else if (!refDateTime.withMinuteOfHour(0).toString(formatter).equals(refDateTimeString)) {
      formatterBuilder = new PeriodFormatterBuilder().appendDays().appendSuffix("d").appendHours().appendSuffix("h").appendMinutes().appendSuffix("m");
      if (!lookbackTimeMatchesFormat(formatterBuilder, lookbackTime)) {
        throw new IllegalArgumentException(String.format("Expected lookback time to be in daily or hourly or minutely format, check %s", LOOKBACK_TIME_KEY));
      }
      return DatePattern.MINUTELY;
    } else if (!refDateTime.withHourOfDay(0).toString(formatter).equals(refDateTimeString)) {
      formatterBuilder = new PeriodFormatterBuilder().appendDays().appendSuffix("d").appendHours().appendSuffix("h");
      if (!lookbackTimeMatchesFormat(formatterBuilder, lookbackTime)) {
        throw new IllegalArgumentException(String.format("Expected lookback time to be in daily or hourly format, check %s", LOOKBACK_TIME_KEY));
      }
      return DatePattern.HOURLY;
    } else if (!refDateTime.withDayOfMonth(1).toString(formatter).equals(refDateTimeString) ) {
      formatterBuilder = new PeriodFormatterBuilder().appendDays().appendSuffix("d");
      if (!lookbackTimeMatchesFormat(formatterBuilder, lookbackTime)) {
        throw new IllegalArgumentException(String.format("Expected lookback time to be in daily format, check %s", LOOKBACK_TIME_KEY));
      }
      return DatePattern.DAILY;
    }
    return null;
  }

  private boolean lookbackTimeMatchesFormat(PeriodFormatterBuilder formatterBuilder, String lookbackTime) {
    try {
      formatterBuilder.toFormatter().parsePeriod(lookbackTime);
    } catch (IllegalArgumentException e) {
      return false;
    }
    return true;
  }

  @Override
  protected List<FileStatus> getFilesAtPath(FileSystem fs, Path path, PathFilter fileFilter) throws IOException {
    DateTimeFormatter formatter = DateTimeFormat.forPattern(this.datePattern);
    LocalDateTime endDate = currentTime;
    LocalDateTime startDate = endDate.minus(this.lookbackPeriod);
    List<FileStatus> fileStatuses = Lists.newArrayList();

    // Data inside of nested folders representing timestamps need to be fetched differently
    if (datePattern.contains(FileSystems.getDefault().getSeparator())) {
      // Use an iterator that traverses through all times from lookback to current time, based on format
      DateRangeIterator dateRangeIterator = new DateRangeIterator(startDate, endDate, this.patternQualifier);
      while (dateRangeIterator.hasNext()) {
        Path pathWithDateTime = new Path(path, dateRangeIterator.next().toString(formatter));
        if (!fs.exists(pathWithDateTime)) {
          continue;
        }
        fileStatuses.addAll(super.getFilesAtPath(fs, pathWithDateTime, fileFilter));
      }
    } else {
      // Look at the top level directories and compare if those fit into the date format
      Iterator<FileStatus> folderIterator = Arrays.asList(fs.listStatus(path)).iterator();
      while (folderIterator.hasNext()) {
        Path folderPath = folderIterator.next().getPath();
        String datePath = folderPath.getName();
        try {
          LocalDateTime folderDate = formatter.parseLocalDateTime(datePath);
          if (folderDate.isAfter(startDate) && folderDate.isBefore(endDate)) {
            fileStatuses.addAll(super.getFilesAtPath(fs, folderPath, fileFilter));
          }
        } catch (IllegalArgumentException e) {
          log.warn(String.format("Folder at path %s is not convertible to format %s. Please confirm that argument %s is valid", datePath, this.datePattern, DATE_PATTERN_KEY));
        }
      }
    }
    return fileStatuses;
  }
}
