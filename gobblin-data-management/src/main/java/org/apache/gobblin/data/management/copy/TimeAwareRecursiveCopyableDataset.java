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
  private final DateTimeZone dateTimeZone;
  private final LocalDateTime currentTime;

  public TimeAwareRecursiveCopyableDataset(FileSystem fs, Path rootPath, Properties properties, Path glob) {
    super(fs, rootPath, properties, glob);
    this.lookbackTime = properties.getProperty(LOOKBACK_TIME_KEY);
    PeriodFormatter periodFormatter = new PeriodFormatterBuilder().appendDays()
        .appendSuffix("d")
        .appendHours()
        .appendSuffix("h")
        .appendMinutes()
        .appendSuffix("m")
        .toFormatter();
    this.lookbackPeriod = periodFormatter.parsePeriod(lookbackTime);
    this.datePattern = properties.getProperty(DATE_PATTERN_KEY);
    this.dateTimeZone = DateTimeZone.forID(properties
        .getProperty(DATE_PATTERN_TIMEZONE_KEY, DEFAULT_DATE_PATTERN_TIMEZONE));
    this.currentTime = LocalDateTime.now(this.dateTimeZone);
    this.validateLookbackWithDatePatternFormat(this.datePattern, this.lookbackTime);
  }

  void validateLookbackWithDatePatternFormat(String datePattern, String lookbackTime) {
    DateTimeFormatter formatter = DateTimeFormat.forPattern(datePattern);
    LocalDateTime refDateTime = new LocalDateTime(2017, 01, 31, 10, 59, 59);
    String refDateTimeString = refDateTime.toString(formatter);
    PeriodFormatterBuilder formatterBuilder;

    // Validate that the lookback is supported for the time format
    if (!refDateTime.withSecondOfMinute(0).toString(formatter).equals(refDateTimeString)) {
      formatterBuilder = new PeriodFormatterBuilder().appendDays()
          .appendSuffix("d")
          .appendHours()
          .appendSuffix("h")
          .appendMinutes()
          .appendSuffix("m")
          .appendSeconds()
          .appendSuffix("s");
      if (!lookbackTimeMatchesFormat(formatterBuilder, lookbackTime)) {
        throw new IllegalArgumentException(String.format("Expected lookback time to be in daily or hourly or minutely or secondly format, check %s",
            LOOKBACK_TIME_KEY));
      }
    } else if (!refDateTime.withMinuteOfHour(0).toString(formatter).equals(refDateTimeString)) {
      formatterBuilder = new PeriodFormatterBuilder().appendDays()
          .appendSuffix("d")
          .appendHours()
          .appendSuffix("h")
          .appendMinutes()
          .appendSuffix("m");
      if (!lookbackTimeMatchesFormat(formatterBuilder, lookbackTime)) {
        throw new IllegalArgumentException(String.format("Expected lookback time to be in daily or hourly or minutely format, check %s",
            LOOKBACK_TIME_KEY));
      }
    } else if (!refDateTime.withHourOfDay(0).toString(formatter).equals(refDateTimeString)) {
      formatterBuilder = new PeriodFormatterBuilder().appendDays().appendSuffix("d").appendHours().appendSuffix("h");
      if (!lookbackTimeMatchesFormat(formatterBuilder, lookbackTime)) {
        throw new IllegalArgumentException(String.format("Expected lookback time to be in daily or hourly format, check %s", LOOKBACK_TIME_KEY));
      }
    } else if (!refDateTime.withDayOfMonth(1).toString(formatter).equals(refDateTimeString)) {
      formatterBuilder = new PeriodFormatterBuilder().appendDays().appendSuffix("d");
      if (!lookbackTimeMatchesFormat(formatterBuilder, lookbackTime)) {
        throw new IllegalArgumentException(String.format("Expected lookback time to be in daily format, check %s", LOOKBACK_TIME_KEY));
      }
    }
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
    LocalDateTime endDate = currentTime;
    DateTimeFormatter formatter = DateTimeFormat.forPattern(this.datePattern);
    LocalDateTime startDate = formatter.parseLocalDateTime(endDate.minus(this.lookbackPeriod).toString(this.datePattern));
    return recursivelyGetFilesAtDatePath(fs, path, "", fileFilter, 1, startDate, endDate, formatter);
  }

  /**
   * Checks if the datePath provided is in the range of the start and end dates.
   * Rounds startDate and endDate to the same granularity as datePath prior to comparing.
   * @param startDate
   * @param endDate
   * @param datePath
   * @param datePathFormat (This is the user set desired format)
   * @param level
   * @return true if the datePath provided is in the range of start and end dates, inclusive.
   */
  public static Boolean checkPathDateTimeValidity(LocalDateTime startDate, LocalDateTime endDate, String datePath,
      String datePathFormat, int level) {
    String [] datePathFormatArray = datePathFormat.split("/");
    String datePathPattern = String.join(FileSystems.getDefault().getSeparator(), Arrays.asList(datePathFormatArray).subList(0, level - 1));
    try {
      DateTimeFormatter formatGranularity = DateTimeFormat.forPattern(datePathPattern);
      LocalDateTime traversedDatePathRound = formatGranularity.parseLocalDateTime(datePath);
      LocalDateTime startDateRound = formatGranularity.parseLocalDateTime(startDate.toString(datePathPattern));
      LocalDateTime endDateRound = formatGranularity.parseLocalDateTime(endDate.toString(datePathPattern));
      return !traversedDatePathRound.isBefore(startDateRound) && !traversedDatePathRound.isAfter(endDateRound);
    } catch (IllegalArgumentException e) {
      log.error(String.format("Cannot parse path provided %s, expected in format of %s", datePath, datePathFormat));
      return false;
    }
  }

  private List<FileStatus> recursivelyGetFilesAtDatePath(FileSystem fs, Path path, String traversedDatePath, PathFilter fileFilter,
      int level,  LocalDateTime startDate, LocalDateTime endDate, DateTimeFormatter formatter) throws IOException {
    List<FileStatus> fileStatuses = Lists.newArrayList();
    if (!traversedDatePath.isEmpty()) {
      if (!checkPathDateTimeValidity(startDate, endDate, traversedDatePath, this.datePattern, level)) {
        return fileStatuses;
      }
    }
    Iterator<FileStatus> folderIterator;
    try {
      if (!fs.exists(path)) {
        return fileStatuses;
      }
      folderIterator = Arrays.asList(fs.listStatus(path)).iterator();
    } catch (Exception e) {
      log.warn(String.format("Error while listing paths at %s due to ", path), e);
      return fileStatuses;
    }
    // Check if at the lowest level/granularity of the date folder
    if (this.datePattern.split(FileSystems.getDefault().getSeparator()).length == level) {
      // Truncate the start date to the most granular unit of time in the datepattern
      while (folderIterator.hasNext()) {
        Path folderPath = folderIterator.next().getPath();
        String datePath = traversedDatePath.isEmpty() ? folderPath.getName() : new Path(traversedDatePath, folderPath.getName()).toString();
        try {
          LocalDateTime folderDate = formatter.parseLocalDateTime(datePath);
          if (!folderDate.isBefore(startDate) && !folderDate.isAfter(endDate)) {
            fileStatuses.addAll(super.getFilesAtPath(fs, folderPath, fileFilter));
          }
        } catch (IllegalArgumentException e) {
          log.warn(String.format(
              "Folder at path %s is not convertible to format %s. Please confirm that argument %s is valid", datePath,
              this.datePattern, DATE_PATTERN_KEY));
        }
      }
    } else {
      // folder has a format such as yyyy/mm/dd/hh, so recursively find date paths
      while (folderIterator.hasNext()) {
        // Start building the date from top-down
        String nextDate = folderIterator.next().getPath().getName();
        String datePath = traversedDatePath.isEmpty() ?  nextDate : new Path(traversedDatePath, nextDate).toString();
        fileStatuses.addAll(recursivelyGetFilesAtDatePath(fs, new Path(path, nextDate), datePath, fileFilter, level + 1, startDate, endDate, formatter));
      }
    }
    return fileStatuses;
  }
}
