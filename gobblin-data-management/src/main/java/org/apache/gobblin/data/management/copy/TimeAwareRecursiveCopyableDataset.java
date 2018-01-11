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

import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;


public class TimeAwareRecursiveCopyableDataset extends RecursiveCopyableDataset {
  private static final String CONFIG_PREFIX = CopyConfiguration.COPY_PREFIX + ".recursive";
  public static final String DATE_PATTERN_KEY = CONFIG_PREFIX + ".date.pattern";
  public static final String LOOKBACK_DAYS_KEY = CONFIG_PREFIX + ".lookback.days";

  private final Integer lookbackDays;
  private final String datePattern;

  public TimeAwareRecursiveCopyableDataset(FileSystem fs, Path rootPath, Properties properties, Path glob) {
    super(fs, rootPath, properties, glob);
    this.lookbackDays = Integer.parseInt(properties.getProperty(LOOKBACK_DAYS_KEY));
    this.datePattern = properties.getProperty(DATE_PATTERN_KEY);
  }

  public static class DateRangeIterator implements Iterator {
    private LocalDateTime startDate;
    private LocalDateTime endDate;
    private boolean isDatePatternHourly;

    public DateRangeIterator(LocalDateTime startDate, LocalDateTime endDate, String datePattern) {
      this.startDate = startDate;
      this.endDate = endDate;
      this.isDatePatternHourly = isDatePatternHourly(datePattern);
    }

    private boolean isDatePatternHourly(String datePattern) {
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern(datePattern);
      LocalDateTime refDateTime = LocalDateTime.of(2017, 01, 01, 10, 0, 0);
      String refDateTimeString = refDateTime.format(formatter);
      LocalDateTime refDateTimeAtStartOfDay = refDateTime.withHour(0);
      String refDateTimeStringAtStartOfDay = refDateTimeAtStartOfDay.format(formatter);
      return !refDateTimeString.equals(refDateTimeStringAtStartOfDay);
    }

    @Override
    public boolean hasNext() {
      return !startDate.isAfter(endDate);
    }

    @Override
    public LocalDateTime next() {
      LocalDateTime dateTime = startDate;
      startDate = isDatePatternHourly ? startDate.plusHours(1) : startDate.plusDays(1);
      return dateTime;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  protected List<FileStatus> getFilesAtPath(FileSystem fs, Path path, PathFilter fileFilter) throws IOException {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(datePattern);
    LocalDateTime endDate = LocalDateTime.now();
    LocalDateTime startDate = endDate.minusDays(lookbackDays);
    DateRangeIterator dateRangeIterator = new DateRangeIterator(startDate, endDate, datePattern);
    List<FileStatus> fileStatuses = Lists.newArrayList();
    while (dateRangeIterator.hasNext()) {
      Path pathWithDateTime = new Path(path, dateRangeIterator.next().format(formatter));
      fileStatuses.addAll(super.getFilesAtPath(fs, pathWithDateTime, fileFilter));
    }
    return fileStatuses;
  }
}
