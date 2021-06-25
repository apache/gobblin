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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.Iterator;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;


/**
 * A {@link CopyableFileFilter} that drops a {@link CopyableFile} if file modification time not within the lookback
 * window
 *  <code>sourceFs<code>
 */
@Slf4j
public class DateRangeBasedFileFilter implements CopyableFileFilter {

  private Period minLookBackPeriod;
  private Period maxLookBackPeriod;
  private String timezone;
  private DateTime currentTime;
  private DateTime minLookBackTime;
  private DateTime maxLookBackTime;

  public static final String CONFIGURATION_KEY_PREFIX = "gobblin.dataset.filter.";
  public static final String DEFAULT_DATE_PATTERN_TIMEZONE = ConfigurationKeys.PST_TIMEZONE_NAME;

  public DateRangeBasedFileFilter(Period minLookback, Period maxLookback, String timezone) {
    this.minLookBackPeriod = minLookback;
    this.maxLookBackPeriod = maxLookback;
    this.timezone = timezone;
    this.currentTime = !Strings.isNullOrEmpty(this.timezone) ? DateTime.now(DateTimeZone.forID(this.timezone))
        : DateTime.now(DateTimeZone.forID(DEFAULT_DATE_PATTERN_TIMEZONE));
    this.minLookBackTime = this.currentTime.minus(this.minLookBackPeriod);
    this.maxLookBackTime = this.currentTime.minus(this.maxLookBackPeriod);
  }

  /**
   * For every {@link CopyableFile} in <code>copyableFiles</code> checks if a
   * {@link CopyableFile#getOrigin()#getPath()#getModificationTime()}
   * + date between the min and max look back window on <code>sourceFs</code> {@inheritDoc}
   *
   * @see CopyableFileFilter#filter(FileSystem,
   *      FileSystem, Collection)
   */
  @Override
  public Collection<CopyableFile> filter(FileSystem sourceFs, FileSystem targetFs,
      Collection<CopyableFile> copyableFiles) {
    Iterator<CopyableFile> iterator = copyableFiles.iterator();

    ImmutableList.Builder<CopyableFile> filtered = ImmutableList.builder();

    while (iterator.hasNext()) {
      CopyableFile file = iterator.next();
      if (isFileModifiedWithinLookBackPeriod(file.getOrigin().getModificationTime())) {
        filtered.add(file);
      }
    }

    return filtered.build();
  }

  /**
   *
   * @param modTime file modification time in long.
   * @return <code>true</code> if the file modification time within lookback window;
   *         <code>false</code> if file modification time not within lookback window.
   *
   */
  private boolean isFileModifiedWithinLookBackPeriod(long modTime) {
    DateTime modifiedTime =
        !Strings.isNullOrEmpty(this.timezone) ? new DateTime(modTime, DateTimeZone.forID(this.timezone))
            : new DateTime(modTime, DateTimeZone.forID(DEFAULT_DATE_PATTERN_TIMEZONE));
    if (modifiedTime.isAfter(this.maxLookBackTime.toDateTime()) && modifiedTime.isBefore(
        this.minLookBackTime.toDateTime())) {
      return true;
    }
    return false;
  }

  protected static PeriodFormatter getPeriodFormatter() {
    PeriodFormatter periodFormatter =
        new PeriodFormatterBuilder().appendDays().appendSuffix("d").appendHours().appendSuffix("h").toFormatter();
    return periodFormatter;
  }
}
