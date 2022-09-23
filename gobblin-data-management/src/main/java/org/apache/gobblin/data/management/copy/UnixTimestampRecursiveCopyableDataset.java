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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import com.google.common.collect.Lists;

import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.filters.AndPathFilter;


/**
 * This dataset filters file paths based on a {@link #timestampPattern} and {@link #versionSelectionPolicy}
 *
 * The default regex will match the first occurrence of a directory matching the pattern after the dataset root
 *
 */
public class UnixTimestampRecursiveCopyableDataset extends RecursiveCopyableDataset {

  private static final String CONFIG_PREFIX = CopyConfiguration.COPY_PREFIX + ".recursive";
  public static final String VERSION_SELECTION_POLICY = CONFIG_PREFIX + ".version.selection.policy";
  public static final String TIMESTAMP_REGEEX = CONFIG_PREFIX + ".timestamp.pattern";
  public static final String DEFAULT_TIMESTAMP_REGEX = ".*/([0-9]{13}).*/.*";
  private final String lookbackTime;
  private final Period lookbackPeriod;
  private final LocalDateTime currentTime;
  private final VersionSelectionPolicy versionSelectionPolicy;
  private final DateTimeZone dateTimeZone;
  private final Pattern timestampPattern;

  public UnixTimestampRecursiveCopyableDataset(FileSystem fs, Path rootPath, Properties properties, Path glob) {
    super(fs, rootPath, properties, glob);
    this.lookbackTime = properties.getProperty(TimeAwareRecursiveCopyableDataset.LOOKBACK_TIME_KEY);
    this.versionSelectionPolicy =
        VersionSelectionPolicy.valueOf(properties.getProperty(VERSION_SELECTION_POLICY).toUpperCase());
    PeriodFormatter periodFormatter = new PeriodFormatterBuilder().appendDays().appendSuffix("d").toFormatter();
    this.lookbackPeriod = periodFormatter.parsePeriod(lookbackTime);
    String timestampRegex = properties.getProperty(TIMESTAMP_REGEEX, DEFAULT_TIMESTAMP_REGEX);
    this.timestampPattern = Pattern.compile(timestampRegex);
    this.dateTimeZone = DateTimeZone.forID(properties
        .getProperty(TimeAwareRecursiveCopyableDataset.DATE_PATTERN_TIMEZONE_KEY,
            TimeAwareRecursiveCopyableDataset.DEFAULT_DATE_PATTERN_TIMEZONE));
    this.currentTime = LocalDateTime.now(this.dateTimeZone);
  }

  private enum VersionSelectionPolicy {
    EARLIEST, LATEST, ALL
  }

  /**
   * Given a lookback period, this filter extracts the timestamp from the path
   * based on {@link #timestampPattern} and filters out the paths that are out the date range
   *
   */
  class TimestampPathFilter implements PathFilter {

    @Override
    public boolean accept(Path path) {

      LocalDate endDate = currentTime.toLocalDate();
      LocalDate startDate = endDate.minus(lookbackPeriod);
      Path relativePath = PathUtils.relativizePath(PathUtils.getPathWithoutSchemeAndAuthority(path), datasetRoot());
      Matcher matcher = timestampPattern.matcher(relativePath.toString());
      if (!matcher.matches()) {
        return false;
      }
      Long timestamp = Long.parseLong(matcher.group(1));
      LocalDate dateOfTimestamp = new LocalDateTime(timestamp, dateTimeZone).toLocalDate();
      return !(dateOfTimestamp == null || dateOfTimestamp.isAfter(endDate) || dateOfTimestamp.isEqual(startDate)
          || dateOfTimestamp.isBefore(startDate));
    }
  }

  @Override
  protected List<FileStatus> getFilesAtPath(FileSystem fs, Path path, PathFilter fileFilter)
      throws IOException {

    // Filter files by lookback period (fileNames >= startDate and fileNames <= endDate)
    PathFilter andPathFilter = new AndPathFilter(fileFilter, new TimestampPathFilter());
    List<FileStatus> files = super.getFilesAtPath(fs, path, andPathFilter);

    if (VersionSelectionPolicy.ALL == versionSelectionPolicy) {
      return files;
    }

    Map<Pair<String, LocalDate>, TreeMap<Long, List<FileStatus>>> pathTimestampFilesMap = new HashMap<>();
    // Now select files per day based on version selection policy
    for (FileStatus fileStatus : files) {
      String relativePath = PathUtils.relativizePath(PathUtils.getPathWithoutSchemeAndAuthority(fileStatus.getPath()), datasetRoot()).toString();
      Matcher matcher = timestampPattern.matcher(relativePath);
      if (!matcher.matches()) {
        continue;
      }
      String timestampStr = matcher.group(1);
      String rootPath = relativePath.substring(0, relativePath.indexOf(timestampStr));
      Long unixTimestamp = Long.parseLong(timestampStr);
      LocalDate localDate = new LocalDateTime(unixTimestamp,dateTimeZone).toLocalDate();
      Pair<String, LocalDate> key = new ImmutablePair<>(rootPath, localDate);
      if (!pathTimestampFilesMap.containsKey(key)) {
        pathTimestampFilesMap.put(key, new TreeMap<Long, List<FileStatus>>());
      }
      Map<Long, List<FileStatus>> timestampFilesMap = pathTimestampFilesMap.get(key);

      if (!timestampFilesMap.containsKey(unixTimestamp)) {
        timestampFilesMap.put(unixTimestamp, Lists.newArrayList());
      }
      List<FileStatus> filesStatuses = timestampFilesMap.get(unixTimestamp);
      filesStatuses.add(fileStatus);

    }

    List<FileStatus> result = new ArrayList<>();
    for(TreeMap<Long, List<FileStatus>> timestampFileStatus : pathTimestampFilesMap.values()) {
      if(timestampFileStatus.size() <=0 ) {
        continue;
      }
      switch (versionSelectionPolicy) {
        case EARLIEST:
          result.addAll(timestampFileStatus.firstEntry().getValue());
          break;
        case LATEST:
          result.addAll(timestampFileStatus.lastEntry().getValue());
          break;
        default:
          throw new RuntimeException("Unsupported version selection policy");
      }
    }
    return result;
  }
}
