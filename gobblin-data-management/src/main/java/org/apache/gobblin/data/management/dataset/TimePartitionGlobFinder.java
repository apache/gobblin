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

package org.apache.gobblin.data.management.dataset;

import java.io.IOException;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.data.management.retention.profile.ConfigurableGlobDatasetFinder;
import org.apache.gobblin.dataset.DatasetsFinder;
import org.apache.gobblin.dataset.FileSystemDataset;
import org.apache.gobblin.time.TimeIterator;
import org.apache.gobblin.util.PathUtils;


/**
 * A {@link TimePartitionGlobFinder} finds all dataset time partitions within time window
 * [current time - look back time, current time]. It derives an efficient dataset partition pattern based
 * on the time window and a supported {@value #TIME_FORMAT}.
 *
 * <p> If {@value #ENABLE_VIRTUAL_PARTITION} is set, it will create virtual {@link SimpleFileSystemDataset}
 * instances if a partition within the time window doesn't exist
 */
@Slf4j
public class TimePartitionGlobFinder implements DatasetsFinder<FileSystemDataset> {
  private static final String CONF_PREFIX = "timePartitionGlobFinder.";

  public static final String PARTITION_PREFIX = CONF_PREFIX + "partitionPrefix";
  public static final String TIME_FORMAT = CONF_PREFIX + "timeFormat";
  public static final String ENABLE_VIRTUAL_PARTITION = CONF_PREFIX + "enableVirtualPartition";
  /**
   * Options are enumerated in {@link org.apache.gobblin.time.TimeIterator.Granularity}
   */
  public static final String GRANULARITY = CONF_PREFIX + "granularity";
  public static final String TIME_ZONE = CONF_PREFIX + "timeZone";
  public static final String LOOKBACK_SPEC = CONF_PREFIX + "lookbackSpec";

  private static final String DEFAULT_TIME_ZONE = "America/Los_Angeles";

  private static final Pattern SUPPORTED_TIME_FORMAT = Pattern.compile("(yyyy/MM(/.*)*)|(yyyy-MM(-.*)*)");

  private final String datasetPattern;
  private final String datasetPartitionPattern;
  private final String partitionPrefix;
  private final DateTimeFormatter timeFormatter;
  private final boolean enableVirtualPartition;

  private final ZonedDateTime startTime;
  private final ZonedDateTime endTime;
  private final TimeIterator.Granularity granularity;

  private final Properties props;
  private final FileSystem fs;

  public TimePartitionGlobFinder(FileSystem fs, Properties properties) {
    this(fs, properties,
        ZonedDateTime.now(ZoneId.of(properties.getProperty(TIME_ZONE, DEFAULT_TIME_ZONE))));
  }

  @VisibleForTesting
  TimePartitionGlobFinder(FileSystem fs, Properties properties, ZonedDateTime curTime) {
    datasetPattern = properties.getProperty(ConfigurableGlobDatasetFinder.DATASET_FINDER_PATTERN_KEY);
    Path datasetPath = new Path(datasetPattern);

    partitionPrefix = properties.getProperty(PARTITION_PREFIX, "");
    String timeFormat = properties.getProperty(TIME_FORMAT).trim();
    Preconditions.checkState(isTimeFormatSupported(timeFormat),
        String.format("Unsupported time format %s, expecting %s", timeFormat, SUPPORTED_TIME_FORMAT));
    timeFormatter = DateTimeFormatter.ofPattern(timeFormat);

    endTime = curTime;
    Duration lookback = Duration.parse(properties.getProperty(LOOKBACK_SPEC));
    startTime = endTime.minus(lookback);
    granularity = TimeIterator.Granularity.valueOf(properties.getProperty(GRANULARITY).toUpperCase());

    datasetPartitionPattern = new Path(datasetPath,
        partitionPrefix + derivePartitionPattern(startTime, endTime, timeFormat)).toString();
    log.info("Dataset partition pattern is {}", datasetPartitionPattern);

    enableVirtualPartition = Boolean.valueOf(properties.getProperty(ENABLE_VIRTUAL_PARTITION, "false"));

    props = properties;
    this.fs = fs;
  }

  /**
   * The finder supports time format matching {@link #SUPPORTED_TIME_FORMAT}
   */
  @VisibleForTesting
  static boolean isTimeFormatSupported(String timeFormat) {
    return SUPPORTED_TIME_FORMAT.matcher(timeFormat).matches();
  }

  /**
   * Derive partition glob pattern from time format. It tries its best to provide
   * a fine pattern by refining year and month options from reasoning
   * start time, end time and {@link #SUPPORTED_TIME_FORMAT}
   */
  @VisibleForTesting
  static String derivePartitionPattern(ZonedDateTime start,
      ZonedDateTime end, String timeFormat) {
    // Refine year options
    int startYear = start.getYear();
    int endYear = end.getYear();
    StringBuilder yearOptions = new StringBuilder("{" + startYear);
    appendOptions(yearOptions, startYear + 1, endYear);
    yearOptions.append("}");

    // Get month options
    StringBuilder monthOptions = buildMonthOptions(start, end);

    StringBuilder pattern = new StringBuilder(yearOptions);
    if (timeFormat.contains("-")) {
      pattern.append("-");
      pattern.append(monthOptions);
      //
      if (!monthOptions.toString().equals("*")) {
        pattern.append("*");
      }
    } else {
      pattern.append("/");
      pattern.append(monthOptions);
      String[] parts = timeFormat.split("/");
      // We already processed year and month components
      for (int i = 2; i < parts.length; i++) {
        pattern.append("/*");
      }
    }

    return pattern.toString();
  }

  /**
   * Refine month options
   */
  private static StringBuilder buildMonthOptions(ZonedDateTime start,
      ZonedDateTime end) {
    int startMonth = start.getMonthValue();
    int endMonth = end.getMonthValue();
    int yearDiff = end.getYear() - start.getYear();
    if ( yearDiff > 1 || (yearDiff == 1 && endMonth >= startMonth)) {
      // All 12 months
      return new StringBuilder("*");
    }
    StringBuilder monthOptions = new StringBuilder("{" + startMonth);
    if (endMonth >= startMonth) {
      appendOptions(monthOptions, startMonth + 1, endMonth);
    } else {
      // from [startMonth + 1, 12] of start year
      appendOptions(monthOptions, startMonth + 1, 12);
      // from [1, endMonth] of current year
      appendOptions(monthOptions, 1, endMonth);
    }
    monthOptions.append("}");
    return monthOptions;
  }

  private static void appendOptions(StringBuilder stringBuilder, int start, int end) {
    for (int i = start; i <= end; i++) {
      stringBuilder.append(",");
      if (i < 10) {
        stringBuilder.append("0");
      }
      stringBuilder.append(i);
    }
  }

  @Override
  public List<FileSystemDataset> findDatasets()
      throws IOException {
    try {
      return doFindDatasets();
    } finally {
      // Recover ConfigurableGlobDatasetFinder.DATASET_FINDER_PATTERN_KEY config
      this.props.setProperty(ConfigurableGlobDatasetFinder.DATASET_FINDER_PATTERN_KEY, datasetPattern);
    }
  }

  private List<FileSystemDataset> doFindDatasets() throws IOException {
    // Find datasets
    List<FileSystemDataset> datasets = findDatasets(datasetPattern);

    // Compute partitions in theory based on startTime and endTime
    Set<String> computedPartitions = new HashSet<>();
    datasets.forEach(dataset -> computedPartitions.addAll(computePartitions(dataset)));

    // This is the final result
    List<FileSystemDataset> resultPartitions = new ArrayList<>(computedPartitions.size());

    // Find all physical dataset time partitions
    List<FileSystemDataset> actualPartitions = findDatasets(datasetPartitionPattern);

    String pathStr;
    for (FileSystemDataset physicalPartition : actualPartitions) {
      pathStr = physicalPartition.datasetRoot().toString();
      if (computedPartitions.contains(pathStr)) {
        resultPartitions.add(physicalPartition);
        computedPartitions.remove(pathStr);
      }
    }

    // Create virtual ones;
    if (enableVirtualPartition) {
      computedPartitions.forEach(partition -> {
        log.info("Creating virtual partition {}", partition);
        resultPartitions.add(new SimpleFileSystemDataset(new Path(partition), true));
      });
    } else {
      log.info("Will not create virtual partitions");
    }

    return resultPartitions;
  }

  private Collection<String> computePartitions(FileSystemDataset dataset) {
    List<String> partitions = new ArrayList<>();
    TimeIterator iterator = new TimeIterator(startTime, endTime, granularity);
    while (iterator.hasNext()) {
      partitions.add(new Path(dataset.datasetRoot(),
          partitionPrefix + timeFormatter.format(iterator.next())).toString());
    }
    return partitions;
  }

  private List<FileSystemDataset> findDatasets(String pattern)
      throws IOException {
    this.props.setProperty(ConfigurableGlobDatasetFinder.DATASET_FINDER_PATTERN_KEY, pattern);
    DefaultFileSystemGlobFinder datasetFinder = new DefaultFileSystemGlobFinder(this.fs, this.props);
    return datasetFinder.findDatasets();
  }

  @Override
  public Path commonDatasetRoot() {
    return PathUtils.deepestNonGlobPath(new Path(this.datasetPattern));
  }
}
