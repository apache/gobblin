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
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.data.management.retention.profile.ConfigurableGlobDatasetFinder;
import org.apache.gobblin.dataset.DatasetsFinder;
import org.apache.gobblin.dataset.FileSystemDataset;
import org.apache.gobblin.util.PathUtils;


@Slf4j
public class TimePartitionGlobFinder implements DatasetsFinder<FileSystemDataset> {
  private static final String CONF_PREFIX = "timePartitionGlobFinder.";

  public static final String PARTITION_PREFIX = CONF_PREFIX + "partitionPrefix";
  public static final String TIME_FORMAT = CONF_PREFIX + "timeFormat";
  public static final String TIME_ZONE = CONF_PREFIX + "timeZone";
  public static final String ENABLE_EMPTY_PARTITION = CONF_PREFIX + "enableEmptyPartition";

  private static final String DEFAULT_TIME_ZONE = "America/Los_Angeles";

  private final String datasetPattern;
  private final String datasetPartitionPattern;
  private final Path yesterdayPartition;

  private final Properties props;
  private final FileSystem fs;
  private final boolean enableEmptyPartition;

  public TimePartitionGlobFinder(FileSystem fs, Properties properties) {
    datasetPattern = properties.getProperty(ConfigurableGlobDatasetFinder.DATASET_FINDER_PATTERN_KEY);
    Path datasetPartitionPath = new Path(datasetPattern);

    String partitionPrefix = properties.getProperty(PARTITION_PREFIX, "");
    String timeFormat = properties.getProperty(TIME_FORMAT).trim();
    DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern(timeFormat);
    datasetPartitionPattern = new Path(datasetPartitionPath,
        partitionPrefix + derivePartitionPattern(timeFormat)).toString();
    log.info("Dataset partition pattern is {}", datasetPartitionPattern);

    ZonedDateTime curTime = ZonedDateTime.now(ZoneId.of(properties.getProperty(TIME_ZONE, DEFAULT_TIME_ZONE)));
    yesterdayPartition = new Path(partitionPrefix + timeFormatter.format(curTime.minusDays(1)));
    log.info("Yesterday partition is {}", yesterdayPartition);

    enableEmptyPartition = Boolean.valueOf(properties.getProperty(ENABLE_EMPTY_PARTITION));

    props = properties;
    this.fs = fs;
  }

  /**
   * Derive partition glob pattern from time format. For example:
   * <p> given {@code timePattern=yyyy/MM/dd}, return '*'/'*'/'*' (adding single quote to avoid breaking comments)
   * <p> given {@code timePattern=yyyy-MM-dd}, return *
   */
  private String derivePartitionPattern(String timeFormat) {
    String[] parts = timeFormat.split("/");
    StringBuilder pattern = new StringBuilder("*");
    for (int i = 1; i < parts.length; i++) {
      pattern.append("/*");
    }
    return pattern.toString();
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
    if (!enableEmptyPartition) {
      log.info("Will not create empty file system dataset");
      return findDatasets(datasetPartitionPattern);
    }

    // Find datasets
    List<FileSystemDataset> datasets = findDatasets(datasetPattern);
    // Identify yesterday dataset partitions
    Set<Path> yesterdayDatasetPartitions = new HashSet<>();
    datasets.forEach(dataset -> yesterdayDatasetPartitions.add(createYesterdayDatasetPartition(dataset)));

    // Find all dataset time partitions
    List<FileSystemDataset> datasetPartitions = findDatasets(datasetPartitionPattern);
    for (FileSystemDataset datasetPartition : datasetPartitions) {
      // Remove yesterday dataset partitions already created
      yesterdayDatasetPartitions.remove(datasetPartition.datasetRoot());
    }

    // Create the remaining ones
    yesterdayDatasetPartitions.forEach(path -> {
      log.info("Creating empty yesterday partition {}", path);
      datasetPartitions.add(new EmptyFileSystemDataset(path));
    });

    return datasetPartitions;
  }

  private List<FileSystemDataset> findDatasets(String pattern)
      throws IOException {
    this.props.setProperty(ConfigurableGlobDatasetFinder.DATASET_FINDER_PATTERN_KEY, pattern);
    DefaultFileSystemGlobFinder datasetFinder = new DefaultFileSystemGlobFinder(this.fs, this.props);
    return datasetFinder.findDatasets();
  }

  private Path createYesterdayDatasetPartition(FileSystemDataset dataset) {
    return new Path(dataset.datasetRoot(), yesterdayPartition);
  }

  @Override
  public Path commonDatasetRoot() {
    return PathUtils.deepestNonGlobPath(new Path(this.datasetPattern));
  }
}
