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

package org.apache.gobblin.data.management.version.finder;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import org.apache.gobblin.data.management.version.TimestampedDatasetVersion;
import org.apache.gobblin.dataset.Dataset;
import org.apache.gobblin.dataset.FileSystemDataset;
import org.apache.gobblin.util.ConfigUtils;


/**
 * {@link DatasetVersionFinder} that constructs {@link TimestampedDatasetVersion}s without actually checking for existence
 * of the version path. The version path is constructed by appending the version partition pattern to the dataset root.
 * The versions are found by looking back a specific period of time and finding unique date partitions between that
 * time and the current time. Lookback is supported to hourly granularity.
 */
public class LookbackDateTimeDatasetVersionFinder extends DateTimeDatasetVersionFinder {
  public static final String VERSION_PATH_PREFIX = "version.path.prefix";
  public static final String VERSION_LOOKBACK_PERIOD = "version.lookback.period";

  private final Duration stepDuration;
  private final Period lookbackPeriod;
  private final String pathPrefix;
  private final Instant endTime;

  public LookbackDateTimeDatasetVersionFinder(FileSystem fs, Config config) {
    this(fs, config, Instant.now());
  }

  @VisibleForTesting
  public LookbackDateTimeDatasetVersionFinder(FileSystem fs, Config config, Instant endTime) {
    super(fs, config);
    Preconditions.checkArgument(config.hasPath(VERSION_LOOKBACK_PERIOD) , "Missing required property " + VERSION_LOOKBACK_PERIOD);
    PeriodFormatter periodFormatter =
        new PeriodFormatterBuilder().appendYears().appendSuffix("y").appendMonths().appendSuffix("M").appendDays()
            .appendSuffix("d").appendHours().appendSuffix("h").toFormatter();
    this.stepDuration = Duration.standardHours(1);
    this.pathPrefix = ConfigUtils.getString(config, VERSION_PATH_PREFIX, "");
    this.lookbackPeriod = periodFormatter.parsePeriod(config.getString(VERSION_LOOKBACK_PERIOD));
    this.endTime = endTime;
  }

  @Override
  public Collection<TimestampedDatasetVersion> findDatasetVersions(Dataset dataset) throws IOException {
    FileSystemDataset fsDataset = (FileSystemDataset) dataset;
    Set<TimestampedDatasetVersion> versions = new HashSet<>();
    Instant startTime = endTime.minus(lookbackPeriod.toStandardDuration());

    for (Instant time = startTime; !time.isAfter(endTime); time = time.plus(stepDuration)) {
      String truncatedTime = formatter.print(time);
      DateTime versionTime = formatter.parseDateTime(truncatedTime);
      Path versionPath = new Path(fsDataset.datasetRoot(), new Path(pathPrefix, truncatedTime));
      versions.add(new TimestampedDatasetVersion(versionTime, versionPath));
    }

    return versions;
  }
}
