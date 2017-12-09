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

package org.apache.gobblin.data.management.retention.dataset;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.gobblin.data.management.retention.policy.RetentionPolicy;
import org.apache.gobblin.data.management.retention.policy.TimeBasedRetentionPolicy;
import org.apache.gobblin.data.management.version.TimestampedDatasetVersion;
import org.apache.gobblin.data.management.retention.version.finder.DateTimeDatasetVersionFinder;
import org.apache.gobblin.data.management.version.finder.VersionFinder;


/**
 * {@link CleanableDatasetBase} for tracking data.
 *
 * Uses a {@link org.apache.gobblin.data.management.retention.version.finder.DateTimeDatasetVersionFinder} and a
 * {@link org.apache.gobblin.data.management.retention.policy.TimeBasedRetentionPolicy}.
 */
public class TrackingDataset extends CleanableDatasetBase<TimestampedDatasetVersion> {

  private final VersionFinder<? extends TimestampedDatasetVersion> versionFinder;
  private final RetentionPolicy<TimestampedDatasetVersion> retentionPolicy;
  private final Path datasetRoot;

  public TrackingDataset(FileSystem fs, Properties props, Path datasetRoot)
      throws IOException {
    this(fs, props, datasetRoot, LoggerFactory.getLogger(TrackingDataset.class));
  }

  public TrackingDataset(FileSystem fs, Properties props, Path datasetRoot, Logger log)
      throws IOException {
    super(fs, props, log);
    this.datasetRoot = datasetRoot;
    // Use the deprecated version finder which uses deprecated keys
    this.versionFinder = new DateTimeDatasetVersionFinder(fs, props);
    this.retentionPolicy = new TimeBasedRetentionPolicy(props);
  }

  @Override
  public VersionFinder<? extends TimestampedDatasetVersion> getVersionFinder() {
    return this.versionFinder;
  }

  @Override
  public RetentionPolicy<TimestampedDatasetVersion> getRetentionPolicy() {
    return this.retentionPolicy;
  }

  @Override
  public Path datasetRoot() {
    return this.datasetRoot;
  }
}
