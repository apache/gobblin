/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.data.management.retention.dataset;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.data.management.retention.policy.RetentionPolicy;
import gobblin.data.management.retention.policy.TimeBasedRetentionPolicy;
import gobblin.data.management.retention.version.TimestampedDatasetVersion;
import gobblin.data.management.retention.version.finder.DateTimeDatasetVersionFinder;
import gobblin.data.management.retention.version.finder.VersionFinder;


/**
 * {@link gobblin.data.management.retention.dataset.DatasetBase} for tracking data.
 *
 * Uses a {@link gobblin.data.management.retention.version.finder.DateTimeDatasetVersionFinder} and a
 * {@link gobblin.data.management.retention.policy.TimeBasedRetentionPolicy}.
 */
public class TrackingDataset extends DatasetBase<TimestampedDatasetVersion> {

  private final VersionFinder<TimestampedDatasetVersion> versionFinder;
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
