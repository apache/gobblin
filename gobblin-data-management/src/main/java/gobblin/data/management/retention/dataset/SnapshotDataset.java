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

import gobblin.data.management.retention.policy.NewestKRetentionPolicy;
import gobblin.data.management.retention.policy.RetentionPolicy;
import gobblin.data.management.retention.version.DatasetVersion;
import gobblin.data.management.retention.version.StringDatasetVersion;
import gobblin.data.management.retention.version.finder.VersionFinder;
import gobblin.data.management.retention.version.finder.WatermarkDatasetVersionFinder;


/**
 * {@link gobblin.data.management.retention.dataset.DatasetBase} for snapshot datasets.
 *
 * Uses a {@link gobblin.data.management.retention.version.finder.WatermarkDatasetVersionFinder} and a
 * {@link gobblin.data.management.retention.policy.NewestKRetentionPolicy}.
 */
public class SnapshotDataset extends DatasetBase<DatasetVersion> {

  private final VersionFinder<StringDatasetVersion> versionFinder;
  private final RetentionPolicy<DatasetVersion> retentionPolicy;
  private final Path datasetRoot;

  public SnapshotDataset(FileSystem fs, Properties props, Path datasetRoot) throws IOException {
    this(fs, props, datasetRoot, LoggerFactory.getLogger(SnapshotDataset.class));
  }

  public SnapshotDataset(FileSystem fs, Properties props, Path datasetRoot, Logger log)
      throws IOException {
    super(fs, props, log);
    this.datasetRoot = datasetRoot;
    this.versionFinder = new WatermarkDatasetVersionFinder(fs, props);
    this.retentionPolicy = new NewestKRetentionPolicy(props);
  }

  @Override
  public VersionFinder<? extends DatasetVersion> getVersionFinder() {
    return this.versionFinder;
  }

  @Override
  public RetentionPolicy<DatasetVersion> getRetentionPolicy() {
    return this.retentionPolicy;
  }

  @Override
  public Path datasetRoot() {
    return this.datasetRoot;
  }
}
