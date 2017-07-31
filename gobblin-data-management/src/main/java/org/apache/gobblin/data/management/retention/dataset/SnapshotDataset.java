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

import org.apache.gobblin.data.management.retention.policy.NewestKRetentionPolicy;
import org.apache.gobblin.data.management.retention.policy.RetentionPolicy;
import org.apache.gobblin.data.management.retention.version.StringDatasetVersion;
import org.apache.gobblin.data.management.retention.version.finder.VersionFinder;
import org.apache.gobblin.data.management.retention.version.finder.WatermarkDatasetVersionFinder;
import org.apache.gobblin.data.management.version.FileSystemDatasetVersion;


/**
 * {@link CleanableDatasetBase} for snapshot datasets.
 *
 * Uses a {@link org.apache.gobblin.data.management.retention.version.finder.WatermarkDatasetVersionFinder} and a
 * {@link org.apache.gobblin.data.management.retention.policy.NewestKRetentionPolicy}.
 */
public class SnapshotDataset extends CleanableDatasetBase<FileSystemDatasetVersion> {

  private final VersionFinder<StringDatasetVersion> versionFinder;
  private final RetentionPolicy<FileSystemDatasetVersion> retentionPolicy;
  private final Path datasetRoot;

  public SnapshotDataset(FileSystem fs, Properties props, Path datasetRoot) throws IOException {
    this(fs, props, datasetRoot, LoggerFactory.getLogger(SnapshotDataset.class));
  }

  public SnapshotDataset(FileSystem fs, Properties props, Path datasetRoot, Logger log)
      throws IOException {
    super(fs, props, log);
    this.datasetRoot = datasetRoot;
    this.versionFinder = new WatermarkDatasetVersionFinder(fs, props);
    this.retentionPolicy = new NewestKRetentionPolicy<FileSystemDatasetVersion>(props);
  }

  @Override
  public VersionFinder<? extends FileSystemDatasetVersion> getVersionFinder() {
    return this.versionFinder;
  }

  @Override
  public RetentionPolicy<FileSystemDatasetVersion> getRetentionPolicy() {
    return this.retentionPolicy;
  }

  @Override
  public Path datasetRoot() {
    return this.datasetRoot;
  }
}
