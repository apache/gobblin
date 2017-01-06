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

package gobblin.data.management.retention.dataset;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.data.management.retention.policy.RetentionPolicy;
import gobblin.data.management.retention.policy.TimeBasedRetentionPolicy;
import gobblin.data.management.version.TimestampedDatasetVersion;
import gobblin.data.management.version.finder.ModDateTimeDatasetVersionFinder;
import gobblin.data.management.version.finder.VersionFinder;


/**
 * {@link CleanableDatasetBase} for a modification time based dataset.
 *
 * Uses a {@link ModDateTimeDatasetVersionFinder} and a {@link TimeBasedRetentionPolicy}.
 */
public class ModificationTimeDataset extends CleanableDatasetBase<TimestampedDatasetVersion> {

  private final VersionFinder<TimestampedDatasetVersion> versionFinder;
  private final RetentionPolicy<TimestampedDatasetVersion> retentionPolicy;
  private final Path datasetRoot;

  public ModificationTimeDataset(FileSystem fs, Properties props, Path datasetRoot) throws IOException {
    this(fs, props, datasetRoot, LoggerFactory.getLogger(ModificationTimeDataset.class));
  }

  public ModificationTimeDataset(FileSystem fs, Properties props, Path datasetRoot, Logger log) throws IOException {
    super(fs, props, log);
    this.versionFinder = new ModDateTimeDatasetVersionFinder(fs, props);
    this.retentionPolicy = new TimeBasedRetentionPolicy(props);
    this.datasetRoot = datasetRoot;
  }

  @Override
  public Path datasetRoot() {
    return this.datasetRoot;
  }

  @Override
  public VersionFinder<? extends TimestampedDatasetVersion> getVersionFinder() {
    return this.versionFinder;
  }

  @Override
  public RetentionPolicy<TimestampedDatasetVersion> getRetentionPolicy() {
    return this.retentionPolicy;
  }

}
