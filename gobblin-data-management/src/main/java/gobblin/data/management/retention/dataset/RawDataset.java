/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
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

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.annotation.Alpha;
import gobblin.data.management.retention.policy.RawDatasetRetentionPolicy;
import gobblin.data.management.retention.policy.RetentionPolicy;
import gobblin.data.management.retention.version.DatasetVersion;
import gobblin.data.management.retention.version.finder.VersionFinder;


/**
 * {@link DatasetBase} for raw data.
 *
 * A raw dataset is a dataset with a corresponding refined dataset. A version of a raw dataset is deletable only if
 * it satisfies certain conditions with the corresponding version of the refined dataset. For example,
 * the modification time of the raw dataset should be earlier than the modification time of the corresponding
 * refined dataset.
 */
@Alpha
public class RawDataset extends DatasetBase<DatasetVersion> {

  public static final String DATASET_CLASS = "dataset.class";
  public static final String DATASET_RETENTION_POLICY_CLASS = "dataset.retention.policy.class";

  private final Path datasetRoot;
  private final DatasetBase<DatasetVersion> embeddedDataset;
  private final RawDatasetRetentionPolicy retentionPolicy;

  public RawDataset(FileSystem fs, Properties props, Path datasetRoot) throws IOException {
    this(fs, props, datasetRoot, LoggerFactory.getLogger(RawDataset.class));
  }

  public RawDataset(FileSystem fs, Properties props, Path datasetRoot, Logger log) throws IOException {
    super(fs, props, log);
    this.datasetRoot = datasetRoot;
    this.embeddedDataset = getDataset(fs, props, datasetRoot);
    this.retentionPolicy = getRetentionPolicy(fs);
  }

  private DatasetBase<DatasetVersion> getDataset(FileSystem fs, Properties props, Path datasetRoot) {
    try {
      @SuppressWarnings("unchecked")
      Class<? extends DatasetBase<DatasetVersion>> clazz =
          (Class<? extends DatasetBase<DatasetVersion>>) Class.forName(DATASET_CLASS);
      return ConstructorUtils.invokeConstructor(clazz, fs, props, datasetRoot);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Failed to instantiate dataset", e);
    }
  }

  private RawDatasetRetentionPolicy getRetentionPolicy(FileSystem fs) {
    try {
      @SuppressWarnings("unchecked")
      Class<? extends RawDatasetRetentionPolicy> clazz =
          (Class<? extends RawDatasetRetentionPolicy>) Class.forName(DATASET_RETENTION_POLICY_CLASS);
      return (RawDatasetRetentionPolicy) ConstructorUtils.invokeConstructor(clazz, fs,
          getVersionFinder().versionClass(), this.embeddedDataset.getRetentionPolicy());
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Failed to instantiate RetentionPolicy", e);
    }
  }

  @Override
  public VersionFinder<? extends DatasetVersion> getVersionFinder() {
    return this.embeddedDataset.getVersionFinder();
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
