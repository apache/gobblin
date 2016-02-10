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

package gobblin.data.management.retention.policy;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;

import gobblin.annotation.Alpha;
import gobblin.data.management.retention.version.DatasetVersion;
import gobblin.util.FileListUtils;


/**
 * An abstract {@link RetentionPolicy} for {@link gobblin.data.management.retention.dataset.RawDataset}.
 *
 * This class embeds another {@link RetentionPolicy}. In {@link #listDeletableVersions(List)} it applies the
 * embedded {@link RetentionPolicy}'s predicate, as well as {@link #listQualifiedRawDatasetVersions(Collection)}.
 */
@Alpha
public abstract class RawDatasetRetentionPolicy implements RetentionPolicy<DatasetVersion> {

  private final FileSystem fs;
  private final Class<? extends DatasetVersion> versionClass;
  private final RetentionPolicy<DatasetVersion> embeddedRetentionPolicy;

  public RawDatasetRetentionPolicy(FileSystem fs, Class<? extends DatasetVersion> versionClass,
      RetentionPolicy<DatasetVersion> retentionPolicy) {
    this.fs = fs;
    this.versionClass = versionClass;
    this.embeddedRetentionPolicy = retentionPolicy;
  }

  @Override
  public Class<? extends DatasetVersion> versionClass() {
    return this.versionClass;
  }

  @Override
  public Collection<DatasetVersion> listDeletableVersions(List<DatasetVersion> allVersions) {
    Collection<DatasetVersion> deletableVersions = this.embeddedRetentionPolicy.listDeletableVersions(allVersions);
    return listQualifiedRawDatasetVersions(deletableVersions);
  }

  /**
   * A raw dataset version is qualified to be deleted, iff the corresponding refined paths exist, and the latest
   * mod time of all files is in the raw dataset is earlier than the latest mod time of all files in the refined paths.
   */
  protected Collection<DatasetVersion> listQualifiedRawDatasetVersions(Collection<DatasetVersion> allVersions) {
    return Lists.newArrayList(Collections2.filter(allVersions, new Predicate<DatasetVersion>() {
      @Override
      public boolean apply(DatasetVersion version) {
        Iterable<Path> refinedDatasetPaths = getRefinedDatasetPaths(version);
        try {
          Optional<Long> latestRawDatasetModTime = getLatestModTime(version.getPathsToDelete());
          Optional<Long> latestRefinedDatasetModTime = getLatestModTime(refinedDatasetPaths);
          return latestRawDatasetModTime.isPresent() && latestRefinedDatasetModTime.isPresent()
              && latestRawDatasetModTime.get() <= latestRefinedDatasetModTime.get();
        } catch (IOException e) {
          throw new RuntimeException("Failed to get modification time", e);
        }
      }
    }));
  }

  private Optional<Long> getLatestModTime(Iterable<Path> paths) throws IOException {
    long latestModTime = Long.MIN_VALUE;
    for (FileStatus status : FileListUtils.listMostNestedPathRecursively(this.fs, paths)) {
      latestModTime = Math.max(latestModTime, status.getModificationTime());
    }
    return latestModTime == Long.MIN_VALUE ? Optional.<Long> absent() : Optional.of(latestModTime);
  }

  /**
   * Get the corresponding refined paths for a raw dataset version. For example, a raw dataset version
   * can be a file containing un-deduplicated records, whose corresponding refined dataset path is a file
   * containing the corresponding deduplicated records.
   */
  protected abstract Iterable<Path> getRefinedDatasetPaths(DatasetVersion version);
}
