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

package gobblin.data.management.retention.version.finder;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import gobblin.data.management.retention.dataset.Dataset;
import gobblin.data.management.retention.version.DatasetVersion;
import gobblin.data.management.retention.version.TimestampedDatasetVersion;
import gobblin.util.FileListUtils;


/**
 * {@link gobblin.data.management.retention.version.finder.VersionFinder} that uses the most nested file,
 * or directory if no file exists, level modifiedTimestamp under the datasetRoot path to find
 * {@link gobblin.data.management.retention.version.DatasetVersion}s, and represents each version as
 * {@link gobblin.data.management.retention.version.TimestampedDatasetVersion} using the file level path
 * and modifiedTimestamp.
 */
public class FileLevelTimestampVersionFinder implements VersionFinder<TimestampedDatasetVersion> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileLevelTimestampVersionFinder.class);
  private final FileSystem fs;

  public FileLevelTimestampVersionFinder(FileSystem fs, Properties props) {
    this.fs = fs;
  }

  @Override
  public Class<? extends DatasetVersion> versionClass() {
    return TimestampedDatasetVersion.class;
  }

  @Override
  public Collection<TimestampedDatasetVersion> findDatasetVersions(Dataset dataset) {
    try {
      List<TimestampedDatasetVersion> timestampedVersions = Lists.newArrayList();
      for (FileStatus fileStatus : FileListUtils.listMostNestedPathRecursively(this.fs, dataset.datasetRoot())) {
        timestampedVersions.add(new TimestampedDatasetVersion(new DateTime(fileStatus.getModificationTime()),
            fileStatus.getPath()));
      }
      return timestampedVersions;
    } catch (IOException e) {
      LOGGER.warn("Failed to get ModifiedTimeStamp for candidate dataset version at " + dataset.datasetRoot()
          + ". Ignoring.");
      return Lists.newArrayList();
    }
  }

}
