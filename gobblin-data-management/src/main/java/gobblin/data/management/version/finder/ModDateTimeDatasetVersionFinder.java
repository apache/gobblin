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

package gobblin.data.management.version.finder;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.joda.time.DateTime;

import com.google.common.collect.Lists;

import gobblin.dataset.Dataset;
import gobblin.dataset.FileSystemDataset;
import gobblin.data.management.version.FileSystemDatasetVersion;
import gobblin.data.management.version.TimestampedDatasetVersion;


/**
 * {@link VersionFinder} for datasets based on modification timestamps.
 */
public class ModDateTimeDatasetVersionFinder implements VersionFinder<TimestampedDatasetVersion> {

  private final FileSystem fs;

  public ModDateTimeDatasetVersionFinder(FileSystem fs, Properties props) {
    this.fs = fs;
  }

  @Override
  public Class<? extends FileSystemDatasetVersion> versionClass() {
    return TimestampedDatasetVersion.class;
  }

  @Override
  public Collection<TimestampedDatasetVersion> findDatasetVersions(Dataset dataset) throws IOException {
    FileSystemDataset fsDataset = (FileSystemDataset) dataset;
    FileStatus status = this.fs.getFileStatus(fsDataset.datasetRoot());
    return Lists.newArrayList(new TimestampedDatasetVersion(new DateTime(status.getModificationTime()), fsDataset
        .datasetRoot()));
  }
}
