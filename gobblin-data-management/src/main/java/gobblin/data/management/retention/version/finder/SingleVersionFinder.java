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
import java.util.Properties;

import com.google.common.collect.Lists;

import org.apache.hadoop.fs.FileSystem;

import lombok.Getter;

import gobblin.data.management.retention.dataset.Dataset;
import gobblin.data.management.retention.version.DatasetVersion;
import gobblin.data.management.retention.version.FileStatusDatasetVersion;
import gobblin.data.management.retention.version.StringDatasetVersion;

/**
 * Implementation of {@link VersionFinder} that uses a {@link StringDatasetVersion} and simply creates a single
 * {@link StringDatasetVersion} for the given {@link Dataset}.
 */
public class SingleVersionFinder implements VersionFinder<FileStatusDatasetVersion> {

  @Getter
  private FileSystem fs;

  public SingleVersionFinder(FileSystem fs, Properties props) {
    this.fs = fs;
  }

  @Override
  public Class<? extends DatasetVersion> versionClass() {
    return StringDatasetVersion.class;
  }

  @Override
  public Collection<FileStatusDatasetVersion> findDatasetVersions(Dataset dataset) throws IOException {
    return Lists.newArrayList(new FileStatusDatasetVersion(this.fs.getFileStatus(dataset.datasetRoot())));
  }
}
