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
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

import gobblin.data.management.retention.dataset.Dataset;
import gobblin.data.management.retention.version.DatasetVersion;
import gobblin.data.management.util.PathUtils;


/**
 * Class to find dataset versions in the file system.
 *
 * Concrete subclasses should implement a ({@link org.apache.hadoop.fs.FileSystem}, {@link java.util.Properties})
 * constructor to be instantiated by {@link gobblin.data.management.retention.DatasetCleaner}.
 *
 * @param <T> Type of {@link gobblin.data.management.retention.version.DatasetVersion} expected from this class.
 */
public abstract class DatasetVersionFinder<T extends DatasetVersion> implements VersionFinder<T> {

  FileSystem fs;

  public DatasetVersionFinder(FileSystem fs, Properties props) {
    this.fs = fs;
  }

  /**
   * Find dataset versions in the input {@link org.apache.hadoop.fs.Path}. Dataset versions are subdirectories of the
   * input {@link org.apache.hadoop.fs.Path} representing a single manageable unit in the dataset.
   * See {@link gobblin.data.management.retention.DatasetCleaner} for more information.
   *
   * @param dataset {@link org.apache.hadoop.fs.Path} to directory containing all versions of a dataset.
   * @return Map of {@link gobblin.data.management.retention.version.DatasetVersion} and {@link org.apache.hadoop.fs.FileStatus}
   *        for each dataset version found.
   * @throws IOException
   */
  @Override
  public Collection<T> findDatasetVersions(Dataset dataset) throws IOException {
    Path versionGlobStatus = new Path(dataset.datasetRoot(), globVersionPattern());
    FileStatus[] dataSetVersionPaths = this.fs.globStatus(versionGlobStatus);

    List<T> dataSetVersions = Lists.newArrayList();
    for(FileStatus dataSetVersionPath: dataSetVersionPaths) {
      T datasetVersion = getDatasetVersion(
          PathUtils.relativizePath(dataSetVersionPath.getPath(), dataset.datasetRoot()), dataSetVersionPath.getPath());
      if(datasetVersion != null) {
        dataSetVersions.add(datasetVersion);
      }
    }

    return dataSetVersions;
  }

  /**
   * Should return class of T.
   */
  public abstract Class<? extends DatasetVersion> versionClass();

  /**
   * Glob pattern relative to the root of the dataset used to find {@link org.apache.hadoop.fs.FileStatus} for each
   * dataset version.
   * @return glob pattern relative to dataset root.
   */
  public abstract Path globVersionPattern();

  /**
   * Parse {@link gobblin.data.management.retention.version.DatasetVersion} from the path of a dataset version.
   * @param pathRelativeToDatasetRoot {@link org.apache.hadoop.fs.Path} of dataset version relative to dataset root.
   * @param fullPath full {@link org.apache.hadoop.fs.Path} of the dataset version.
   * @return {@link gobblin.data.management.retention.version.DatasetVersion} for that path.
   */
  public abstract T getDatasetVersion(Path pathRelativeToDatasetRoot, Path fullPath);

}
