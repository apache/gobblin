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

import gobblin.data.management.retention.dataset.Dataset;
import gobblin.data.management.retention.version.DatasetVersion;


/**
 * Finds dataset versions in the file system.
 *
 * @param <T> Type of {@link gobblin.data.management.retention.version.DatasetVersion} expected from this class.
 */
public interface VersionFinder<T extends DatasetVersion> {

  /**
   * Should return class of T.
   */
  public abstract Class<? extends DatasetVersion> versionClass();

  /**
   * Find dataset versions for the dataset at {@link org.apache.hadoop.fs.Path}. Dataset versions are subdirectories of the
   * input {@link org.apache.hadoop.fs.Path} representing a single manageable unit in the dataset.
   * See {@link gobblin.data.management.retention.DatasetCleaner} for more information.
   *
   * @param dataset {@link org.apache.hadoop.fs.Path} to directory containing all versions of a dataset.
   * @return Collection of {@link gobblin.data.management.retention.version.DatasetVersion}
   *        for each dataset version found.
   * @throws IOException
   */
  public Collection<T> findDatasetVersions(Dataset dataset) throws IOException;
}
