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

import gobblin.dataset.Dataset;
import gobblin.data.management.version.DatasetVersion;


/**
 * Finds dataset versions.
 *
 * @param <T> Type of {@link DatasetVersion} expected from this class.
 */
public interface VersionFinder<T extends DatasetVersion> {

  /**
   * Should return class of T.
   */
  public abstract Class<? extends DatasetVersion> versionClass();

  /**
   * Find dataset versions for {@link Dataset}. Each dataset versions represents a single manageable unit in the dataset.
   *
   * @param dataset which contains all versions.
   * @return Collection of {@link DatasetVersion} for each dataset version found.
   * @throws IOException
   */
  public Collection<T> findDatasetVersions(Dataset dataset) throws IOException;
}
