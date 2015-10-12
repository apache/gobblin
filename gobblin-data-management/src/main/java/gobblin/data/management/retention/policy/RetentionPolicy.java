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

package gobblin.data.management.retention.policy;

import java.util.Collection;
import java.util.List;

import gobblin.data.management.retention.version.DatasetVersion;


/**
 * Retention policy around versions of a dataset. Specifies which versions of a dataset should be deleted by
 * {@link gobblin.data.management.retention.DatasetCleaner}.
 * @param <T> {@link gobblin.data.management.retention.version.DatasetVersion} accepted by this policy.
 */
public interface RetentionPolicy<T extends DatasetVersion> {

  /**
   * Should return class of T.
   * @return class of T.
   */
  public Class<? extends DatasetVersion> versionClass();

  /**
   * Logic to decide which dataset versions should be deleted. Only datasets returned will be deleted from filesystem.
   *
   * @param allVersions List of all dataset versions in the file system,
   *                                               sorted from newest to oldest.
   * @return Collection of dataset versions that should be deleted.
   */
  public Collection<T> listDeletableVersions(List<T> allVersions);

}
