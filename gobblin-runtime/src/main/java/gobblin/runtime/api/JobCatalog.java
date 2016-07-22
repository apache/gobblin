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
package gobblin.runtime.api;

import java.net.URI;
import java.util.Collection;

import gobblin.annotation.Alpha;

/**
 * A catalog of all the {@link JobSpec}s a Gobblin instance is currently aware of.
 */
@Alpha
public interface JobCatalog {
  /** Returns an immutable {@link Collection} of {@link JobSpec}s that are known to the catalog. */
  Collection<JobSpec> getJobs();

  /**
   * Get a {@link JobSpec} by uri.
   * @throws JobSpecNotFoundException if no such JobSpec exists
   **/
  JobSpec getJobSpec(URI uri);

  /**
   * Adds a {@link JobCatalogListener} that will be invoked upon updates on the
   * {@link JobCatalog}. Upon registration {@link JobCatalogListener#onAddJob(JobSpec)} will be
   * invoked for all pre-existing jobs in the JobCatalog.
   */
  void addListener(JobCatalogListener jobListener);

  /**
   * Removes the specified listener. No-op if not registered.
   */
  void removeListener(JobCatalogListener jobListener);
}
