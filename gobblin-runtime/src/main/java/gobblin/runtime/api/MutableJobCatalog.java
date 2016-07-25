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

import gobblin.annotation.Alpha;

/**
 * A {@link JobCatalog} that can have its {@link Collection} of {@link JobSpec}s modified
 * programmatically. Note that jobs in a job catalog can change from the outside. This is covered
 * by the base JobCatalog interface.
 */
@Alpha
public interface MutableJobCatalog extends JobCatalog {
  /**
   * Registers a new JobSpec. If a JobSpec with the same {@link JobSpec#getUri()} exists,
   * it will be replaced. */
  public void put(JobSpec jobSpec);

  /**
   * Removes an existing JobSpec with the given URI. A no-op if such JobSpec does not exist.
   */
  public void remove(URI uri);
}
