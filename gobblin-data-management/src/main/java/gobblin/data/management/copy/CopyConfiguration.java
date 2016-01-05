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

package gobblin.data.management.copy;

import lombok.Data;

import org.apache.hadoop.fs.Path;


/**
 * Configuration for Gobblin distcp jobs.
 */
@Data
public class CopyConfiguration {

  /**
   * Directory where dataset should be replicated.
   * This directory corresponds to the {@link CopyableDataset#datasetRoot} in the new location.
   */
  private final Path targetRoot;
  /**
   * Preserve options passed by the user.
   */
  private final PreserveAttributes preserve;
  /**
   * {@link CopyContext} for this job.
   */
  private final CopyContext copyContext;

}
