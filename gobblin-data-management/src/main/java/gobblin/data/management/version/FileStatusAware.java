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
package gobblin.data.management.version;

import java.util.Set;

import org.apache.hadoop.fs.FileStatus;

/**
 * A {@link FileSystemDatasetVersion} that is aware {@link FileStatus}s or its paths
 */
public interface FileStatusAware {
  /**
   * Get the set of {@link FileStatus}s that are included in this dataset version or the {@link FileStatus} of the dataset
   * version itself (In which case the set has one file status).
   *
   */
  public Set<FileStatus> getFileStatuses();
}
