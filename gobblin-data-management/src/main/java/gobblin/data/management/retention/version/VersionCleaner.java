/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gobblin.data.management.retention.version;

import java.io.IOException;

import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

import gobblin.data.management.retention.dataset.CleanableDataset;
import gobblin.data.management.version.DatasetVersion;


/**
 * An abstraction for cleaning a {@link DatasetVersion} of a {@link CleanableDataset}.
 */
@Slf4j
public abstract class VersionCleaner {

  protected final CleanableDataset cleanableDataset;
  protected final DatasetVersion datasetVersion;

  public VersionCleaner(DatasetVersion datasetVersion, CleanableDataset cleanableDataset) {
    Preconditions.checkNotNull(cleanableDataset);
    Preconditions.checkNotNull(datasetVersion);

    this.cleanableDataset = cleanableDataset;
    this.datasetVersion = datasetVersion;
  }

  /**
   * Action to perform before cleaning a {@link DatasetVersion} of a {@link CleanableDataset}.
   * @throws IOException
   */
  public abstract void preCleanAction() throws IOException;

  /**
   * Cleans the {@link DatasetVersion} of a {@link CleanableDataset}.
   * @throws IOException
   */
  public abstract void clean() throws IOException;

  /**
   * Action to perform after cleaning a {@link DatasetVersion} of a {@link CleanableDataset}.
   * @throws IOException
   */
  public abstract void postCleanAction() throws IOException;
}
