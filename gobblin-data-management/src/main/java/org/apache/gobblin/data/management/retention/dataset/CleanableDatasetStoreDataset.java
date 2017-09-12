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

package org.apache.gobblin.data.management.retention.dataset;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import org.apache.gobblin.data.management.version.TimestampedDatasetStateStoreVersion;
import org.apache.gobblin.data.management.version.finder.TimestampedDatasetStateStoreVersionFinder;
import org.apache.gobblin.data.management.version.finder.VersionFinder;
import org.apache.gobblin.metastore.DatasetStoreDataset;
import org.apache.hadoop.fs.FileSystem;
import lombok.Data;


/**
 * A cleanable {@link DatasetStoreDataset}
 */
@Data
public class CleanableDatasetStoreDataset extends ModificationTimeDataset {

  private final DatasetStoreDataset store;
  private final VersionFinder<TimestampedDatasetStateStoreVersion> versionFinder;

  public CleanableDatasetStoreDataset(FileSystem fs, Properties props, DatasetStoreDataset store) throws IOException {
    super(fs, props, null);
    this.store = store;
    this.versionFinder = new TimestampedDatasetStateStoreVersionFinder();
  }

  @Override
  protected void cleanImpl(Collection deletableVersions) throws IOException {
    for (Object version : deletableVersions) {
      ((TimestampedDatasetStateStoreVersion) version).getEntry().delete();
    }
  }

  @Override
  public String datasetURN() {
    return this.store.datasetURN();
  }

  @Override
  public VersionFinder<? extends TimestampedDatasetStateStoreVersion> getVersionFinder() {
    return this.versionFinder;
  }
}
