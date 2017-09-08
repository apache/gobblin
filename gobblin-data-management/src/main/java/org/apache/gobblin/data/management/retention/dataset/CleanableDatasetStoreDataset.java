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
import java.util.List;
import org.apache.gobblin.metastore.DatasetStoreDataset;
import org.apache.gobblin.metastore.metadata.DatasetStateStoreEntryManager;
import org.apache.hadoop.fs.Path;


/**
 * A cleanable {@link DatasetStoreDataset}
 */
public class CleanableDatasetStoreDataset extends DatasetStoreDataset implements CleanableDataset {

  public CleanableDatasetStoreDataset(DatasetStoreDataset.Key key, List<DatasetStateStoreEntryManager> entries) {
    super(key, entries);
  }

  public void clean() throws IOException {
    for (DatasetStateStoreEntryManager stateStoreEntry : this.getDatasetStateStoreMetadataEntries()) {
      stateStoreEntry.delete();
    }
  }

  public Path datasetRoot() {
    return null;
  }
}
