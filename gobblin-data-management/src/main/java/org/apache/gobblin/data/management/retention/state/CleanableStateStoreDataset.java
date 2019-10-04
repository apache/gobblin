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

package org.apache.gobblin.data.management.retention.state;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.gobblin.data.management.policy.VersionSelectionPolicy;
import org.apache.gobblin.data.management.retention.dataset.CleanableDataset;
import org.apache.gobblin.data.management.version.DatasetStateStoreVersion;
import org.apache.gobblin.data.management.version.DatasetVersion;
import org.apache.gobblin.data.management.version.finder.VersionFinder;
import org.apache.gobblin.metastore.DatasetStoreDataset;
import org.apache.gobblin.metastore.StateStoreDataset;
import org.apache.gobblin.metastore.metadata.StateStoreEntryManager;


/**
 * A cleanable {@link StateStoreDataset}
 */
public abstract class CleanableStateStoreDataset<T extends DatasetVersion> extends StateStoreDataset implements CleanableDataset {

  public CleanableStateStoreDataset(Key key, List<StateStoreEntryManager> entries) {
    super(key, entries);
  }

  public abstract VersionFinder<? extends T> getVersionFinder();

  public abstract VersionSelectionPolicy<T> getVersionSelectionPolicy();

  @Override
  public void clean() throws IOException {

    List<T> versions = Lists.newArrayList(this.getVersionFinder().findDatasetVersions(this));

    Collections.sort(versions, Collections.reverseOrder());

    Collection<T> deletableVersions = this.getVersionSelectionPolicy().listSelectedVersions(versions);

    for (Object version : deletableVersions) {
      ((StateStoreEntryManager) ((DatasetStateStoreVersion) version).getEntry()).delete();
    }
  }
}
