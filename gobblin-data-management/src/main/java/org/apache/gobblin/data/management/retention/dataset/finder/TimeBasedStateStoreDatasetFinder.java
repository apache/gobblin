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

package org.apache.gobblin.data.management.retention.dataset.finder;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.gobblin.data.management.retention.state.CleanableStateStoreDataset;
import org.apache.gobblin.data.management.retention.state.TimeBasedStateStoreDataset;
import org.apache.gobblin.metastore.StateStoreDataset;
import org.apache.gobblin.metastore.StateStoreDatasetFinder;


/**
 * A {@link StateStoreDatasetFinder} that returns {@link CleanableStateStoreDataset}
 */
public class TimeBasedStateStoreDatasetFinder extends StateStoreDatasetFinder {

  private Properties props;

  public TimeBasedStateStoreDatasetFinder(Properties props) {
    super(props);
    this.props = props;
  }

  @Override
  public List<StateStoreDataset> findStates() throws IOException {
    return super.findStates().stream()
                .map(dataset -> new TimeBasedStateStoreDataset(dataset.getKey(), dataset.getDatasetStateStoreMetadataEntries(), props))
                .collect(Collectors.toList());
      }
}
