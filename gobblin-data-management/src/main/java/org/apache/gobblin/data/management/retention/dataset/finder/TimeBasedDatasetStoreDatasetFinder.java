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
import org.apache.gobblin.data.management.retention.dataset.CleanableDatasetStoreDataset;
import org.apache.gobblin.data.management.retention.dataset.TimeBasedDatasetStoreDataset;
import org.apache.gobblin.metastore.DatasetStoreDataset;
import org.apache.gobblin.metastore.DatasetStoreDatasetFinder;
import org.apache.hadoop.fs.FileSystem;


/**
 * A {@link DatasetStoreDatasetFinder} that returns {@link CleanableDatasetStoreDataset}
 */
public class TimeBasedDatasetStoreDatasetFinder extends DatasetStoreDatasetFinder {

  private Properties props;

  public TimeBasedDatasetStoreDatasetFinder(FileSystem fs, Properties props) throws IOException {
    super(fs, props);
    this.props = props;
  }

  @Override
  public List<DatasetStoreDataset> findDatasets() throws IOException {
    return super.findDatasets().stream()
                .map(dataset -> new TimeBasedDatasetStoreDataset(dataset.getKey(), dataset.getDatasetStateStoreMetadataEntries(), props))
                .collect(Collectors.toList());
      }
}
