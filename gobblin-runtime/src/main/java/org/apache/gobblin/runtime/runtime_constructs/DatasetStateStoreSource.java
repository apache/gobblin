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

package org.apache.gobblin.runtime.runtime_constructs;

import java.io.IOException;
import java.util.List;
import java.util.ListIterator;

import com.google.gson.Gson;
import com.google.common.collect.Lists;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.dataset.DatasetsFinder;
import org.apache.gobblin.metastore.DatasetStoreDataset;
import org.apache.gobblin.metastore.DatasetStoreDatasetFinder;
import org.apache.gobblin.metastore.metadata.DatasetStateStoreEntryManager;
import org.apache.gobblin.source.Source;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.io.GsonInterfaceAdapter;


/**
 * A source that mainly for collecting state store
 * and migrate into another one with different implementation.
 *
 * e.g. Move .jst file from File System into MySQL.
 */

public class DatasetStateStoreSource implements Source<String,  ListIterator<DatasetStateStoreEntryManager>> {

  /**
   * For SerDe purpose.
   */
  public static final Gson GENERICS_AWARE_GSON = GsonInterfaceAdapter.getGson(Object.class);

  public static final String ABSTRACT_DATASET = "source.abstract.datasetEntry";

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    List<WorkUnit> workUnits = Lists.newArrayList();
    try {
      DatasetsFinder<DatasetStoreDataset> _datasetsFinder = new DatasetStoreDatasetFinder(state.getProperties());
      for (DatasetStoreDataset datasetStoreDataset : _datasetsFinder.findDatasets()) {
        WorkUnit wu = new WorkUnit();
        wu.setProp(this.ABSTRACT_DATASET, GENERICS_AWARE_GSON.toJson(datasetStoreDataset, DatasetStoreDataset.class));
        workUnits.add(wu);
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Hits runtime exception while finding datasets", ioe);
    }
    return workUnits;
  }

  @Override
  public Extractor<String, ListIterator<DatasetStateStoreEntryManager>> getExtractor(WorkUnitState state)
      throws IOException {
    return new DatasetStateStoreExtractor(state);
  }

  @Override
  public void shutdown(SourceState state) {
    // doing nothing here.
  }
}
