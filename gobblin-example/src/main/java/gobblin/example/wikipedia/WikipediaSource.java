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

package gobblin.example.wikipedia;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonElement;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.source.Source;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.WatermarkInterval;
import gobblin.source.extractor.extract.AbstractSource;
import gobblin.source.extractor.extract.LongWatermark;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.WorkUnit;
import gobblin.source.workunit.Extract.TableType;

/**
 * An implementation of {@link Source} for the Wikipedia example.
 *
 * <p>
 *   This source creates a {@link gobblin.source.workunit.WorkUnit}, and uses
 *   {@link WikipediaExtractor} to pull the data from Wikipedia.
 * </p>
 *
 * @author Ziyang Liu
 */
public class WikipediaSource extends AbstractSource<String, JsonElement> {

  public static final String ARTICLE_TITLE="gobblin.wikipediaSource.workUnit.title";

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {

    Map<String, Iterable<WorkUnitState>> previousWorkUnits = state.getPreviousWorkUnitStatesByDatasetUrns();
    List<String> titles = new LinkedList<>(Splitter.on(",").omitEmptyStrings().
        splitToList(state.getProp(WikipediaExtractor.SOURCE_PAGE_TITLES)));

    Map<String, LongWatermark> prevHighWatermarks = Maps.newHashMap();
    for (Map.Entry<String, Iterable<WorkUnitState>> entry : previousWorkUnits.entrySet()) {
      Iterable<LongWatermark> watermarks =
          Iterables.transform(entry.getValue(), new Function<WorkUnitState, LongWatermark>() {
        @Override
        public LongWatermark apply(WorkUnitState wus) {
          return wus.getActualHighWatermark(LongWatermark.class);
        }
      });
      watermarks = Iterables.filter(watermarks, Predicates.notNull());
      List<LongWatermark> watermarkList = Lists.newArrayList(watermarks);
      if (watermarkList.size() > 0) {
        prevHighWatermarks.put(entry.getKey(), Collections.max(watermarkList));
      }
    }

    Extract extract = createExtract(TableType.SNAPSHOT_ONLY,
        state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY), "WikipediaOutput");
    List<WorkUnit> workUnits = Lists.newArrayList();

    for (String title : titles) {
      LongWatermark prevWatermark = prevHighWatermarks.containsKey(title) ? prevHighWatermarks.get(title) :
          new LongWatermark(-1);
      prevHighWatermarks.remove(title);
      WorkUnit workUnit = WorkUnit.create(extract, new WatermarkInterval(prevWatermark, new LongWatermark(-1)));
      workUnit.setProp(ConfigurationKeys.DATASET_URN_KEY, title);
      workUnits.add(workUnit);
    }

    for (Map.Entry<String, LongWatermark> nonProcessedDataset : prevHighWatermarks.entrySet()) {
      WorkUnit workUnit = WorkUnit.create(extract, new WatermarkInterval(nonProcessedDataset.getValue(),
          nonProcessedDataset.getValue()));
      workUnit.setProp(ConfigurationKeys.DATASET_URN_KEY, nonProcessedDataset.getKey());
      workUnits.add(workUnit);
    }

    return workUnits;
  }

  @Override
  public Extractor<String, JsonElement> getExtractor(WorkUnitState state) throws IOException {
    return new WikipediaExtractor(state);
  }

  @Override
  public void shutdown(SourceState state) {
    //nothing to do
  }
}
