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

package gobblin.ingestion.google.webmaster;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.extract.LongWatermark;


public class GoogleWebMasterSourceDaily extends GoogleWebMasterSource {

  @Override
  GoogleWebmasterExtractor createExtractor(WorkUnitState state, Map<String, Integer> columnPositionMap,
      List<GoogleWebmasterFilter.Dimension> requestedDimensions,
      List<GoogleWebmasterDataFetcher.Metric> requestedMetrics) throws IOException {

    long lowWatermark = state.getWorkunit().getLowWatermark(LongWatermark.class).getValue();
    long expectedHighWatermark = state.getWorkunit().getExpectedHighWatermark(LongWatermark.class).getValue();
    return new GoogleWebmasterExtractor(state, lowWatermark, expectedHighWatermark, columnPositionMap, requestedDimensions,
        requestedMetrics);
  }
}
