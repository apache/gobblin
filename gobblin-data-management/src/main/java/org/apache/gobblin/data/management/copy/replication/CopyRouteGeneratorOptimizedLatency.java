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

/**
 * Used to generate the {@link CopyRoute} based on the {@link DataFlowTopology} optimized for latency.
 *
 * In Pull mode, query multiple data sources and pick the data source with the highest watermark
 */
package org.apache.gobblin.data.management.copy.replication;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.base.Optional;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.source.extractor.ComparableWatermark;


/**
 * In Pull mode, Optimized for data replication latency: pick the highest watermark from all the data sources
 * @author mitu
 *
 */

@Alias(value = "OptimizedLatency")
public class CopyRouteGeneratorOptimizedLatency extends CopyRouteGeneratorOptimizer {
  /**
   *
   * @param routes
   * @return the {@link CopyRoute} which has the highest watermark
   */
  @Override
  public Optional<CopyRoute> getOptimizedCopyRoute(List<CopyRoute> routes) {
    CopyRoute preferred = Collections.max(routes, new CopyRouteComparatorBySourceWatermark());
    return Optional.of(preferred);
  }

  static class CopyRouteComparatorBySourceWatermark implements Comparator<CopyRoute>, Serializable{

    private static final long serialVersionUID = 1439642339646179830L;

    @Override
    public int compare(CopyRoute o1, CopyRoute o2) {
      EndPoint from1 = o1.getCopyFrom();
      EndPoint from2 = o2.getCopyFrom();
      Optional<ComparableWatermark> w1 = from1.getWatermark();
      Optional<ComparableWatermark> w2 = from2.getWatermark();

      // both are absent
      if(!w1.isPresent() && !w2.isPresent()){
        return 0;
      }

      if(!w2.isPresent()){
        return 1;
      }

      if(!w1.isPresent()){
        return -1;
      }

      return w1.get().compareTo(w2.get());
    }
  }

}
