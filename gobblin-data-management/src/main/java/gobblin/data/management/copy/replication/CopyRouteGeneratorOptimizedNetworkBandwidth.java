/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.data.management.copy.replication;

import java.util.List;

import com.google.common.base.Optional;
import gobblin.annotation.Alias;

/**
 * In Pull mode, Optimized for network bandwidth : pick the first available data source
 * @author mitu
 *
 */


@Alias(value="OptimizedNetworkBandwidth")
public class CopyRouteGeneratorOptimizedNetworkBandwidth extends CopyRouteGeneratorOptimizer {
  /**
   * 
   * @param routes
   * @return the first available {@link CopyRoute}
   */
  @Override
  public Optional<CopyRoute> getOptimizedCopyRoute(List<CopyRoute> routes) {
    for (CopyRoute copyRoute : routes) {
      if (!(copyRoute.getCopyFrom() instanceof HadoopFsEndPoint)) {
        continue;
      }

      HadoopFsEndPoint copyFrom = (HadoopFsEndPoint) (copyRoute.getCopyFrom());
      if(copyFrom.isAvailable()) {
        return Optional.of(copyRoute);
      }
    }
    return Optional.absent();
  }
}
