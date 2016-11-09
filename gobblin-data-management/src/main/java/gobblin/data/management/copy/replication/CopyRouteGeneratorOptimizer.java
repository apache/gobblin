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


/**
 * Provide the basic optimizer implementation in pull mode. The subclass should override the {@link #getOptimizedCopyRoute(List)} function
 * @author mitu
 *
 */
public class CopyRouteGeneratorOptimizer extends CopyRouteGeneratorBase {

  @Override
  public Optional<CopyRoute> getPullRoute(ReplicationConfiguration rc, EndPoint copyTo) {
    if (rc.getCopyMode() == ReplicationCopyMode.PUSH)
      return Optional.absent();

    DataFlowTopology topology = rc.getDataFlowToplogy();
    List<DataFlowTopology.DataFlowPath> paths = topology.getDataFlowPaths();

    for (DataFlowTopology.DataFlowPath p : paths) {
      List<CopyRoute> routes = p.getCopyRoutes();
      if (routes.isEmpty()) {
        continue;
      }

      if (routes.get(0).getCopyTo().equals(copyTo)) {
        return getOptimizedCopyRoute(routes);
      }
    }

    return Optional.absent();
  }

  public Optional<CopyRoute> getOptimizedCopyRoute(List<CopyRoute> routes) {
    return Optional.absent();
  }
}
