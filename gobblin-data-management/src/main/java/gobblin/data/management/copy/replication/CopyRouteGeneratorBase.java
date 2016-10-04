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
 * Basic implementation for picking a {@link CopyRoute} in Push mode
 * @author mitu
 *
 */
public class CopyRouteGeneratorBase implements CopyRouteGenerator {

  @Override
  public Optional<CopyRoute> getPullRoute(ReplicationConfiguration rc, EndPoint copyTo) {
    return Optional.absent();
  }

  /**
   * for push mode, there is no optimization
   */
  @Override
  public Optional<List<CopyRoute>> getPushRoutes(ReplicationConfiguration rc, EndPoint copyFrom) {
    if (rc.getCopyMode() == ReplicationCopyMode.PULL)
      return Optional.absent();

    DataFlowTopology topology = rc.getDataFlowToplogy();
    List<DataFlowTopology.DataFlowPath> paths = topology.getDataFlowPaths();

    for (DataFlowTopology.DataFlowPath p : paths) {
      List<CopyRoute> routes = p.getCopyRoutes();
      if (routes.isEmpty()) {
        continue;
      }

      if (routes.get(0).getCopyFrom().equals(copyFrom)) {
        return Optional.of(routes);
      }
    }
    return Optional.absent();
  }

}
