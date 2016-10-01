package gobblin.data.management.copy.replication;

import java.util.List;

import com.google.common.base.Optional;


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

      if (routes.get(0).getCopyFrom() == copyFrom) {
        return Optional.of(routes);
      }
    }
    return Optional.absent();
  }

}
