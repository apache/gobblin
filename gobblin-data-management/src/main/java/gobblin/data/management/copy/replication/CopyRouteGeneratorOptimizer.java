package gobblin.data.management.copy.replication;

import java.util.List;

import com.google.common.base.Optional;


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

      if (routes.get(0).getCopyTo() == copyTo) {
        return getOptimizedCopyRoute(routes);
      }
    }

    return Optional.absent();
  }

  public Optional<CopyRoute> getOptimizedCopyRoute(List<CopyRoute> routes) {
    return Optional.absent();
  }
}
