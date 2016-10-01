package gobblin.data.management.copy.replication;

import java.util.List;

import com.google.common.base.Optional;


public class CopyRouteGeneratorOptimizedLatency extends CopyRouteGeneratorOptimizer {
  /**
   * 
   * @param routes
   * @return the {@link CopyRoute} which has the highest watermark
   */
  @Override
  public Optional<CopyRoute> getOptimizedCopyRoute(List<CopyRoute> routes) {
    CopyRoute result = null;
    for (CopyRoute copyRoute : routes) {
      if (result == null || copyRoute.getCopyFrom().getWatermark().compareTo(result.getCopyFrom().getWatermark()) > 0) {
        result = copyRoute;
      }
    }
    return Optional.of(result);
  }
}
