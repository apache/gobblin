package gobblin.data.management.copy.replication;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;

import gobblin.util.HadoopUtils;
import lombok.extern.slf4j.Slf4j;


@Slf4j
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
      try {
        Configuration conf = HadoopUtils.newConfiguration();
        FileSystem fs = FileSystem.get(copyFrom.getFsURI(), conf);
        if (fs.exists(new Path("/"))) {
          return Optional.of(copyRoute);
        } else {
          log.warn("Skipped the problematic FileSystem " + copyFrom.getFsURI());
        }
      } catch (IOException ioe) {
        log.warn("Skipped the problematic FileSystem " + copyFrom.getFsURI());
      }
    }
    return Optional.absent();
  }
}
