package gobblin.data.management.copy.replication;

import com.typesafe.config.Config;

import lombok.Data;


/**
 * This class is used to pick {@link DataFlowTopology.CopyRoute} based on the replication location name in {@link SourceEndPoint}
 * For hadoop file system, the replication location name is the cluster name
 * @author mitu
 *
 */
@Data
public class DataFlowRoutesPickerBySourceCluster implements DataFlowRoutesPicker {

  private final Config allRoutes;
  private final SourceEndPoint source;

  @Override
  public Config getPreferredRoutes() {
    ReplicationLocation location = source.getReplicationLocation();

    String replicationLocationName = location.getReplicationLocationName();

    if (!allRoutes.hasPath(replicationLocationName)) {
      throw new IllegalArgumentException("can not get routes for cluster " + replicationLocationName);
    }
    return this.allRoutes.getConfig(replicationLocationName);
  }
}
