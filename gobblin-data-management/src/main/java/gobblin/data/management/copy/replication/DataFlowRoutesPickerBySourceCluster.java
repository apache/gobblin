package gobblin.data.management.copy.replication;

import com.typesafe.config.Config;

import lombok.Data;

/**
 * This class is used to pick {@link DataFlowTopology.CopyRoute} based on the cluster name in {@link ReplicationSource}
 * @author mitu
 *
 */
@Data
public class DataFlowRoutesPickerBySourceCluster implements DataFlowRoutesPicker{

  private final Config allRoutes;
  private final ReplicationSource source;
  
  @Override
  public Config getPreferredRoutes() {
    ReplicationLocation location = source.getReplicationLocation();
    if(location.getType() != ReplicationType.HDFS){
      throw new IllegalArgumentException("can not get routes from non " + ReplicationType.HDFS);
    }
    
    HdfsReplicationLocation hdfsLocation = (HdfsReplicationLocation)location;
    String clusterName = hdfsLocation.getClustername();
    
    if(!allRoutes.hasPath(clusterName)){
      throw new IllegalArgumentException("can not get routes for cluster " + clusterName);
    }
    return this.allRoutes.getConfig(clusterName);
  }
}
