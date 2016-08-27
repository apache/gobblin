package gobblin.data.management.copy.replication;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

public class ReplicationLocationBuilder {
  public static final String REPLICATION_LOCATION_TYPE_KEY = "type";

  public static ReplicationLocation buildReplicationLocation(Config config){
    Preconditions.checkArgument(config.hasPath(REPLICATION_LOCATION_TYPE_KEY));
    
    String type = config.getString(REPLICATION_LOCATION_TYPE_KEY);
    Preconditions.checkArgument(config.hasPath(type));
    
    ReplicationType RType = ReplicationType.forName(type);
    
    if(RType == ReplicationType.HDFS){
      return new HdfsReplicationLocation(config.getConfig(type));
    }
    
    return null;
  }

}
