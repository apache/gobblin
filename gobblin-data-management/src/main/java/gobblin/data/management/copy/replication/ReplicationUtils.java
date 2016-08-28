package gobblin.data.management.copy.replication;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigValue;

/**
 * Utility class to build different java class from {@link com.typesafe.config.Config} 
 * @author mitu
 *
 */
public class ReplicationUtils {
  public static final String METADATA = "metadata";
  public static final String METADATA_JIRA = "jira";
  public static final String METADATA_OWNER = "owner";
  public static final String METADATA_NAME = "name";
  
  public static final String REPLICATION_SOURCE = "source";
  public static final String REPLICATION_REPLICAS = "replicas";
  public static final String REPLICATOIN_REPLICAS_LIST = "list";
  
  public static final String REPLICATION_LOCATION_TYPE_KEY = "type";
  
  public static ReplicationMetaData buildMetaData(Config config){
    if(!config.hasPath(METADATA)){
      return new ReplicationMetaData(Optional.absent(), Optional.absent(), Optional.absent());
    }
    
    Config metaDataConfig = config.getConfig(METADATA);
    
    Optional<String> metaDataJira = metaDataConfig.hasPath(METADATA_JIRA)? Optional.of(metaDataConfig.getString(METADATA_JIRA)): Optional.absent();
    Optional<String> metaDataOwner = metaDataConfig.hasPath(METADATA_OWNER)? Optional.of(metaDataConfig.getString(METADATA_OWNER)): Optional.absent();
    Optional<String> metaDataName = metaDataConfig.hasPath(METADATA_NAME)? Optional.of(metaDataConfig.getString(METADATA_NAME)): Optional.absent();
    
    ReplicationMetaData metaData = new ReplicationMetaData(metaDataJira, metaDataOwner, metaDataName);
    return metaData;
  }
  
  public static ReplicationSource buildSource(Config config){
    Preconditions.checkArgument(config.hasPath(REPLICATION_SOURCE));
    
    Config sourceConfig = config.getConfig(REPLICATION_SOURCE);
    
    return new ReplicationSource(ReplicationUtils.buildReplicationLocation(sourceConfig), Optional.absent());
  }
  
  public static List<ReplicationReplica> buildReplicas(Config config){
    Preconditions.checkArgument(config.hasPath(REPLICATION_REPLICAS));
    
    Config replicaConfig = config.getConfig(REPLICATION_REPLICAS);
    Preconditions.checkArgument(replicaConfig.hasPath(REPLICATOIN_REPLICAS_LIST));
    ConfigList replicas = replicaConfig.getList(REPLICATOIN_REPLICAS_LIST);
    Iterator<ConfigValue> it = replicas.iterator();
    
    List<ReplicationReplica> result = new ArrayList<ReplicationReplica>();
    while(it.hasNext()){
      String replicaName = it.next().render();
      Config subConfig = replicaConfig.getConfig(replicaName);
      result.add(new ReplicationReplica(replicaName, ReplicationUtils.buildReplicationLocation(subConfig), Optional.absent()));
    }
    
    return result;
  }

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
