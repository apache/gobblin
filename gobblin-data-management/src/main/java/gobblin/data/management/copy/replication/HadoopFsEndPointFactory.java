package gobblin.data.management.copy.replication;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;


public class HadoopFsEndPointFactory implements EndPointFactory {
  public static final String HADOOP_FS_CONFIG_KEY = "hadoopfs";

  @Override
  public EndPoint buildSource(Config sourceConfig) {
    Preconditions.checkArgument(sourceConfig.hasPath(HADOOP_FS_CONFIG_KEY),
        "missing required config entery " + HADOOP_FS_CONFIG_KEY);

    return new SourceHadoopFsEndPoint(new HadoopFsReplicaConfig(sourceConfig.getConfig(HADOOP_FS_CONFIG_KEY)));
  }

  @Override
  public List<EndPoint> buildReplicas(Config replicasConfig) {
    Preconditions.checkArgument(replicasConfig.hasPath(ReplicationConfiguration.REPLICATOIN_REPLICAS_LIST),
        "missing required config entery " + ReplicationConfiguration.REPLICATOIN_REPLICAS_LIST);
    List<String> replicas = replicasConfig.getStringList(ReplicationConfiguration.REPLICATOIN_REPLICAS_LIST);

    List<EndPoint> result = new ArrayList<EndPoint>();
    for (String replicaName : replicas) {
      Config subConfig = replicasConfig.getConfig(replicaName);
      result.add(buildReplica(subConfig, replicaName));
    }

    return result;
  }

  protected EndPoint buildReplica(Config replicaConfig, String replicaName) {
    Preconditions.checkArgument(replicaConfig.hasPath(HADOOP_FS_CONFIG_KEY),
        "missing required config entery " + HADOOP_FS_CONFIG_KEY);

    return new ReplicaHadoopFsEndPoint(new HadoopFsReplicaConfig(replicaConfig.getConfig(HADOOP_FS_CONFIG_KEY)),
        replicaName);
  }
}
