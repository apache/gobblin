package gobblin.data.management.copy.replication;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import gobblin.annotation.Alias;

@Alias(value="HadoopFsEndPointFactory")
public class HadoopFsEndPointFactory implements EndPointFactory {
  public static final String HADOOP_FS_CONFIG_KEY = "hadoopfs";

  @Override
  public EndPoint buildSource(Config sourceConfig) {
    Preconditions.checkArgument(sourceConfig.hasPath(HADOOP_FS_CONFIG_KEY),
        "missing required config entery " + HADOOP_FS_CONFIG_KEY);

    return new SourceHadoopFsEndPoint(new HadoopFsReplicaConfig(sourceConfig.getConfig(HADOOP_FS_CONFIG_KEY)));
  }

  @Override
  public EndPoint buildReplica(Config replicaConfig, String replicaName) {
    Preconditions.checkArgument(replicaConfig.hasPath(HADOOP_FS_CONFIG_KEY),
        "missing required config entery " + HADOOP_FS_CONFIG_KEY);

    return new ReplicaHadoopFsEndPoint(new HadoopFsReplicaConfig(replicaConfig.getConfig(HADOOP_FS_CONFIG_KEY)),
        replicaName);
  }
}
