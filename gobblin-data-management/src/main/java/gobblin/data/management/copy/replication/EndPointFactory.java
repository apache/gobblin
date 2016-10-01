package gobblin.data.management.copy.replication;

import com.typesafe.config.Config;


public interface EndPointFactory {
  
  public EndPoint buildSource(Config sourceConfig);

  public EndPoint buildReplica(Config replicasConfig, String replicaName);

}
