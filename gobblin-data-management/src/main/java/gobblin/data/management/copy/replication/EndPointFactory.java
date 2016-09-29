package gobblin.data.management.copy.replication;

import java.util.List;

import com.typesafe.config.Config;


public interface EndPointFactory {
  
  public EndPoint buildSource(Config sourceConfig);

  public List<EndPoint> buildReplicas(Config replicasConfig);

}
