package gobblin.data.management.copy.replication;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;


public class ReplicaEndPoint extends AbstractReplicationEndPoint {

  @Override
  public boolean isSource() {
    return false;
  }

  public ReplicaEndPoint(String name, ReplicationLocation rl, String dataset) {
    super(name, rl, dataset);
  }

  public static List<ReplicaEndPoint> buildReplicaEndPoints(Config config) {
    Preconditions.checkArgument(config.hasPath(ReplicationConfiguration.REPLICATION_REPLICAS),
        "missing required config entery " + ReplicationConfiguration.REPLICATION_REPLICAS);

    Config replicaConfig = config.getConfig(ReplicationConfiguration.REPLICATION_REPLICAS);
    Preconditions.checkArgument(replicaConfig.hasPath(ReplicationConfiguration.REPLICATOIN_REPLICAS_LIST),
        "missing required config entery " + ReplicationConfiguration.REPLICATOIN_REPLICAS_LIST);
    List<String> replicas = replicaConfig.getStringList(ReplicationConfiguration.REPLICATOIN_REPLICAS_LIST);

    List<ReplicaEndPoint> result = new ArrayList<ReplicaEndPoint>();
    for (String replicaName : replicas) {
      Config subConfig = replicaConfig.getConfig(replicaName);
      result.add(new ReplicaEndPoint(replicaName, ReplicationLocationFactory.buildReplicationLocation(subConfig),
          AbstractReplicationEndPoint.buildDataset(subConfig)));
    }

    return result;
  }
}
