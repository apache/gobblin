package gobblin.data.management.copy.replication;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import gobblin.data.management.copy.CopyableDataset;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Class to represent the data flow topology from copy destination to copy sources
 * @author mitu
 *
 */
@AllArgsConstructor
public class DataFlowTopology {

  public static final String ROUTES = "routes";
  
  @Getter
  private final List<CopyRoute> routes;

  private DataFlowTopology(Builder builder) {
    Preconditions.checkArgument(builder.source != null, "Can not build topology without source");
    Preconditions.checkArgument(builder.replicas != null && builder.replicas.size() > 0, "Can not build topology without replicas");
    Preconditions.checkArgument(builder.topologyConfig != null, "Can not build topology without topology config");
    Preconditions.checkArgument(builder.topologyConfig.hasPath(ROUTES), "Can not build topology without " + ROUTES);

    // key of the map is replication name, value is {@link ReplicationReplica}
    Map<String, ReplicationReplica> replicasMap = new HashMap<String, ReplicationReplica>();
    for (ReplicationReplica replica : builder.replicas) {
      String name = replica.getReplicationName();
      // replica name can not be {@link ReplicationUtils#REPLICATION_SOURCE}
      Preconditions.checkArgument(!name.equals(ReplicationUtils.REPLICATION_SOURCE),
          String.format("replica name %s can not be reserved word %s ", name, ReplicationUtils.REPLICATION_SOURCE));
      replicasMap.put(name, replica);
    }

    this.routes = new ArrayList<CopyRoute>();

    Config routesConfig = builder.topologyConfig.getConfig(ROUTES);
    // routes should be available for each replica
    for (ReplicationReplica replica : builder.replicas) {
      Preconditions.checkArgument(routesConfig.hasPath(replica.getReplicationName()),
          "can not find route for replica " + replica.getReplicationName());

      CopyRoute route = new CopyRoute();
      route.copyDestination = replica;
      route.copyFrom = new ArrayList<ReplicationData<CopyableDataset>>();

      List<String> copyFroms = routesConfig.getStringList(replica.getReplicationName());

      for (String copyFromName : copyFroms) {
        Preconditions.checkArgument(!copyFromName.equals(route.copyDestination.getReplicationName()),
            "can not have same name in both destination and copy from list " + copyFromName);
        // copy from source
        if (copyFromName.equals(ReplicationUtils.REPLICATION_SOURCE)) {
          route.copyFrom.add(builder.source);
        }
        // copy from other replicas
        else {
          route.copyFrom.add(replicasMap.get(copyFromName));
        }
      }

      this.routes.add(route);
    }
  }

  public static class CopyRoute {
    @Getter
    private ReplicationReplica copyDestination;

    @Getter
    private List<ReplicationData<CopyableDataset>> copyFrom;

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(copyDestination.getReplicationName() + ":");
      for (ReplicationData<CopyableDataset> r : copyFrom) {
        sb.append(r.getReplicationName() + ",");
      }

      return sb.toString();
    }
  }

  public static class Builder {

    private ReplicationSource source;

    private List<ReplicationReplica> replicas;

    private Config topologyConfig;

    public Builder withReplicationSource(ReplicationSource source) {
      this.source = source;
      return this;
    }

    public Builder withReplicationReplicas(List<ReplicationReplica> replicas) {
      this.replicas = replicas;
      return this;
    }

    public Builder withTopologyConfig(Config config) {
      this.topologyConfig = config;
      return this;
    }

    public DataFlowTopology build() {
      return new DataFlowTopology(this);
    }
  }
}
