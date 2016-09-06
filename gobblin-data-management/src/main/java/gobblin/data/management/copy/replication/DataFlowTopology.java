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

      List<ReplicationData<CopyableDataset>> copyFromReplica = new ArrayList<ReplicationData<CopyableDataset>>();

      List<String> copyFromStrings = routesConfig.getStringList(replica.getReplicationName());

      for (String copyFromName : copyFromStrings) {
        Preconditions.checkArgument(!copyFromName.equals(replica.getReplicationName()),
            "can not have same name in both destination and copy from list " + copyFromName);
        // copy from source
        if (copyFromName.equals(ReplicationUtils.REPLICATION_SOURCE)) {
          copyFromReplica.add(builder.source);
        }
        // copy from other replicas
        else if(replicasMap.containsKey(copyFromName)){
          copyFromReplica.add(replicasMap.get(copyFromName));
        }
        else{
          throw new IllegalArgumentException("can not find replica with name " + copyFromName);
        }
      }
      
      CopyRouteBuilder routeBuilder = new CopyRouteBuilder();
      routeBuilder.withCopyDestination(replica);
      routeBuilder.withCopyFrom(copyFromReplica);
      this.routes.add(routeBuilder.build());
    }
  }

  public static class CopyRoute {
    @Getter
    private final ReplicationReplica copyDestination;

    @Getter
    private final List<ReplicationData<CopyableDataset>> copyFrom;
    
    private CopyRoute(CopyRouteBuilder builder){
      this.copyDestination = builder.copyDestination;
      this.copyFrom = builder.copyFrom;
    }

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
  
  public static class CopyRouteBuilder{
    private ReplicationReplica copyDestination;
    
    private List<ReplicationData<CopyableDataset>> copyFrom;
    
    public CopyRouteBuilder withCopyDestination(ReplicationReplica copyDest) {
      this.copyDestination = copyDest;
      return this;
    }

    public CopyRouteBuilder withCopyFrom(List<ReplicationData<CopyableDataset>> copyFrom) {
      this.copyFrom = copyFrom;
      return this;
    }

    public CopyRoute build() {
      return new CopyRoute(this);
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
