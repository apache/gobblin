package gobblin.data.management.copy.replication;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import gobblin.data.management.copy.CopyableDataset;
import gobblin.util.reflection.GobblinConstructorUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;


/**
 * Class to represent the data flow topology from copy source to copy destinations. Each {@link DataFlowTopology} contains 
 * a list of {@link DataFlowTopology.CopyRoute}s
 * 
 * Each {@link DataFlowTopology.CopyRoute}, includes
 * <ul>
 *  <li>A copyTo {@link ReplicaEndPoint}
 *  <li>A list of copyFrom of {@link ReplicaEndPoint}
 * </ul>
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
    Preconditions.checkArgument(builder.replicas != null && builder.replicas.size() > 0,
        "Can not build topology without replicas");
    Preconditions.checkArgument(builder.topologyConfig != null, "Can not build topology without topology config");
    Preconditions.checkArgument(builder.topologyConfig.hasPath(ROUTES), "Can not build topology without " + ROUTES);

    // key of the map is replication name, value is {@link ReplicationReplica}
    Map<String, ReplicaEndPoint> replicasMap = new HashMap<String, ReplicaEndPoint>();
    for (ReplicaEndPoint replica : builder.replicas) {
      String name = replica.getReplicationName();
      // replica name can not be {@link ReplicationConfiguration#REPLICATION_SOURCE}
      Preconditions.checkArgument(!name.equals(ReplicationConfiguration.REPLICATION_SOURCE), String
          .format("replica name %s can not be reserved word %s ", name, ReplicationConfiguration.REPLICATION_SOURCE));
      replicasMap.put(name, replica);
    }

    this.routes = new ArrayList<CopyRoute>();

    Config routesConfig = builder.topologyConfig.getConfig(ROUTES);
    // routes should be available for each replica
    for (ReplicaEndPoint replica : builder.replicas) {
      Preconditions.checkArgument(routesConfig.hasPath(replica.getReplicationName()),
          "can not find route for replica " + replica.getReplicationName());

      List<ReplicationEndPoint<CopyableDataset>> copyFromReplica =
          new ArrayList<ReplicationEndPoint<CopyableDataset>>();

      List<String> copyFromStrings = routesConfig.getStringList(replica.getReplicationName());

      for (String copyFromName : copyFromStrings) {
        Preconditions.checkArgument(!copyFromName.equals(replica.getReplicationName()),
            "can not have same name in both destination and copy from list " + copyFromName);
        // copy from source
        if (copyFromName.equals(ReplicationConfiguration.REPLICATION_SOURCE)) {
          copyFromReplica.add(builder.source);
        }
        // copy from other replicas
        else if (replicasMap.containsKey(copyFromName)) {
          copyFromReplica.add(replicasMap.get(copyFromName));
        } else {
          throw new IllegalArgumentException("can not find replica with name " + copyFromName);
        }
      }

      CopyRouteBuilder routeBuilder = new CopyRouteBuilder();
      routeBuilder.withCopyTo(replica);
      for (ReplicationEndPoint<CopyableDataset> from : copyFromReplica) {
        routeBuilder.addCopyFrom(from);
      }
      this.routes.add(routeBuilder.build());
    }
  }

  public static class CopyRoute {
    @Getter
    private final ReplicaEndPoint copyTo;

    @Getter
    private final List<ReplicationEndPoint<CopyableDataset>> copyFrom;

    private CopyRoute(CopyRouteBuilder builder) {
      this.copyTo = builder.copyTo;
      this.copyFrom = builder.copyFroms;
    }

    @Override
    public String toString() {
      Function<ReplicationEndPoint<CopyableDataset>, String> func =
          new Function<ReplicationEndPoint<CopyableDataset>, String>() {
            @Override
            public String apply(ReplicationEndPoint<CopyableDataset> t) {
              return t.getReplicationName();
            }
          };

      return Objects.toStringHelper(this.getClass()).add("copyTo", this.copyTo.getReplicationName())
          .add("copyFrom", Joiner.on(",").join(Lists.transform(this.copyFrom, func))).toString();
    }
  }

  public static class CopyRouteBuilder {
    private ReplicaEndPoint copyTo;

    private List<ReplicationEndPoint<CopyableDataset>> copyFroms =
        new ArrayList<ReplicationEndPoint<CopyableDataset>>();

    public CopyRouteBuilder withCopyTo(ReplicaEndPoint copyTo) {
      this.copyTo = copyTo;
      return this;
    }

    public CopyRouteBuilder addCopyFrom(ReplicationEndPoint<CopyableDataset> from) {
      this.copyFroms.add(from);
      return this;
    }

    public CopyRoute build() {
      return new CopyRoute(this);
    }
  }

  public static class Builder {

    private SourceEndPoint source;

    private List<ReplicaEndPoint> replicas = new ArrayList<ReplicaEndPoint>();

    private Config topologyConfig;

    public Builder withReplicationSource(SourceEndPoint source) {
      this.source = source;
      return this;
    }

    public Builder addReplicationReplica(ReplicaEndPoint replica) {
      this.replicas.add(replica);
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

  public static DataFlowTopology buildDataFlowTopology(Config config, SourceEndPoint source,
      List<ReplicaEndPoint> replicas) {
    Preconditions.checkArgument(config.hasPath(ReplicationConfiguration.DATAFLOWTOPOLOGY),
        "missing required config entery " + ReplicationConfiguration.DATAFLOWTOPOLOGY);
    Config dataflowConfig = config.getConfig(ReplicationConfiguration.DATAFLOWTOPOLOGY);

    // NOT specified by literal routes, need to pick it
    if (!dataflowConfig.hasPath(DataFlowTopology.ROUTES)) {
      // DEFAULT_ALL_ROUTES_KEY specified in top level config
      Preconditions.checkArgument(config.hasPath(ReplicationConfiguration.DEFAULT_ALL_ROUTES_KEY),
          "missing required config entery " + ReplicationConfiguration.DEFAULT_ALL_ROUTES_KEY);

      Config allRoutes = config.getConfig(ReplicationConfiguration.DEFAULT_ALL_ROUTES_KEY);
      DataFlowRoutesPickerTypes routePickerType = dataflowConfig.hasPath(ReplicationConfiguration.ROUTES_PICKER_TYPE)
          ? DataFlowRoutesPickerTypes.forName(dataflowConfig.getString(ReplicationConfiguration.ROUTES_PICKER_TYPE))
          : ReplicationConfiguration.DEFAULT_ROUTES_PICKER_TYPE;

      DataFlowRoutesPicker picker =
          DataFlowRoutesPickerFactory.createDataFlowRoutesPicker(routePickerType, allRoutes, source);
      dataflowConfig = picker.getPreferredRoutes();
    }
    DataFlowTopology.Builder builder = new DataFlowTopology.Builder().withReplicationSource(source);
    for (ReplicaEndPoint replica : replicas) {
      builder.addReplicationReplica(replica);
    }

    return builder.withTopologyConfig(dataflowConfig).build();
  }
}
