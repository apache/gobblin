package gobblin.data.management.copy.replication;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import gobblin.util.ClassAliasResolver;
import lombok.Getter;


public class ReplicationConfiguration {
  public static final String REPLICATION_COPY_MODE = "copymode";
  public static final String METADATA = "metadata";
  public static final String METADATA_JIRA = "jira";
  public static final String METADATA_OWNER = "owner";
  public static final String METADATA_NAME = "name";

  public static final String REPLICATION_SOURCE = "source";
  public static final String REPLICATION_REPLICAS = "replicas";
  public static final String REPLICATOIN_REPLICAS_LIST = "list";

  public static final String DATA_FLOW_TOPOLOGY = "dataFlowTopology";

  public static final String DEFAULT_DATA_FLOW_TOPOLOGIES = "defaultDataFlowTopologies";

  public static final String ROUTES_PICKER_TYPE = "routePickerType";

  public static final String END_POINT_FACTORY_CLASS = "endPointFactoryClass";
  public static final String DEFAULT_END_POINT_FACTORY_CLASS = HadoopFsEndPointFactory.class.getCanonicalName();

  public static final ClassAliasResolver<EndPointFactory> endPointFactoryResolver =
      new ClassAliasResolver<>(EndPointFactory.class);

  @Getter
  private final ReplicationCopyMode copyMode;

  @Getter
  private final ReplicationMetaData metaData;

  @Getter
  private final EndPoint source;

  @Getter
  private final List<EndPoint> replicas;

  public static ReplicationConfiguration buildFromConfig(Config config)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    Preconditions.checkArgument(config != null, "can not build ReplicationConfig from null");

    return new Builder().withReplicationMetaData(ReplicationMetaData.buildMetaData(config))
        .withReplicationCopyMode(ReplicationCopyMode.getReplicationCopyMode(config)).withReplicationSource(config)
        .withReplicationReplica(config).build();
  }

  private ReplicationConfiguration(Builder builder) {
    this.metaData = builder.metaData;
    this.source = builder.source;
    this.replicas = builder.replicas;
    this.copyMode = builder.copyMode;
  }

  private static class Builder {
    private ReplicationMetaData metaData;

    private EndPoint source;

    private List<EndPoint> replicas = new ArrayList<EndPoint>();

    private ReplicationCopyMode copyMode;

    public Builder withReplicationMetaData(ReplicationMetaData metaData) {
      this.metaData = metaData;
      return this;
    }

    public Builder withReplicationSource(Config config)
        throws InstantiationException, IllegalAccessException, ClassNotFoundException {
      Preconditions.checkArgument(config.hasPath(ReplicationConfiguration.REPLICATION_SOURCE),
          "missing required config entery " + ReplicationConfiguration.REPLICATION_SOURCE);

      Config sourceConfig = config.getConfig(ReplicationConfiguration.REPLICATION_SOURCE);
      String endPointFactory = sourceConfig.hasPath(END_POINT_FACTORY_CLASS)
          ? sourceConfig.getString(END_POINT_FACTORY_CLASS) : DEFAULT_END_POINT_FACTORY_CLASS;
      EndPointFactory factory = endPointFactoryResolver.resolveClass(endPointFactory).newInstance();
      this.source = factory.buildSource(sourceConfig);
      return this;
    }

    public Builder withReplicationReplica(Config config)
        throws InstantiationException, IllegalAccessException, ClassNotFoundException {
      Preconditions.checkArgument(config.hasPath(ReplicationConfiguration.REPLICATION_REPLICAS),
          "missing required config entery " + ReplicationConfiguration.REPLICATION_REPLICAS);

      Config replicasConfig = config.getConfig(REPLICATION_REPLICAS);
      Preconditions.checkArgument(replicasConfig.hasPath(ReplicationConfiguration.REPLICATOIN_REPLICAS_LIST),
          "missing required config entery " + ReplicationConfiguration.REPLICATOIN_REPLICAS_LIST);
      List<String> replicaNames = replicasConfig.getStringList(ReplicationConfiguration.REPLICATOIN_REPLICAS_LIST);

      for (String replicaName : replicaNames) {
        Preconditions.checkArgument(replicasConfig.hasPath(replicaName), "missing replica name " + replicaName);
        Config subConfig = replicasConfig.getConfig(replicaName);

        // each replica could have own EndPointFactory resolver 
        String endPointFactory = subConfig.hasPath(END_POINT_FACTORY_CLASS)
            ? subConfig.getString(END_POINT_FACTORY_CLASS) : DEFAULT_END_POINT_FACTORY_CLASS;
        EndPointFactory factory = endPointFactoryResolver.resolveClass(endPointFactory).newInstance();
        this.replicas.add(factory.buildReplica(subConfig, replicaName));
      }
      return this;
    }

    public Builder withReplicationCopyMode(ReplicationCopyMode copyMode) {
      this.copyMode = copyMode;
      return this;
    }

    public ReplicationConfiguration build() {
      return new ReplicationConfiguration(this);
    }
  }
}
