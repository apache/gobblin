package gobblin.data.management.copy.replication;

import java.util.List;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import lombok.AllArgsConstructor;
import lombok.Getter;


/**
 * Class ReplicationConfiguration is used to describe the overall configuration of the replication flow for
 * a specific {@link ReplicationEndPoint}, including:
 * <ul>
 *  <li>Replication copy mode {@link ReplicationCopyMode}
 *  <li>Meta data {@link ReplicationMetaData}
 *  <li>Replication source {@link SourceEndPoint}
 *  <li>Replication replicas {@link ReplicaEndPoint}
 *  <li>Replication topology {@link DataFlowTopology}
 * </ul>
 * @author mitu
 *
 */
@AllArgsConstructor
public class ReplicationConfiguration {

  public static final String REPLICATION_COPY_MODE = "copymode";
  public static final String METADATA = "metadata";
  public static final String METADATA_JIRA = "jira";
  public static final String METADATA_OWNER = "owner";
  public static final String METADATA_NAME = "name";

  public static final String REPLICATION_SOURCE = "source";
  public static final String REPLICATION_REPLICAS = "replicas";
  public static final String REPLICATOIN_REPLICAS_LIST = "list";

  public static final String REPLICATION_LOCATION_TYPE_KEY = "type";

  public static final String DATAFLOWTOPOLOGY = "dataFlowTopology";

  public static final String DEFAULT_ALL_ROUTES_KEY = "defaultDataFlowTopology";

  public static final String ROUTES_PICKER_CLASS_KEY = "routePickerClass";
  public static final String DEFAULT_ROUTES_PICKER_CLASS_KEY =
      DataFlowRoutesPickerBySourceCluster.class.getCanonicalName();

  @Getter
  private final ReplicationCopyMode copyMode;

  @Getter
  private final ReplicationMetaData metaData;

  @Getter
  private final SourceEndPoint source;

  @Getter
  private final List<ReplicaEndPoint> replicas;

  @Getter
  private final DataFlowTopology topology;

  public static ReplicationConfiguration buildFromConfig(Config config) {
    Preconditions.checkArgument(config != null, "can not build ReplicationConfig from null");

    SourceEndPoint source = SourceEndPoint.buildSource(config);
    List<ReplicaEndPoint> replicas = ReplicaEndPoint.buildReplicaEndPoints(config);

    return new Builder().withReplicationMetaData(ReplicationMetaData.buildMetaData(config))
        .withReplicationSource(source).withReplicationReplicas(replicas)
        .withReplicationCopyMode(ReplicationCopyMode.getReplicationCopyMode(config))
        .withDataFlowTopology(DataFlowTopology.buildDataFlowTopology(config, source, replicas)).build();
  }

  private ReplicationConfiguration(Builder builder) {
    this.metaData = builder.metaData;
    this.source = builder.source;
    this.replicas = builder.replicas;
    this.topology = builder.topology;
    this.copyMode = builder.copyMode;
  }

  private static class Builder {
    private ReplicationMetaData metaData;

    private SourceEndPoint source;

    private List<ReplicaEndPoint> replicas;

    private DataFlowTopology topology;

    private ReplicationCopyMode copyMode;

    public Builder withReplicationMetaData(ReplicationMetaData metaData) {
      this.metaData = metaData;
      return this;
    }

    public Builder withReplicationSource(SourceEndPoint source) {
      this.source = source;
      return this;
    }

    public Builder withReplicationReplicas(List<ReplicaEndPoint> replicas) {
      this.replicas = replicas;
      return this;
    }

    public Builder withDataFlowTopology(DataFlowTopology topology) {
      this.topology = topology;
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
