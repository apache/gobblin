package gobblin.data.management.copy.replication;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import gobblin.data.management.copy.CopyableDataset;
import gobblin.dataset.DatasetsFinder;


/**
 * Utility class to build different java class from {@link com.typesafe.config.Config} 
 * @author mitu
 *
 */
public class ReplicationUtils {
  public static final String METADATA = "metadata";
  public static final String METADATA_JIRA = "jira";
  public static final String METADATA_OWNER = "owner";
  public static final String METADATA_NAME = "name";

  public static final String REPLICATION_SOURCE = "source";
  public static final String REPLICATION_REPLICAS = "replicas";
  public static final String REPLICATOIN_REPLICAS_LIST = "list";

  public static final String REPLICATION_LOCATION_TYPE_KEY = "type";

  public static final String DATAFLOWTOPOLOGY = "dataFlowTopology";

  public static ReplicationMetaData buildMetaData(Config config) {
    if (!config.hasPath(METADATA)) {
      return new ReplicationMetaData(Optional.absent(), Optional.absent(), Optional.absent());
    }

    Config metaDataConfig = config.getConfig(METADATA);

    Optional<String> metaDataJira = metaDataConfig.hasPath(METADATA_JIRA)
        ? Optional.of(metaDataConfig.getString(METADATA_JIRA)) : Optional.absent();
    Optional<String> metaDataOwner = metaDataConfig.hasPath(METADATA_OWNER)
        ? Optional.of(metaDataConfig.getString(METADATA_OWNER)) : Optional.absent();
    Optional<String> metaDataName = metaDataConfig.hasPath(METADATA_NAME)
        ? Optional.of(metaDataConfig.getString(METADATA_NAME)) : Optional.absent();

    ReplicationMetaData metaData = new ReplicationMetaData(metaDataJira, metaDataOwner, metaDataName);
    return metaData;
  }

  public static ReplicationSource buildSource(Config config) {
    Preconditions.checkArgument(config.hasPath(REPLICATION_SOURCE), "missing required config entery " + REPLICATION_SOURCE);

    Config sourceConfig = config.getConfig(REPLICATION_SOURCE);

    return new ReplicationSource(ReplicationUtils.buildReplicationLocation(sourceConfig), 
        ReplicationUtils.buildDatasetFinder(sourceConfig));
  }

  public static List<ReplicationReplica> buildReplicas(Config config) {
    Preconditions.checkArgument(config.hasPath(REPLICATION_REPLICAS), "missing required config entery " + REPLICATION_REPLICAS);

    Config replicaConfig = config.getConfig(REPLICATION_REPLICAS);
    Preconditions.checkArgument(replicaConfig.hasPath(REPLICATOIN_REPLICAS_LIST), "missing required config entery " + REPLICATOIN_REPLICAS_LIST);
    List<String> replicas = replicaConfig.getStringList(REPLICATOIN_REPLICAS_LIST);

    List<ReplicationReplica> result = new ArrayList<ReplicationReplica>();
    for (String replicaName : replicas) {
      Config subConfig = replicaConfig.getConfig(replicaName);
      result.add(
          new ReplicationReplica(replicaName, ReplicationUtils.buildReplicationLocation(subConfig), 
              ReplicationUtils.buildDatasetFinder(subConfig)));
    }

    return result;
  }

  public static DataFlowTopology buildDataFlowTopology(Config config, ReplicationSource source,
      List<ReplicationReplica> replicas) {
    Preconditions.checkArgument(config.hasPath(DATAFLOWTOPOLOGY), "missing required config entery " + DATAFLOWTOPOLOGY);
    Config dataflowConfig = config.getConfig(DATAFLOWTOPOLOGY);

    return new DataFlowTopology.Builder().withReplicationSource(source).withReplicationReplicas(replicas)
        .withTopologyConfig(dataflowConfig).build();
  }

  public static ReplicationLocation buildReplicationLocation(Config config) {
    Preconditions.checkArgument(config.hasPath(REPLICATION_LOCATION_TYPE_KEY), "missing required config entery " + REPLICATION_LOCATION_TYPE_KEY);

    String type = config.getString(REPLICATION_LOCATION_TYPE_KEY);
    Preconditions.checkArgument(config.hasPath(type));

    ReplicationType RType = ReplicationType.forName(type);

    if (RType == ReplicationType.HDFS) {
      return new HdfsReplicationLocation(config.getConfig(type));
    }

    // TODO, other replication types implmenetation 
    return null;
  }
  
  //TODO
  /**
   * Based on the input {@link Config} , build corresponding {@link DatasetsFinder}
   * @param config
   * @return
   */
  public static DatasetsFinder<CopyableDataset> buildDatasetFinder(Config config){
    return null;
  }
}
