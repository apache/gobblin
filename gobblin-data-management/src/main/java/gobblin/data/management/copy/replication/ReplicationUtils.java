package gobblin.data.management.copy.replication;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

import gobblin.data.management.copy.CopyableDataset;
import gobblin.dataset.DatasetsFinder;
import gobblin.util.reflection.GobblinConstructorUtils;


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

  public static final String DEFAULT_ALL_ROUTES_KEY = "defaultDataFlowTopology";

  public static final String ROUTES_PICKER_CLASS_KEY = "routePickerClass";
  public static final String DEFAULT_ROUTES_PICKER_CLASS_KEY =
      DataFlowRoutesPickerBySourceCluster.class.getCanonicalName();

  public static ReplicationMetaData buildMetaData(Config config) {
    if (!config.hasPath(METADATA)) {
      return new ReplicationMetaData(Optional.<Map<String, String>> absent());
    }

    Config metaDataConfig = config.getConfig(METADATA);
    Map<String, String> metaDataValues = new HashMap<>();
    Set<Map.Entry<String, ConfigValue>> meataDataEntry = metaDataConfig.entrySet();
    for (Map.Entry<String, ConfigValue> entry : meataDataEntry) {
      metaDataValues.put(entry.getKey(), metaDataConfig.getString(entry.getKey()));
    }

    ReplicationMetaData metaData = new ReplicationMetaData(Optional.of(metaDataValues));
    return metaData;
  }

  public static SourceEndPoint buildSource(Config config) {
    Preconditions.checkArgument(config.hasPath(REPLICATION_SOURCE),
        "missing required config entery " + REPLICATION_SOURCE);

    Config sourceConfig = config.getConfig(REPLICATION_SOURCE);

    return new SourceEndPoint(ReplicationUtils.buildReplicationLocation(sourceConfig),
        ReplicationUtils.buildDataset(sourceConfig));
  }

  public static List<ReplicaEndPoint> buildReplicas(Config config) {
    Preconditions.checkArgument(config.hasPath(REPLICATION_REPLICAS),
        "missing required config entery " + REPLICATION_REPLICAS);

    Config replicaConfig = config.getConfig(REPLICATION_REPLICAS);
    Preconditions.checkArgument(replicaConfig.hasPath(REPLICATOIN_REPLICAS_LIST),
        "missing required config entery " + REPLICATOIN_REPLICAS_LIST);
    List<String> replicas = replicaConfig.getStringList(REPLICATOIN_REPLICAS_LIST);

    List<ReplicaEndPoint> result = new ArrayList<ReplicaEndPoint>();
    for (String replicaName : replicas) {
      Config subConfig = replicaConfig.getConfig(replicaName);
      result.add(new ReplicaEndPoint(replicaName, ReplicationUtils.buildReplicationLocation(subConfig),
          ReplicationUtils.buildDataset(subConfig)));
    }

    return result;
  }

  public static DataFlowTopology buildDataFlowTopology(Config config, SourceEndPoint source,
      List<ReplicaEndPoint> replicas) {
    Preconditions.checkArgument(config.hasPath(DATAFLOWTOPOLOGY), "missing required config entery " + DATAFLOWTOPOLOGY);
    Config dataflowConfig = config.getConfig(DATAFLOWTOPOLOGY);

    // NOT specified by literal routes, need to pick it
    if (!dataflowConfig.hasPath(DataFlowTopology.ROUTES)) {
      // DEFAULT_ALL_ROUTES_KEY specified in top level config
      Preconditions.checkArgument(config.hasPath(DEFAULT_ALL_ROUTES_KEY),
          "missing required config entery " + DEFAULT_ALL_ROUTES_KEY);

      Config allRoutes = config.getConfig(DEFAULT_ALL_ROUTES_KEY);
      String routePickerClassName = dataflowConfig.hasPath(ROUTES_PICKER_CLASS_KEY)
          ? dataflowConfig.getString(ROUTES_PICKER_CLASS_KEY) : DEFAULT_ROUTES_PICKER_CLASS_KEY;

      List<Object> args = Lists.newArrayList(allRoutes, source);

      try {
        Class<?> routePickerClass = Class.forName(routePickerClassName);

        DataFlowRoutesPicker picker =
            (DataFlowRoutesPicker) (GobblinConstructorUtils.invokeLongestConstructor(routePickerClass, args.toArray()));
        dataflowConfig = picker.getPreferredRoutes();
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException("Can not build topology from class: " + e.getMessage());
      }
    }
    DataFlowTopology.Builder builder = new DataFlowTopology.Builder().withReplicationSource(source);
    for (ReplicaEndPoint replica : replicas) {
      builder.addReplicationReplica(replica);
    }

    return builder.withTopologyConfig(dataflowConfig).build();
  }

  public static ReplicationLocation buildReplicationLocation(Config config) {
    Preconditions.checkArgument(config.hasPath(REPLICATION_LOCATION_TYPE_KEY),
        "missing required config entery " + REPLICATION_LOCATION_TYPE_KEY);

    String type = config.getString(REPLICATION_LOCATION_TYPE_KEY);
    Preconditions.checkArgument(config.hasPath(type));

    ReplicationType RType = ReplicationType.forName(type);

    if (RType == ReplicationType.HDFS) {
      return new HdfsReplicationLocation(config.getConfig(type));
    }

    throw new UnsupportedOperationException("Not support for replication type " + RType);
  }

  //TODO
  /**
   * Based on the input {@link Config} , build corresponding {@link DatasetsFinder}
   * @param config
   * @return
   */
  public static CopyableDataset buildDataset(Config config) {
    return null;
  }
}
