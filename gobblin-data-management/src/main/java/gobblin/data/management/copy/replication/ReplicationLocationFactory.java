package gobblin.data.management.copy.replication;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;


/**
 * Utility class to build {@link ReplicationLocation} from {@link com.typesafe.config.Config} 
 * @author mitu
 *
 */
public class ReplicationLocationFactory {

  public static ReplicationLocation buildReplicationLocation(Config config) {
    Preconditions.checkArgument(config.hasPath(ReplicationConfiguration.REPLICATION_LOCATION_TYPE_KEY),
        "missing required config entery " + ReplicationConfiguration.REPLICATION_LOCATION_TYPE_KEY);

    String type = config.getString(ReplicationConfiguration.REPLICATION_LOCATION_TYPE_KEY);
    Preconditions.checkArgument(config.hasPath(type));

    ReplicationLocationType RType = ReplicationLocationType.forName(type);

    if (RType == ReplicationLocationType.HADOOPFS) {
      return new HdfsReplicationLocation(config.getConfig(type));
    }

    throw new UnsupportedOperationException("Not support for replication type " + RType);
  }
}
