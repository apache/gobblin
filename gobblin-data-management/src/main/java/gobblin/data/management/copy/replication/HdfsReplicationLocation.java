package gobblin.data.management.copy.replication;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import lombok.AllArgsConstructor;
import lombok.Getter;


/**
 * Class HdfsReplicationLocation is used to specify the Hdfs location of the replication data,
 * including hdfs colo, hdfs cluster name and hdfs path.
 * @author mitu
 *
 */
@AllArgsConstructor
public class HdfsReplicationLocation implements ReplicationLocation {
  public static final String HDFS_COLO_KEY = "cluster.colo";
  public static final String HDFS_CLUSTERNAME_KEY = "cluster.name";
  public static final String HDFS_PATH_KEY = "path";

  @Getter
  private final String colo;

  @Getter
  private final String clustername;

  @Getter
  private final String path;

  public HdfsReplicationLocation(Config config) {
    Preconditions.checkArgument(config.hasPath(HDFS_COLO_KEY));
    Preconditions.checkArgument(config.hasPath(HDFS_CLUSTERNAME_KEY));
    Preconditions.checkArgument(config.hasPath(HDFS_PATH_KEY));

    this.colo = config.getString(HDFS_COLO_KEY);
    this.clustername = config.getString(HDFS_CLUSTERNAME_KEY);
    this.path = config.getString(HDFS_PATH_KEY);
  }

  @Override
  public ReplicationType getType() {
    return ReplicationType.HDFS;
  }

  @Override
  public String toString() {
    return ReplicationType.HDFS + " colo:" + this.colo + ", clusterName: " + this.clustername + ",path: " + this.path;
  }
}
