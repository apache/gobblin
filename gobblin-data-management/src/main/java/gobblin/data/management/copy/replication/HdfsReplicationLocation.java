package gobblin.data.management.copy.replication;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Objects;
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
  public static final String HDFS_FILESYSTEM_URI_KEY = "cluster.FsURI";
  public static final String HDFS_PATH_KEY = "path";

  @Getter
  private final String colo;

  private final String clustername;

  private final URI fsURI;

  private final Path path;

  public HdfsReplicationLocation(Config config) {
    Preconditions.checkArgument(config.hasPath(HDFS_COLO_KEY));
    Preconditions.checkArgument(config.hasPath(HDFS_CLUSTERNAME_KEY));
    Preconditions.checkArgument(config.hasPath(HDFS_PATH_KEY));
    Preconditions.checkArgument(config.hasPath(HDFS_FILESYSTEM_URI_KEY));

    this.colo = config.getString(HDFS_COLO_KEY);
    this.clustername = config.getString(HDFS_CLUSTERNAME_KEY);
    this.path = new Path(config.getString(HDFS_PATH_KEY));
    try {
      this.fsURI = new URI(config.getString(HDFS_FILESYSTEM_URI_KEY));
    } catch (URISyntaxException e) {
      throw new RuntimeException("can not build URI based on " + config.getString(HDFS_FILESYSTEM_URI_KEY));
    }
  }

  @Override
  public ReplicationType getType() {
    return ReplicationType.HADOOPFS;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this.getClass()).add("colo", this.colo).add("name", this.clustername)
        .add("FilesystemURI", this.fsURI).add("rootPath", this.path).toString();

  }

  @Override
  public String getReplicationLocationName() {
    return this.clustername;
  }

  @Override
  public URI getFsURI() {
    return this.fsURI;
  }

  @Override
  public Path getRootPath() {
    return this.path;
  }
}
